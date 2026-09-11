// Package hook provides the bundled lifecycle callback dispatcher for
// agilePool. Create one with NewHooks, register callbacks, and hand it to
// Pool.SetHook before submitting tasks:
//
//	h := hook.NewHooks()
//	h.AddTaskStarted(func(ctx context.Context) { ... })
//	pool.SetHook(h)
//
// Every callback runs in the goroutine that triggers the event: the
// submitting goroutine for Submitted/Enqueued, a worker for Started/
// Completed, and the Close caller for PoolClosed. A panicking callback is
// recovered and logged, and does not affect the remaining callbacks of the
// same event.
package hook

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// Hooks stores and dispatches lifecycle callbacks. It implements the
// agilepool.Hooks interface.
type Hooks struct {
	// mu serializes registrations. Dispatch never takes this lock: callbacks
	// are published as immutable snapshots and loaded atomically on the hot
	// path.
	mu        sync.Mutex
	callbacks atomic.Pointer[callbackSnapshot]
	logger    log.Logger
}

// callbackSnapshot is immutable after publication. Add methods create a new
// snapshot before storing it, so a dispatch can safely range over a snapshot
// while another goroutine registers a callback.
type callbackSnapshot struct {
	taskSubmitted []agilepool.TaskHook
	taskEnqueued  []agilepool.TaskHook
	taskStarted   []agilepool.TaskHook
	taskCompleted []agilepool.TaskCompleteHook
	poolClosed    []agilepool.PoolHook
}

// NewHooks returns an empty dispatcher that logs recovered callback panics
// through the standard logger.
func NewHooks() *Hooks {
	h := &Hooks{
		logger: *log.Default(),
	}
	h.callbacks.Store(&callbackSnapshot{})
	return h
}

// AddTaskSubmitted registers a callback for task submission.
func (h *Hooks) AddTaskSubmitted(fn agilepool.TaskHook) {
	h.mu.Lock()
	current := h.currentCallbacks()
	next := *current
	next.taskSubmitted = appendCallback(current.taskSubmitted, fn)
	h.callbacks.Store(&next)
	h.mu.Unlock()
}

// AddTaskEnqueued registers a callback for task enqueue.
func (h *Hooks) AddTaskEnqueued(fn agilepool.TaskHook) {
	h.mu.Lock()
	current := h.currentCallbacks()
	next := *current
	next.taskEnqueued = appendCallback(current.taskEnqueued, fn)
	h.callbacks.Store(&next)
	h.mu.Unlock()
}

// AddTaskStarted registers a callback for task start.
func (h *Hooks) AddTaskStarted(fn agilepool.TaskHook) {
	h.mu.Lock()
	current := h.currentCallbacks()
	next := *current
	next.taskStarted = appendCallback(current.taskStarted, fn)
	h.callbacks.Store(&next)
	h.mu.Unlock()
}

// AddTaskCompleted registers a callback for task completion. The callback
// receives the value a panicking task panicked with, or nil on normal exit.
func (h *Hooks) AddTaskCompleted(fn agilepool.TaskCompleteHook) {
	h.mu.Lock()
	current := h.currentCallbacks()
	next := *current
	next.taskCompleted = appendCallback(current.taskCompleted, fn)
	h.callbacks.Store(&next)
	h.mu.Unlock()
}

// AddPoolClosed registers a callback for pool close.
func (h *Hooks) AddPoolClosed(fn agilepool.PoolHook) {
	h.mu.Lock()
	current := h.currentCallbacks()
	next := *current
	next.poolClosed = appendCallback(current.poolClosed, fn)
	h.callbacks.Store(&next)
	h.mu.Unlock()
}

// DispatchTaskSubmitted dispatches submission callbacks. It must only be
// called by the pool submission path.
func (h *Hooks) DispatchTaskSubmitted(ctx context.Context) {
	callbacks := h.currentCallbacks()
	for _, fn := range callbacks.taskSubmitted {
		h.invoke(func() { fn(ctx) }, "OnTaskSubmitted")
	}
}

// DispatchTaskEnqueued dispatches enqueue callbacks. It must only be called
// by the pool enqueue path.
func (h *Hooks) DispatchTaskEnqueued(ctx context.Context) {
	callbacks := h.currentCallbacks()
	for _, fn := range callbacks.taskEnqueued {
		h.invoke(func() { fn(ctx) }, "OnTaskEnqueued")
	}
}

// DispatchTaskStarted dispatches start callbacks. It must only be called by
// the worker execution path.
func (h *Hooks) DispatchTaskStarted(ctx context.Context) {
	callbacks := h.currentCallbacks()
	for _, fn := range callbacks.taskStarted {
		h.invoke(func() { fn(ctx) }, "OnTaskStarted")
	}
}

// DispatchTaskCompleted dispatches completion callbacks. It must only be
// called by the worker completion path.
func (h *Hooks) DispatchTaskCompleted(ctx context.Context, recovered any) {
	callbacks := h.currentCallbacks()
	for _, fn := range callbacks.taskCompleted {
		h.invoke(func() { fn(ctx, recovered) }, "OnTaskCompleted")
	}
}

// DispatchPoolClosed dispatches pool-close callbacks. It must only be called
// by Pool.Close.
func (h *Hooks) DispatchPoolClosed(pool *agilepool.Pool) {
	if pool == nil {
		h.logger.Println("[ERROR] DispatchPoolClosed: received nil pointer")
		return
	}
	callbacks := h.currentCallbacks()
	for _, fn := range callbacks.poolClosed {
		h.invoke(func() { fn(pool) }, "OnPoolClosed")
	}
}

func (h *Hooks) currentCallbacks() *callbackSnapshot {
	if callbacks := h.callbacks.Load(); callbacks != nil {
		return callbacks
	}
	// Hooks' zero value remains usable. NewHooks is preferred because it also
	// configures the standard logger, but a zero-value dispatcher can still
	// safely register and dispatch callbacks.
	return &callbackSnapshot{}
}

func appendCallback[T any](callbacks []T, fn T) []T {
	next := make([]T, len(callbacks)+1)
	copy(next, callbacks)
	next[len(callbacks)] = fn
	return next
}

func (h *Hooks) invoke(fn func(), name string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Printf("hook %s panicked: %v\n%s \n", name, recovered, debug.Stack())
		}
	}()
	fn()
}
