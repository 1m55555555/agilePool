package main

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	hook "github.com/Yiming1997/agilePool/v2/hook"
	"github.com/gin-gonic/gin"
)

// This example keeps request work bounded. NONBLOCK makes TrySubmit return
// immediately when both the handoff channel and overflow buffer are full.
var pool = agilepool.NewPool(agilepool.NewConfig(
	agilepool.WithBlockMode(agilepool.NONBLOCK),
	agilepool.WithTaskQueueSize(32),
	agilepool.WithWorkerNumCapacity(8),
))

// A newly-created Pool has no hook dispatcher, so hooks are disabled.
// SetHook enables them.
var taskStartTimes sync.Map

func main() {
	defer pool.Close()

	// Register callbacks before submitting tasks: the hook set is read
	// without synchronization by every dispatch path. Hook callbacks run in
	// the submitting goroutine, a worker goroutine, or the Close caller as
	// appropriate. hook.NewHooks is the bundled dispatcher; a custom
	// implementation only needs agilepool.Hooks' five dispatch methods.
	h := hook.NewHooks()
	h.AddTaskSubmitted(onSubmitted)
	h.AddTaskEnqueued(onEnqueued)
	h.AddTaskStarted(onStarted)
	h.AddTaskCompleted(onCompleted)
	h.AddPoolClosed(onPoolClosed)
	pool.SetHook(h)
	r := gin.Default()
	r.Use(PoolMiddleware)
	// This example shows how to limit concurrency for incoming requests.
	r.POST("/jobs", anRouterFunc)
	_ = r.Run("127.0.0.1:8080")
}

func onSubmitted(ctx context.Context) {
	log.Printf("task submitted")
}

func onEnqueued(ctx context.Context) {
	log.Printf("task enqueued")
}

func onStarted(ctx context.Context) {
	if c, ok := ctx.(*gin.Context); ok {
		traceID, _ := c.Get("traceID")
		taskStartTimes.Store(traceID, time.Now())
		log.Printf("task started traceID=%v clientIP=%s", traceID, c.ClientIP())
	}
}

func onCompleted(ctx context.Context, recovered any) {
	if c, ok := ctx.(*gin.Context); ok {
		traceID, _ := c.Get("traceID")
		if started, exists := taskStartTimes.LoadAndDelete(traceID); exists {
			log.Printf("task completed traceID=%v duration=%s recovered=%v", traceID, time.Since(started.(time.Time)), recovered)
		}
	}
}

func onPoolClosed(p *agilepool.Pool) {
	log.Println("pool closed")
}

type Content struct {
	Content string `json:"content"`
}

// PoolMiddleware runs the complete Gin handler chain in an agilePool task.
// UpdateTask keeps the original Gin context available to the task.
func PoolMiddleware(c *gin.Context) {
	// done keeps the HTTP request open until the pool worker finishes the
	// complete Gin handler chain and writes the response.
	done := make(chan struct{})
	c.Set("traceID", time.Now().UnixNano())
	// UpdateTask carries the Gin context through the pool. The worker executes
	// c.Next(), so normal handlers can write responses in the usual way.
	task := agilepool.UpdateTask(c, agilepool.TaskFunc(func() error {
		defer close(done)
		c.Next()
		return nil
	}))
	if !pool.TrySubmit(task) {
		// NONBLOCK rejects the request when the pool is saturated.
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "system busy"})
		c.Abort()
		return
	}
	// For this small example, assume the request is not cancelled and wait
	// until the handler chain has completed.
	<-done
}

func anRouterFunc(c *gin.Context) {
	var content Content
	if err := c.ShouldBindBodyWithJSON(&content); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// The handler itself runs in the pool; no extra result channel is needed.
	// It can write the response normally because PoolMiddleware waits for
	// c.Next() to finish before returning to the HTTP server.
	time.Sleep(10 * time.Second)
	c.JSON(http.StatusOK, content)
}
