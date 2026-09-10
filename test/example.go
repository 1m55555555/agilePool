package main

// example.go holds two demonstration plugins that cover every optional
// capability of the framework in the smallest runnable form:
//
//   - the minimum plugin contract Name/Desc/Run (both plugins)
//   - Optioned.Options(): declares key=value options shown by --help <name>
//     (provider)
//   - Lifecycle.Start/End: Start runs before every Run in order (and receives
//     the segment args), End after every Run in reverse (both plugins; a
//     plugin needing only one phase returns nil from the other)
//   - Depender.Deps(): declares who must run first; the host auto-inserts a
//     missing --provider before its dependent (consumer depends on provider)
//   - shared data: the upstream Provides into its own namespace, the
//     downstream Require-reads it (consumer reads the provider's integer)
//
// The examples are not registered by default (the real plugins live in
// plugins.go); to try them, temporarily add both types to allPlugins:
//
//	agilepool_test.exe --provider seed=5     # provider alone
//	agilepool_test.exe --consumer            # provider auto-inserted first
//	agilepool_test.exe --consumer --provider # misorder -> error
//	agilepool_test.exe --provider fail=true  # Run error -> exit 1, End still runs
//	agilepool_test.exe --help provider       # option table
//
// Expected order for --consumer:
// Start: provider → consumer; Run: provider → consumer; End: consumer → provider.

import (
	"context"
	"fmt"
)

// --- provider: dependency-free upstream, demonstrates Options/Provide/Lifecycle ---

type providerPlugin struct{}

func (providerPlugin) Name() string { return "provider" }
func (providerPlugin) Desc() string { return "example: parse args and share an int via Store" }

func (providerPlugin) Options() []Option {
	return []Option{
		{Name: "seed", Default: "1", Help: "the integer to provide"},
		{Name: "fail", Default: "false", Help: "when true, Run returns an error (error path demo)"},
	}
}

func (providerPlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	fmt.Fprintln(rt.Out, "[provider] Start")
	return nil
}

func (providerPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	opts := ParseOptions(args)
	seed, err := GetInt(opts, "seed", 1)
	if err != nil {
		return err
	}
	fail, err := GetBool(opts, "fail", false)
	if err != nil {
		return err
	}
	fmt.Fprintf(rt.Out, "[provider] Run seed=%d\n", seed)
	if fail {
		return fmt.Errorf("injected failure (fail=true)")
	}
	// Writing one's own namespace; the key only has to be unique here.
	Provide(rt.Store, "provider", "seed", seed)
	return nil
}

func (providerPlugin) End(ctx context.Context, rt *Runtime) error {
	fmt.Fprintln(rt.Out, "[provider] End")
	return nil
}

// --- consumer: downstream depending on provider, demonstrates Deps/Require/Lifecycle ---

type consumerPlugin struct{}

func (consumerPlugin) Name() string { return "consumer" }
func (consumerPlugin) Desc() string { return "example: read the provider's Store value" }

// Deps is metadata consumed by the planner; the actual read happens in Run.
func (consumerPlugin) Deps() []string { return []string{"provider"} }

func (consumerPlugin) Start(ctx context.Context, rt *Runtime, args []string) error {
	fmt.Fprintln(rt.Out, "[consumer] Start")
	return nil
}

func (consumerPlugin) Run(ctx context.Context, rt *Runtime, args []string) error {
	seed, err := Require[int](rt.Store, "provider", "seed")
	if err != nil {
		return err
	}
	fmt.Fprintf(rt.Out, "[consumer] Run got provider.seed=%d\n", seed)
	return nil
}

func (consumerPlugin) End(ctx context.Context, rt *Runtime) error {
	fmt.Fprintln(rt.Out, "[consumer] End")
	return nil
}
