package graphdb

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"
)

// ---------------------------------------------------------------------------
// Query Governor — enforces resource limits on query execution.
//
// The governor protects the database from two classes of problems:
//
//  1. Runaway result sets — a MATCH (n) RETURN n on 10M nodes would OOM
//     the process without a row cap. MaxResultRows bounds the number of
//     rows any single query can accumulate before returning ErrResultTooLarge.
//
//  2. Unbounded execution time — if no context deadline is set by the caller,
//     DefaultQueryTimeout provides a safety net so queries don't run forever.
//
// The governor is initialized once in DB.Open() and threaded through the
// query execution path. It is immutable after creation (no mutex needed).
// ---------------------------------------------------------------------------

// Sentinel errors returned by the governor and panic recovery.
var (
	// ErrResultTooLarge is returned when a query's result set exceeds
	// the configured MaxResultRows limit. The caller should add a LIMIT
	// clause or increase the limit in Options.
	ErrResultTooLarge = errors.New("graphdb: result set exceeds MaxResultRows limit")

	// ErrQueryPanic is returned when a query execution panics.
	// The panic is caught at the query boundary so the DB remains operational.
	// The original panic value and stack trace are included in the error message.
	ErrQueryPanic = errors.New("graphdb: query panicked")

	// errRowLimitReached is an internal sentinel used to stop iteration
	// callbacks (e.g., forEachNode) when the governor's row limit is hit.
	// It is unwrapped into ErrResultTooLarge at the public API boundary.
	errRowLimitReached = errors.New("graphdb: row limit reached")
)

// queryGovernor enforces per-query resource limits.
// Created once in DB.Open() and shared (read-only) across all queries.
type queryGovernor struct {
	maxRows        int           // 0 = unlimited
	defaultTimeout time.Duration // 0 = no default timeout
}

// wrapContext applies DefaultQueryTimeout when the caller's context has no
// deadline. If the caller already set a deadline (e.g., via context.WithTimeout),
// their deadline takes priority — the governor does NOT override explicit timeouts.
//
// Returns the (possibly wrapped) context and a cancel function that MUST be
// called by the caller (even if the context was not wrapped, cancel is a no-op).
func (g *queryGovernor) wrapContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if g.defaultTimeout <= 0 {
		return ctx, func() {} // no-op cancel
	}

	// Only apply the default timeout if the caller did not set one.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return context.WithTimeout(ctx, g.defaultTimeout)
	}
	return ctx, func() {} // caller's deadline wins
}

// checkRowCount returns ErrResultTooLarge if the accumulated row count
// exceeds the configured limit. Called at result accumulation points
// (forEachNode callbacks, projectResults, etc.).
func (g *queryGovernor) checkRowCount(n int) error {
	if g.maxRows > 0 && n > g.maxRows {
		return ErrResultTooLarge
	}
	return nil
}

// ---------------------------------------------------------------------------
// Panic Recovery
// ---------------------------------------------------------------------------

// safeExecute runs fn inside a deferred recover() so that panics in query
// execution are converted to errors instead of crashing the process.
//
// When a panic is caught:
//   - A stack trace is captured (up to 4KB) for debugging.
//   - The error wraps ErrQueryPanic so callers can check with errors.Is().
//   - The DB remains fully operational for subsequent queries.
//
// This is applied at every public Cypher entry point (Cypher, CypherWithParams,
// CypherCreate, CypherStream, ExecutePrepared, etc.).
func safeExecute(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Capture a stack trace for the panic location.
			// 4KB is enough for most stacks; runtime.Stack truncates if needed.
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("%w: %v\n\nstack trace:\n%s", ErrQueryPanic, r, buf[:n])
		}
	}()
	return fn()
}

// safeExecuteResult is the generic version of safeExecute for functions that
// return a value and an error (e.g., Cypher → *CypherResult, error).
func safeExecuteResult[T any](fn func() (T, error)) (result T, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			var zero T
			result = zero
			err = fmt.Errorf("%w: %v\n\nstack trace:\n%s", ErrQueryPanic, r, buf[:n])
		}
	}()
	return fn()
}
