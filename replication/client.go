package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
	pb "github.com/mstrYoda/goraphdb/replication/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// Replication Client — follower side
//
// The client connects to the leader's gRPC replication service, receives
// WAL entries via a server-streaming RPC, and applies them using the Applier.
//
// Architecture (concurrent pipeline):
//
//	┌────────────────────────────────────────────────────────────┐
//	│  Client.Start()                                           │
//	│    │                                                      │
//	│    ▼                                                      │
//	│  connect to leader (gRPC)                                 │
//	│    │                                                      │
//	│    ▼                                                      │
//	│  StreamWAL(from_lsn = appliedLSN + 1)                     │
//	│    │                                                      │
//	│    ▼                                                      │
//	│  ┌──────────────┐   chan    ┌─────────────────────────┐   │
//	│  │   receiver   │ ───────► │  N apply workers        │   │
//	│  │  goroutine   │  entries │  (concurrent ApplyOp)   │   │
//	│  │ stream.Recv  │          │  bbolt Batch() groups   │   │
//	│  └──────────────┘          └────────┬────────────────┘   │
//	│                                     │                     │
//	│                                     ▼                     │
//	│                              lsnTracker                   │
//	│                          (contiguous LSN)                 │
//	└────────────────────────────────────────────────────────────┘
//
// Why concurrent: on the follower, the Applier is the ONLY writer. With
// bbolt Batch(), a single writer can't batch — each Batch() call degrades
// to Update() (1 fsync per entry ≈ 38 ops/s). With N concurrent workers,
// their writeUpdate() calls are grouped by bbolt into a single fsync,
// giving N × throughput for a single fsync cost.
// ---------------------------------------------------------------------------

const (
	// applierWorkers is the number of concurrent goroutines applying WAL
	// entries on the follower. More workers = better bbolt Batch() grouping.
	// 8 workers with a 10ms batch window ≈ 8 entries per fsync.
	applierWorkers = 8

	// applierChBuffer is the size of the entry channel between the receiver
	// goroutine and the apply workers. Larger buffers decouple network
	// latency from apply latency.
	applierChBuffer = 64
)

// Client connects to a leader and replicates WAL entries to the local database.
type Client struct {
	leaderAddr string
	followerID string
	applier    *graphdb.Applier
	log        *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}

	// Backoff configuration for reconnection.
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// ClientOption configures the replication client.
type ClientOption func(*Client)

// WithFollowerID sets the follower identifier for logging.
func WithFollowerID(id string) ClientOption {
	return func(c *Client) { c.followerID = id }
}

// WithClientLogger sets the logger for the replication client.
func WithClientLogger(l *slog.Logger) ClientOption {
	return func(c *Client) { c.log = l }
}

// WithBackoff sets the initial and max backoff for reconnection.
func WithBackoff(initial, max time.Duration) ClientOption {
	return func(c *Client) {
		c.initialBackoff = initial
		c.maxBackoff = max
	}
}

// NewClient creates a replication client that will connect to the given
// leader address and apply WAL entries using the provided applier.
func NewClient(leaderAddr string, applier *graphdb.Applier, opts ...ClientOption) *Client {
	c := &Client{
		leaderAddr:     leaderAddr,
		followerID:     "",
		applier:        applier,
		log:            slog.Default(),
		initialBackoff: 100 * time.Millisecond,
		maxBackoff:     5 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start begins the replication loop in a background goroutine.
// It connects to the leader, streams WAL entries, and applies them.
// Call Stop() to gracefully shut down.
func (c *Client) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.cancel = cancel
	c.done = make(chan struct{})
	c.mu.Unlock()

	go c.run(ctx)
}

// Stop gracefully stops the replication client and waits for it to finish.
func (c *Client) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// run is the main replication loop. It connects to the leader, streams WAL
// entries, and reconnects with exponential backoff on failure.
func (c *Client) run(ctx context.Context) {
	defer close(c.done)

	backoff := c.initialBackoff

	for {
		select {
		case <-ctx.Done():
			c.log.Info("replication client stopped")
			return
		default:
		}

		err := c.streamOnce(ctx)
		if ctx.Err() != nil {
			return // Context cancelled — shutting down.
		}

		c.log.Warn("replication stream disconnected, reconnecting",
			"error", err,
			"backoff", backoff,
		)

		// Exponential backoff.
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = backoff * 2
		if backoff > c.maxBackoff {
			backoff = c.maxBackoff
		}
	}
}

// streamOnce connects to the leader, opens a StreamWAL stream, and applies
// entries using a concurrent worker pipeline until the stream ends or error.
func (c *Client) streamOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(
		c.leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial leader: %w", err)
	}
	defer conn.Close()

	client := pb.NewReplicationServiceClient(conn)

	fromLSN := c.applier.AppliedLSN() + 1
	c.log.Info("connecting to leader",
		"addr", c.leaderAddr,
		"from_lsn", fromLSN,
		"workers", applierWorkers,
	)

	stream, err := client.StreamWAL(ctx, &pb.StreamWALRequest{
		FromLsn:    fromLSN,
		FollowerId: c.followerID,
	})
	if err != nil {
		return fmt.Errorf("StreamWAL: %w", err)
	}

	c.log.Info("replication stream established", "from_lsn", fromLSN)

	// -----------------------------------------------------------------------
	// Concurrent apply pipeline
	//
	// 1. Receiver goroutine reads entries from the gRPC stream into a channel.
	// 2. N worker goroutines read from the channel and call ApplyOp().
	//    Their concurrent writeUpdate() calls are batched by bbolt Batch().
	// 3. The LSN tracker ensures we only advance appliedLSN contiguously
	//    (no gaps), which is critical for correct crash recovery.
	// -----------------------------------------------------------------------

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()

	entryCh := make(chan *graphdb.WALEntry, applierChBuffer)
	tracker := newLSNTracker(c.applier.AppliedLSN())

	// Error collection: first error wins.
	var (
		applyErr  error
		errOnce   sync.Once
		workerWg  sync.WaitGroup
	)

	// Start apply workers.
	for i := 0; i < applierWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for entry := range entryCh {
				if pipeCtx.Err() != nil {
					return
				}
				if err := c.applier.ApplyOp(entry); err != nil {
					errOnce.Do(func() {
						applyErr = fmt.Errorf("apply LSN %d: %w", entry.LSN, err)
						pipeCancel() // stop all workers
					})
					c.log.Error("failed to apply entry",
						"lsn", entry.LSN,
						"op", entry.Op,
						"error", err,
					)
					return
				}
				// Mark this LSN as done. The tracker advances the contiguous
				// LSN and updates the applier atomically.
				contiguous := tracker.MarkDone(entry.LSN)
				c.applier.SetAppliedLSN(contiguous)
			}
		}()
	}

	// Receiver: read from gRPC stream and push to workers.
	var recvErr error
	var applied uint64
	for {
		protoEntry, err := stream.Recv()
		if err == io.EOF {
			recvErr = fmt.Errorf("leader closed stream after %d entries", applied)
			break
		}
		if err != nil {
			recvErr = fmt.Errorf("stream recv after %d entries: %w", applied, err)
			break
		}

		entry := &graphdb.WALEntry{
			LSN:       protoEntry.Lsn,
			Timestamp: protoEntry.TimestampNs,
			Op:        graphdb.OpType(protoEntry.Op),
			Payload:   protoEntry.Payload,
		}

		// Skip already-applied entries.
		if entry.LSN <= c.applier.AppliedLSN() {
			continue
		}

		select {
		case entryCh <- entry:
			applied++
		case <-pipeCtx.Done():
			// A worker hit an error.
			break
		}

		if pipeCtx.Err() != nil {
			break
		}

		if applied%10000 == 0 {
			c.log.Debug("replication progress",
				"applied_count", applied,
				"lsn", entry.LSN,
			)
		}
	}

	// Signal workers to drain and stop.
	close(entryCh)
	workerWg.Wait()

	if applyErr != nil {
		return applyErr
	}
	return recvErr
}

// ---------------------------------------------------------------------------
// lsnTracker — contiguous LSN tracking for concurrent apply
//
// When N workers apply entries concurrently, they may complete out of order.
// For example, with entries [5, 6, 7, 8], worker B might finish 7 before
// worker A finishes 6. We must only advance appliedLSN to the highest LSN
// where ALL preceding entries are also complete.
//
// This is similar to TCP's cumulative acknowledgment: we track which LSNs
// are "done" and advance the contiguous watermark as gaps fill in.
//
//	 baseline=5  done={7,8}  →  contiguous=5 (gap at 6)
//	 baseline=5  done={6,7,8}  →  contiguous=8 (gap filled)
// ---------------------------------------------------------------------------

type lsnTracker struct {
	mu       sync.Mutex
	done     map[uint64]struct{} // LSNs that have been applied but not yet contiguous
	baseline uint64              // highest contiguous applied LSN
}

func newLSNTracker(baseline uint64) *lsnTracker {
	return &lsnTracker{
		done:     make(map[uint64]struct{}),
		baseline: baseline,
	}
}

// MarkDone records that the given LSN has been applied and returns the new
// contiguous watermark (highest LSN where all preceding LSNs are also done).
func (t *lsnTracker) MarkDone(lsn uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.done[lsn] = struct{}{}

	// Advance baseline as far as possible.
	for {
		if _, ok := t.done[t.baseline+1]; !ok {
			break
		}
		t.baseline++
		delete(t.done, t.baseline)
	}

	return t.baseline
}
