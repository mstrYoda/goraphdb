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
// It runs a background goroutine that handles:
//   - Initial catch-up: replays all entries from the follower's last applied LSN
//   - Continuous streaming: receives new entries as they're committed on the leader
//   - Automatic reconnection: retries with exponential backoff on disconnect
//
// Architecture:
//
//	┌──────────────────────────────────────────┐
//	│  Client.Start()                          │
//	│    │                                     │
//	│    ▼                                     │
//	│  connect to leader (gRPC)                │
//	│    │                                     │
//	│    ▼                                     │
//	│  StreamWAL(from_lsn = appliedLSN + 1)    │
//	│    │                                     │
//	│    ▼                                     │
//	│  for entry := range stream {             │
//	│      applier.Apply(entry)                │
//	│  }                                       │
//	│    │  on error → backoff → reconnect     │
//	│    └─────────────────────────────────────│
//	└──────────────────────────────────────────┘
// ---------------------------------------------------------------------------

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
// entries until the stream ends or an error occurs.
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
	)

	stream, err := client.StreamWAL(ctx, &pb.StreamWALRequest{
		FromLsn:    fromLSN,
		FollowerId: c.followerID,
	})
	if err != nil {
		return fmt.Errorf("StreamWAL: %w", err)
	}

	c.log.Info("replication stream established",
		"from_lsn", fromLSN,
	)

	var applied uint64
	for {
		protoEntry, err := stream.Recv()
		if err == io.EOF {
			return fmt.Errorf("leader closed stream after %d entries", applied)
		}
		if err != nil {
			return fmt.Errorf("stream recv after %d entries: %w", applied, err)
		}

		// Convert proto to WALEntry.
		entry := &graphdb.WALEntry{
			LSN:       protoEntry.Lsn,
			Timestamp: protoEntry.TimestampNs,
			Op:        graphdb.OpType(protoEntry.Op),
			Payload:   protoEntry.Payload,
		}

		if err := c.applier.Apply(entry); err != nil {
			c.log.Error("failed to apply entry",
				"lsn", entry.LSN,
				"op", entry.Op,
				"error", err,
			)
			return fmt.Errorf("apply LSN %d: %w", entry.LSN, err)
		}

		applied++
		if applied%10000 == 0 {
			c.log.Debug("replication progress",
				"applied_count", applied,
				"lsn", entry.LSN,
			)
		}

		// Reset backoff on successful apply (we're connected and working).
	}
}
