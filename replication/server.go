package replication

import (
	"io"
	"log/slog"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
	pb "github.com/mstrYoda/goraphdb/replication/proto"
)

// ---------------------------------------------------------------------------
// Replication Server — leader side
//
// The server implements the gRPC ReplicationService. When a follower calls
// StreamWAL, the server opens a WALReader at the requested LSN and streams
// entries as they become available. The stream stays open — new entries are
// pushed via a polling loop with configurable interval.
//
// Architecture:
//
//	Follower gRPC client
//	    │  StreamWAL(from_lsn=42)
//	    ▼
//	┌───────────────────────────────┐
//	│  ReplicationServer.StreamWAL  │
//	│    │                          │
//	│    ▼                          │
//	│  WALReader.Next() ──► send    │ ─── entries are pushed as they appear
//	│    │  EOF? poll...            │
//	│    └──────────────────────────│
//	└───────────────────────────────┘
//
// The poll interval is a trade-off between replication lag and CPU usage.
// Default: 50ms — provides sub-100ms replication lag under normal load.
// ---------------------------------------------------------------------------

// Server implements the gRPC ReplicationService on the leader.
type Server struct {
	pb.UnimplementedReplicationServiceServer
	db           *graphdb.DB
	wal          *graphdb.WAL
	log          *slog.Logger
	pollInterval time.Duration
}

// ServerOption configures the replication server.
type ServerOption func(*Server)

// WithPollInterval sets the WAL polling interval for new entries.
func WithPollInterval(d time.Duration) ServerOption {
	return func(s *Server) { s.pollInterval = d }
}

// WithLogger sets the logger for the replication server.
func WithLogger(l *slog.Logger) ServerOption {
	return func(s *Server) { s.log = l }
}

// NewServer creates a replication server backed by the given DB and WAL.
func NewServer(db *graphdb.DB, wal *graphdb.WAL, opts ...ServerOption) *Server {
	s := &Server{
		db:           db,
		wal:          wal,
		log:          slog.Default(),
		pollInterval: 50 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// StreamWAL implements the gRPC server-streaming RPC.
// It reads WAL entries starting from the requested LSN and sends them over
// the stream. When it catches up to the head, it polls for new entries.
func (s *Server) StreamWAL(req *pb.StreamWALRequest, stream pb.ReplicationService_StreamWALServer) error {
	fromLSN := req.FromLsn
	followerID := req.FollowerId

	s.log.Info("follower connected",
		"follower_id", followerID,
		"from_lsn", fromLSN,
	)

	reader, err := s.wal.NewReader(fromLSN)
	if err != nil {
		s.log.Error("failed to create WAL reader",
			"follower_id", followerID,
			"from_lsn", fromLSN,
			"error", err,
		)
		return err
	}
	defer reader.Close()

	var sentCount uint64
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		// Check if the client disconnected.
		if err := stream.Context().Err(); err != nil {
			s.log.Info("follower disconnected",
				"follower_id", followerID,
				"sent_count", sentCount,
				"reason", err,
			)
			return nil
		}

		entry, err := reader.Next()
		if err == io.EOF {
			// Caught up — wait for new entries.
			<-ticker.C
			continue
		}
		if err != nil {
			s.log.Error("WAL read error",
				"follower_id", followerID,
				"error", err,
			)
			return err
		}

		// Convert to protobuf message.
		protoEntry := &pb.WALEntryProto{
			Lsn:         entry.LSN,
			TimestampNs: entry.Timestamp,
			Op:          uint32(entry.Op),
			Payload:     entry.Payload,
		}

		if err := stream.Send(protoEntry); err != nil {
			s.log.Error("failed to send entry",
				"follower_id", followerID,
				"lsn", entry.LSN,
				"error", err,
			)
			return err
		}

		sentCount++
		if sentCount%10000 == 0 {
			s.log.Debug("replication progress",
				"follower_id", followerID,
				"sent_count", sentCount,
				"lsn", entry.LSN,
			)
		}
	}
}
