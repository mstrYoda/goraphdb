package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

// ---------------------------------------------------------------------------
// Query Router — read/write routing with write forwarding
//
// The router sits in front of the local DB and the cluster. It decides
// whether a query should be handled locally or forwarded to the leader.
//
// Routing rules:
//   - Read queries (MATCH, GetNode, GetEdge, etc.) → handled locally
//     on any node (leader or follower). For external clients, a load
//     balancer distributes reads across replicas.
//   - Write queries (CREATE, AddNode, AddEdge, etc.) →
//     if local node is leader: execute locally
//     if local node is follower: forward to leader via HTTP
//
// Architecture:
//
//	Client Request
//	     │
//	     ▼
//	┌──────────────────────────────┐
//	│  Router.Query(cypher)        │
//	│  Router.AddNode(props)       │
//	│  Router.AddEdge(...)         │
//	│    │                         │
//	│    ├─ read? → local DB       │
//	│    │                         │
//	│    └─ write?                 │
//	│         ├─ I'm leader → DB   │
//	│         └─ I'm follower      │
//	│              → forward to    │
//	│                leader HTTP   │
//	└──────────────────────────────┘
//
// Write forwarding uses a simple HTTP JSON API to the leader's server.
// This keeps the protocol simple and leverages the existing HTTP server.
// ---------------------------------------------------------------------------

var (
	// ErrNoLeader is returned when no leader is known for write forwarding.
	ErrNoLeader = errors.New("router: no leader available for write forwarding")
	// ErrForwardFailed is returned when write forwarding to the leader fails.
	ErrForwardFailed = errors.New("router: write forwarding failed")
)

// Router routes queries to the appropriate node in the cluster.
type Router struct {
	db       *graphdb.DB
	log      *slog.Logger
	election *Election // nil if no election configured

	mu             sync.RWMutex
	leaderHTTPAddr string // HTTP address of the leader for write forwarding
	httpClient     *http.Client

	// Round-robin counter for follower read distribution (future use).
	readCounter atomic.Uint64
}

// RouterOption configures the Router.
type RouterOption func(*Router)

// WithElection sets the election module for automatic leader discovery.
func WithElection(e *Election) RouterOption {
	return func(r *Router) { r.election = e }
}

// WithLeaderHTTPAddr sets the HTTP address of the leader for write forwarding.
// This can be set statically or updated dynamically via SetLeaderHTTPAddr.
func WithLeaderHTTPAddr(addr string) RouterOption {
	return func(r *Router) { r.leaderHTTPAddr = addr }
}

// WithRouterLogger sets the logger for the router.
func WithRouterLogger(l *slog.Logger) RouterOption {
	return func(r *Router) { r.log = l }
}

// NewRouter creates a query router for the given database.
func NewRouter(db *graphdb.DB, opts ...RouterOption) *Router {
	r := &Router{
		db:  db,
		log: slog.Default(),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// SetLeaderHTTPAddr updates the leader's HTTP address for write forwarding.
// Called when the leader changes (e.g., from the election callback).
func (r *Router) SetLeaderHTTPAddr(addr string) {
	r.mu.Lock()
	r.leaderHTTPAddr = addr
	r.mu.Unlock()
}

// LeaderHTTPAddr returns the current leader's HTTP address.
func (r *Router) LeaderHTTPAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderHTTPAddr
}

// IsLocalLeader returns true if the local DB is the current leader.
func (r *Router) IsLocalLeader() bool {
	return r.db.Role() == "leader"
}

// ---------------------------------------------------------------------------
// Cypher query routing
// ---------------------------------------------------------------------------

// CypherQueryRequest is the JSON payload for forwarded Cypher queries.
type CypherQueryRequest struct {
	Query  string         `json:"query"`
	Params map[string]any `json:"params,omitempty"`
}

// CypherQueryResponse is the JSON response from forwarded Cypher queries.
type CypherQueryResponse struct {
	Columns []string         `json:"columns,omitempty"`
	Rows    []map[string]any `json:"rows,omitempty"`
	Error   string           `json:"error,omitempty"`
}

// Query executes a Cypher query, routing it appropriately.
// Read queries are always executed locally.
// Write queries are forwarded to the leader if this node is a follower.
func (r *Router) Query(ctx context.Context, query string) (*graphdb.CypherResult, error) {
	// Try local execution first.
	result, err := r.db.Cypher(ctx, query)
	if err == nil {
		return result, nil
	}

	// If the error is ErrReadOnlyReplica, forward to leader.
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardCypherQuery(ctx, CypherQueryRequest{Query: query})
	}

	return nil, err
}

// QueryWithParams executes a parameterized Cypher query with routing.
func (r *Router) QueryWithParams(ctx context.Context, query string, params map[string]any) (*graphdb.CypherResult, error) {
	result, err := r.db.CypherWithParams(ctx, query, params)
	if err == nil {
		return result, nil
	}

	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardCypherQuery(ctx, CypherQueryRequest{Query: query, Params: params})
	}

	return nil, err
}

// ---------------------------------------------------------------------------
// Write operation routing
// ---------------------------------------------------------------------------

// writeOpRequest is the JSON payload for forwarded write operations.
type writeOpRequest struct {
	Op        string           `json:"op"`
	ID        uint64           `json:"id,omitempty"`
	From      uint64           `json:"from,omitempty"`
	To        uint64           `json:"to,omitempty"`
	Label     string           `json:"label,omitempty"`
	Labels    []string         `json:"labels,omitempty"`
	Props     map[string]any   `json:"props,omitempty"`
	Nodes     []map[string]any `json:"nodes,omitempty"`
	PropName  string           `json:"prop_name,omitempty"`
	PropNames []string         `json:"prop_names,omitempty"`
}

// writeOpResponse is the JSON response from forwarded write operations.
type writeOpResponse struct {
	ID    uint64   `json:"id,omitempty"`
	IDs   []uint64 `json:"ids,omitempty"`
	Error string   `json:"error,omitempty"`
}

// AddNode routes an AddNode operation.
func (r *Router) AddNode(props graphdb.Props) (graphdb.NodeID, error) {
	id, err := r.db.AddNode(props)
	if err == nil {
		return id, nil
	}
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardAddNode(props)
	}
	return 0, err
}

// AddEdge routes an AddEdge operation.
func (r *Router) AddEdge(from, to graphdb.NodeID, label string, props graphdb.Props) (graphdb.EdgeID, error) {
	id, err := r.db.AddEdge(from, to, label, props)
	if err == nil {
		return id, nil
	}
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardAddEdge(from, to, label, props)
	}
	return 0, err
}

// DeleteNode routes a DeleteNode operation.
func (r *Router) DeleteNode(id graphdb.NodeID) error {
	err := r.db.DeleteNode(id)
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardSimpleWrite("DeleteNode", uint64(id))
	}
	return err
}

// DeleteEdge routes a DeleteEdge operation.
func (r *Router) DeleteEdge(id graphdb.EdgeID) error {
	err := r.db.DeleteEdge(id)
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardSimpleWrite("DeleteEdge", uint64(id))
	}
	return err
}

// UpdateNode routes an UpdateNode operation.
func (r *Router) UpdateNode(id graphdb.NodeID, props graphdb.Props) error {
	err := r.db.UpdateNode(id, props)
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardUpdateWrite("UpdateNode", uint64(id), props)
	}
	return err
}

// UpdateEdge routes an UpdateEdge operation.
func (r *Router) UpdateEdge(id graphdb.EdgeID, props graphdb.Props) error {
	err := r.db.UpdateEdge(id, props)
	if errors.Is(err, graphdb.ErrReadOnlyReplica) {
		return r.forwardUpdateWrite("UpdateEdge", uint64(id), props)
	}
	return err
}

// ---------------------------------------------------------------------------
// Read operations — always local
// ---------------------------------------------------------------------------

// GetNode reads a node locally (works on any replica).
func (r *Router) GetNode(id graphdb.NodeID) (*graphdb.Node, error) {
	return r.db.GetNode(id)
}

// GetEdge reads an edge locally (works on any replica).
func (r *Router) GetEdge(id graphdb.EdgeID) (*graphdb.Edge, error) {
	return r.db.GetEdge(id)
}

// OutEdges reads outgoing edges locally.
func (r *Router) OutEdges(id graphdb.NodeID) ([]*graphdb.Edge, error) {
	return r.db.OutEdges(id)
}

// InEdges reads incoming edges locally.
func (r *Router) InEdges(id graphdb.NodeID) ([]*graphdb.Edge, error) {
	return r.db.InEdges(id)
}

// ---------------------------------------------------------------------------
// Forwarding helpers
// ---------------------------------------------------------------------------

func (r *Router) leaderAddr() (string, error) {
	r.mu.RLock()
	addr := r.leaderHTTPAddr
	r.mu.RUnlock()
	if addr != "" {
		return addr, nil
	}
	return "", ErrNoLeader
}

func (r *Router) forwardCypherQuery(ctx context.Context, req CypherQueryRequest) (*graphdb.CypherResult, error) {
	addr, err := r.leaderAddr()
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("router: marshal query: %w", err)
	}

	url := fmt.Sprintf("%s/api/cypher", addr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("router: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Forwarded-By", "graphdb-router")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrForwardFailed, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("router: read response: %w", err)
	}

	var cypherResp CypherQueryResponse
	if err := json.Unmarshal(respBody, &cypherResp); err != nil {
		return nil, fmt.Errorf("router: unmarshal response: %w", err)
	}

	if cypherResp.Error != "" {
		return nil, fmt.Errorf("router: leader error: %s", cypherResp.Error)
	}

	return &graphdb.CypherResult{
		Columns: cypherResp.Columns,
		Rows:    cypherResp.Rows,
	}, nil
}

func (r *Router) forwardAddNode(props graphdb.Props) (graphdb.NodeID, error) {
	addr, err := r.leaderAddr()
	if err != nil {
		return 0, err
	}

	body, _ := json.Marshal(writeOpRequest{Op: "AddNode", Props: props})
	resp, err := r.postJSON(addr+"/api/write", body)
	if err != nil {
		return 0, err
	}
	return graphdb.NodeID(resp.ID), nil
}

func (r *Router) forwardAddEdge(from, to graphdb.NodeID, label string, props graphdb.Props) (graphdb.EdgeID, error) {
	addr, err := r.leaderAddr()
	if err != nil {
		return 0, err
	}

	body, _ := json.Marshal(writeOpRequest{
		Op: "AddEdge", From: uint64(from), To: uint64(to), Label: label, Props: props,
	})
	resp, err := r.postJSON(addr+"/api/write", body)
	if err != nil {
		return 0, err
	}
	return graphdb.EdgeID(resp.ID), nil
}

func (r *Router) forwardSimpleWrite(op string, id uint64) error {
	addr, err := r.leaderAddr()
	if err != nil {
		return err
	}

	body, _ := json.Marshal(writeOpRequest{Op: op, ID: id})
	_, err = r.postJSON(addr+"/api/write", body)
	return err
}

func (r *Router) forwardUpdateWrite(op string, id uint64, props graphdb.Props) error {
	addr, err := r.leaderAddr()
	if err != nil {
		return err
	}

	body, _ := json.Marshal(writeOpRequest{Op: op, ID: id, Props: props})
	_, err = r.postJSON(addr+"/api/write", body)
	return err
}

func (r *Router) postJSON(url string, body []byte) (*writeOpResponse, error) {
	resp, err := r.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrForwardFailed, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("router: read response: %w", err)
	}

	var writeResp writeOpResponse
	if err := json.Unmarshal(respBody, &writeResp); err != nil {
		return nil, fmt.Errorf("router: unmarshal response: %w", err)
	}

	if writeResp.Error != "" {
		return nil, fmt.Errorf("router: leader error: %s", writeResp.Error)
	}

	return &writeResp, nil
}
