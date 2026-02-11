// Package server provides an HTTP/JSON API for the graphdb management UI.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

// ---------------------------------------------------------------------------
// Write Forwarding Interface
// ---------------------------------------------------------------------------

// WriteForwarder abstracts cluster-aware write operations.
// In cluster mode, *replication.Router implements this interface and
// transparently forwards writes to the leader when this node is a follower.
// In standalone mode, this field is nil and the server writes to the local DB.
type WriteForwarder interface {
	AddNode(props graphdb.Props) (graphdb.NodeID, error)
	AddEdge(from, to graphdb.NodeID, label string, props graphdb.Props) (graphdb.EdgeID, error)
	DeleteNode(id graphdb.NodeID) error
	DeleteEdge(id graphdb.EdgeID) error
	UpdateNode(id graphdb.NodeID, props graphdb.Props) error
	UpdateEdge(id graphdb.EdgeID, props graphdb.Props) error
	Query(ctx context.Context, query string) (*graphdb.CypherResult, error)
}

// ---------------------------------------------------------------------------
// Cluster Info Interface
// ---------------------------------------------------------------------------

// ClusterInfoProvider provides cluster state information for the status API.
// *replication.ClusterManager implements this interface.
type ClusterInfoProvider interface {
	NodeID() string
	IsLeader() bool
	LeaderID() string
	HTTPAddr() string
	GRPCAddr() string
	RaftAddr() string
	// PeerHTTPAddrs returns a map of nodeID → HTTP address for all known peers (including self).
	PeerHTTPAddrs() map[string]string
	// WALLastLSN returns the last WAL LSN on this node (meaningful for leaders).
	WALLastLSN() uint64
	// AppliedLSN returns the last applied LSN on this node (meaningful for followers).
	AppliedLSN() uint64
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server wraps a graphdb.DB and exposes an HTTP/JSON API.
// It also serves the React SPA static files when uiDir is set.
//
// In cluster mode, a WriteForwarder (the Router) is set via SetRouter().
// Write operations are then transparently forwarded to the leader if this
// node is a follower. Read operations always execute locally.
type Server struct {
	db      *graphdb.DB
	router  WriteForwarder      // nil in standalone mode
	cluster ClusterInfoProvider // nil in standalone mode
	mux     *http.ServeMux
	uiDir   string // path to ui/dist (empty = API-only mode)

	// Prepared statement pool: stmtID → *graphdb.PreparedQuery
	stmts   map[string]*graphdb.PreparedQuery
	stmtsMu sync.RWMutex
}

// New creates a ready-to-use Server.
func New(db *graphdb.DB, uiDir string) *Server {
	s := &Server{
		db:    db,
		uiDir: uiDir,
		stmts: make(map[string]*graphdb.PreparedQuery),
	}
	s.mux = http.NewServeMux()
	s.routes()
	return s
}

// SetRouter sets the cluster-aware write forwarder.
// When set, write operations on follower nodes are transparently forwarded
// to the leader. Call this after cluster initialization.
func (s *Server) SetRouter(r WriteForwarder) {
	s.router = r
}

// SetCluster sets the cluster info provider for the /api/cluster endpoint.
func (s *Server) SetCluster(c ClusterInfoProvider) {
	s.cluster = c
}

// ServeHTTP implements http.Handler with CORS headers.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		return
	}
	s.mux.ServeHTTP(w, r)
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

func (s *Server) routes() {
	// Stats
	s.mux.HandleFunc("GET /api/stats", s.handleStats)

	// Indexes
	s.mux.HandleFunc("GET /api/indexes", s.handleListIndexes)
	s.mux.HandleFunc("POST /api/indexes", s.handleCreateIndex)
	s.mux.HandleFunc("DELETE /api/indexes/{name}", s.handleDropIndex)
	s.mux.HandleFunc("POST /api/indexes/{name}/reindex", s.handleReIndex)

	// Cypher
	s.mux.HandleFunc("POST /api/cypher", s.handleCypher)
	s.mux.HandleFunc("POST /api/cypher/stream", s.handleCypherStream)
	s.mux.HandleFunc("POST /api/cypher/prepare", s.handleCypherPrepare)
	s.mux.HandleFunc("POST /api/cypher/execute", s.handleCypherExecute)
	s.mux.HandleFunc("GET /api/cache/stats", s.handleCacheStats)

	// Nodes
	s.mux.HandleFunc("GET /api/nodes", s.handleListNodes)
	s.mux.HandleFunc("GET /api/nodes/cursor", s.handleListNodesCursor)
	s.mux.HandleFunc("GET /api/nodes/{id}", s.handleGetNode)
	s.mux.HandleFunc("GET /api/nodes/{id}/neighborhood", s.handleNodeNeighborhood)
	s.mux.HandleFunc("POST /api/nodes", s.handleCreateNode)
	s.mux.HandleFunc("PUT /api/nodes/{id}", s.handleUpdateNode)
	s.mux.HandleFunc("DELETE /api/nodes/{id}", s.handleDeleteNode)

	// Edges
	s.mux.HandleFunc("POST /api/edges", s.handleCreateEdge)
	s.mux.HandleFunc("GET /api/edges/cursor", s.handleListEdgesCursor)
	s.mux.HandleFunc("DELETE /api/edges/{id}", s.handleDeleteEdge)

	// Unique Constraints
	s.mux.HandleFunc("GET /api/constraints", s.handleListConstraints)
	s.mux.HandleFunc("POST /api/constraints", s.handleCreateConstraint)
	s.mux.HandleFunc("DELETE /api/constraints", s.handleDropConstraint)

	// Cluster & Health
	s.mux.HandleFunc("POST /api/write", s.handleForwardedWrite)
	s.mux.HandleFunc("GET /api/cluster", s.handleClusterStatus)
	s.mux.HandleFunc("GET /api/cluster/nodes", s.handleClusterNodes)
	s.mux.HandleFunc("GET /api/health", s.handleHealth)

	// Metrics & Observability
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /api/metrics", s.handleMetricsJSON)
	s.mux.HandleFunc("GET /api/slow-queries", s.handleSlowQueries)

	// SPA fallback — must be last.
	if s.uiDir != "" {
		s.mux.Handle("/", s.spaHandler())
	}
}

// ---------------------------------------------------------------------------
// SPA static file handler
// ---------------------------------------------------------------------------

func (s *Server) spaHandler() http.Handler {
	fsys := http.Dir(s.uiDir)
	fileServer := http.FileServer(fsys)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to open the requested path as a static file.
		p := r.URL.Path
		if p == "/" {
			http.ServeFile(w, r, filepath.Join(s.uiDir, "index.html"))
			return
		}
		f, err := fsys.Open(p)
		if err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}
		// Fallback: serve index.html so react-router can handle the route.
		http.ServeFile(w, r, filepath.Join(s.uiDir, "index.html"))
	})
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.db.Stats()
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, stats)
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

func (s *Server) handleListIndexes(w http.ResponseWriter, _ *http.Request) {
	indexes := s.db.ListIndexes()
	writeJSON(w, 200, map[string]any{"indexes": indexes})
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Property string `json:"property"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if req.Property == "" {
		writeError(w, 400, "property is required")
		return
	}
	if err := s.db.CreateIndex(req.Property); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]string{"status": "created", "property": req.Property})
}

func (s *Server) handleDropIndex(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.db.DropIndex(name); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "dropped", "property": name})
}

func (s *Server) handleReIndex(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.db.ReIndex(name); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "reindexed", "property": name})
}

// ---------------------------------------------------------------------------
// Cypher
// ---------------------------------------------------------------------------

type cypherResponse struct {
	Columns    []string         `json:"columns"`
	Rows       []map[string]any `json:"rows"`
	Graph      graphData        `json:"graph"`
	RowCount   int              `json:"rowCount"`
	ExecTimeMs float64          `json:"execTimeMs"`
}

type graphData struct {
	Nodes []graphNode `json:"nodes"`
	Edges []graphEdge `json:"edges"`
}

type graphNode struct {
	ID    uint64         `json:"id"`
	Props map[string]any `json:"props"`
	Label string         `json:"label"` // display label derived from props
}

type graphEdge struct {
	ID    uint64 `json:"id"`
	From  uint64 `json:"from"`
	To    uint64 `json:"to"`
	Label string `json:"label"`
}

func (s *Server) handleCypher(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeError(w, 400, "query is required")
		return
	}

	start := time.Now()

	// Use router if available — it transparently forwards write queries
	// to the leader when this node is a follower.
	var result *graphdb.CypherResult
	var err error
	if s.router != nil {
		result, err = s.router.Query(r.Context(), req.Query)
	} else {
		result, err = s.db.Cypher(r.Context(), req.Query)
	}
	elapsed := time.Since(start)

	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	rows := result.Rows
	if rows == nil {
		rows = []map[string]any{}
	}

	graph := s.extractGraphData(result)

	writeJSON(w, 200, cypherResponse{
		Columns:    result.Columns,
		Rows:       rows,
		Graph:      graph,
		RowCount:   len(rows),
		ExecTimeMs: float64(elapsed.Microseconds()) / 1000.0,
	})
}

// extractGraphData scans Cypher result rows for *Node values, then looks up
// the edges between discovered nodes to build a subgraph for visualization.
func (s *Server) extractGraphData(result *graphdb.CypherResult) graphData {
	gd := graphData{
		Nodes: []graphNode{},
		Edges: []graphEdge{},
	}
	nodeSet := make(map[uint64]bool)

	for _, row := range result.Rows {
		for _, val := range row {
			if n := extractNode(val); n != nil && !nodeSet[n.ID] {
				nodeSet[n.ID] = true
				gd.Nodes = append(gd.Nodes, *n)
			}
		}
	}

	// Discover edges between the collected nodes.
	for nid := range nodeSet {
		edges, err := s.db.OutEdges(graphdb.NodeID(nid))
		if err != nil {
			continue
		}
		for _, e := range edges {
			if nodeSet[uint64(e.To)] {
				gd.Edges = append(gd.Edges, graphEdge{
					ID:    uint64(e.ID),
					From:  uint64(e.From),
					To:    uint64(e.To),
					Label: e.Label,
				})
			}
		}
	}

	return gd
}

// extractNode tries to interpret a Cypher result value as a graph node.
func extractNode(val any) *graphNode {
	switch v := val.(type) {
	case *graphdb.Node:
		return &graphNode{
			ID:    uint64(v.ID),
			Props: v.Props,
			Label: nodeLabel(v.Props),
		}
	case map[string]any:
		// Serialized node: {"id": N, "props": {...}}
		idRaw, ok1 := v["id"]
		propsRaw, ok2 := v["props"]
		if !ok1 || !ok2 {
			return nil
		}
		var id uint64
		switch nid := idRaw.(type) {
		case float64:
			id = uint64(nid)
		case json.Number:
			i, _ := nid.Int64()
			id = uint64(i)
		default:
			return nil
		}
		props, _ := propsRaw.(map[string]any)
		return &graphNode{ID: id, Props: props, Label: nodeLabel(props)}
	}
	return nil
}

func nodeLabel(props map[string]any) string {
	for _, key := range []string{"name", "title", "label", "username"} {
		if v, ok := props[key]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Streaming Cypher (NDJSON)
// ---------------------------------------------------------------------------

func (s *Server) handleCypherStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query  string         `json:"query"`
		Params map[string]any `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeError(w, 400, "query is required")
		return
	}

	var iter graphdb.RowIterator
	var err error
	if len(req.Params) > 0 {
		iter, err = s.db.CypherStreamWithParams(r.Context(), req.Query, req.Params)
	} else {
		iter, err = s.db.CypherStream(r.Context(), req.Query)
	}
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	defer iter.Close()

	// Stream as newline-delimited JSON (NDJSON).
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Columns", strings.Join(iter.Columns(), ","))
	w.WriteHeader(200)

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)

	for iter.Next() {
		if encErr := enc.Encode(iter.Row()); encErr != nil {
			return // client disconnected
		}
		if canFlush {
			flusher.Flush()
		}
	}
	if iterErr := iter.Err(); iterErr != nil {
		// Best effort: write error as final NDJSON line.
		_ = enc.Encode(map[string]string{"error": iterErr.Error()})
	}
}

// ---------------------------------------------------------------------------
// Prepared Statements
// ---------------------------------------------------------------------------

func (s *Server) handleCypherPrepare(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeError(w, 400, "query is required")
		return
	}

	pq, err := s.db.PrepareCypher(req.Query)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	// Generate a deterministic statement ID from the query hash.
	h := sha256.Sum256([]byte(req.Query))
	stmtID := hex.EncodeToString(h[:8]) // 16-char hex

	s.stmtsMu.Lock()
	s.stmts[stmtID] = pq
	s.stmtsMu.Unlock()

	writeJSON(w, 200, map[string]string{
		"stmt_id": stmtID,
		"query":   req.Query,
		"status":  "prepared",
	})
}

func (s *Server) handleCypherExecute(w http.ResponseWriter, r *http.Request) {
	var req struct {
		StmtID string         `json:"stmt_id"`
		Params map[string]any `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if req.StmtID == "" {
		writeError(w, 400, "stmt_id is required")
		return
	}

	s.stmtsMu.RLock()
	pq, ok := s.stmts[req.StmtID]
	s.stmtsMu.RUnlock()

	if !ok {
		writeError(w, 404, "statement not found — call /api/cypher/prepare first")
		return
	}

	start := time.Now()
	var result *graphdb.CypherResult
	var err error
	if len(req.Params) > 0 {
		result, err = s.db.ExecutePreparedWithParams(r.Context(), pq, req.Params)
	} else {
		result, err = s.db.ExecutePrepared(r.Context(), pq)
	}
	elapsed := time.Since(start)

	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	rows := result.Rows
	if rows == nil {
		rows = []map[string]any{}
	}

	graph := s.extractGraphData(result)

	writeJSON(w, 200, cypherResponse{
		Columns:    result.Columns,
		Rows:       rows,
		Graph:      graph,
		RowCount:   len(rows),
		ExecTimeMs: float64(elapsed.Microseconds()) / 1000.0,
	})
}

func (s *Server) handleCacheStats(w http.ResponseWriter, _ *http.Request) {
	cacheStats := s.db.QueryCacheStats()

	s.stmtsMu.RLock()
	stmtCount := len(s.stmts)
	s.stmtsMu.RUnlock()

	writeJSON(w, 200, map[string]any{
		"query_cache":         cacheStats,
		"prepared_statements": stmtCount,
	})
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	limit := intQuery(r, "limit", 50)
	offset := intQuery(r, "offset", 0)

	type nodeJSON struct {
		ID    uint64         `json:"id"`
		Props map[string]any `json:"props"`
	}

	var nodes []nodeJSON
	idx := 0
	_ = s.db.ForEachNode(func(n *graphdb.Node) error {
		if len(nodes) >= limit {
			return fmt.Errorf("stop")
		}
		if idx >= offset {
			nodes = append(nodes, nodeJSON{ID: uint64(n.ID), Props: n.Props})
		}
		idx++
		return nil
	})
	if nodes == nil {
		nodes = []nodeJSON{}
	}

	writeJSON(w, 200, map[string]any{
		"nodes":  nodes,
		"total":  s.db.NodeCount(),
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, 400, "invalid node id")
		return
	}
	node, err := s.db.GetNode(graphdb.NodeID(id))
	if err != nil {
		writeError(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, node)
}

// handleNodeNeighborhood returns a node, its edges, and all neighbor nodes
// in a single response — ideal for graph visualization.
func (s *Server) handleNodeNeighborhood(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, 400, "invalid node id")
		return
	}

	center, err := s.db.GetNode(graphdb.NodeID(id))
	if err != nil {
		writeError(w, 404, err.Error())
		return
	}

	allEdges, err := s.db.Edges(graphdb.NodeID(id))
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	edges := make([]graphEdge, 0, len(allEdges))
	neighborIDs := make(map[uint64]bool)
	for _, e := range allEdges {
		edges = append(edges, graphEdge{
			ID: uint64(e.ID), From: uint64(e.From), To: uint64(e.To), Label: e.Label,
		})
		if uint64(e.From) != id {
			neighborIDs[uint64(e.From)] = true
		}
		if uint64(e.To) != id {
			neighborIDs[uint64(e.To)] = true
		}
	}

	neighbors := make([]graphNode, 0, len(neighborIDs))
	for nid := range neighborIDs {
		n, err := s.db.GetNode(graphdb.NodeID(nid))
		if err != nil {
			neighbors = append(neighbors, graphNode{ID: nid, Props: map[string]any{}, Label: fmt.Sprintf("Node %d", nid)})
			continue
		}
		neighbors = append(neighbors, graphNode{
			ID: uint64(n.ID), Props: n.Props, Label: nodeLabel(n.Props),
		})
	}

	writeJSON(w, 200, map[string]any{
		"center": graphNode{
			ID: uint64(center.ID), Props: center.Props, Label: nodeLabel(center.Props),
		},
		"neighbors": neighbors,
		"edges":     edges,
	})
}

func (s *Server) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Props map[string]any `json:"props"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}

	var id graphdb.NodeID
	var err error
	if s.router != nil {
		id, err = s.router.AddNode(req.Props)
	} else {
		id, err = s.db.AddNode(req.Props)
	}
	if err != nil {
		status := 500
		if errors.Is(err, graphdb.ErrReadOnlyReplica) {
			status = 503
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, 400, "invalid node id")
		return
	}
	var delErr error
	if s.router != nil {
		delErr = s.router.DeleteNode(graphdb.NodeID(id))
	} else {
		delErr = s.db.DeleteNode(graphdb.NodeID(id))
	}
	if delErr != nil {
		status := 500
		if errors.Is(delErr, graphdb.ErrReadOnlyReplica) {
			status = 503
		}
		writeError(w, status, delErr.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

func (s *Server) handleUpdateNode(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, 400, "invalid node id")
		return
	}

	var req struct {
		Props map[string]any `json:"props"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}

	var updateErr error
	if s.router != nil {
		updateErr = s.router.UpdateNode(graphdb.NodeID(id), req.Props)
	} else {
		updateErr = s.db.UpdateNode(graphdb.NodeID(id), req.Props)
	}
	if updateErr != nil {
		status := 500
		if errors.Is(updateErr, graphdb.ErrReadOnlyReplica) {
			status = 503
		}
		writeError(w, status, updateErr.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "updated"})
}

// ---------------------------------------------------------------------------
// Edges
// ---------------------------------------------------------------------------

func (s *Server) handleCreateEdge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From  uint64         `json:"from"`
		To    uint64         `json:"to"`
		Label string         `json:"label"`
		Props map[string]any `json:"props"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if req.Label == "" {
		writeError(w, 400, "label is required")
		return
	}

	var id graphdb.EdgeID
	var err error
	if s.router != nil {
		id, err = s.router.AddEdge(graphdb.NodeID(req.From), graphdb.NodeID(req.To), req.Label, req.Props)
	} else {
		id, err = s.db.AddEdge(graphdb.NodeID(req.From), graphdb.NodeID(req.To), req.Label, req.Props)
	}
	if err != nil {
		status := 500
		if errors.Is(err, graphdb.ErrReadOnlyReplica) {
			status = 503
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleDeleteEdge(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, 400, "invalid edge id")
		return
	}
	var delErr error
	if s.router != nil {
		delErr = s.router.DeleteEdge(graphdb.EdgeID(id))
	} else {
		delErr = s.db.DeleteEdge(graphdb.EdgeID(id))
	}
	if delErr != nil {
		status := 500
		if errors.Is(delErr, graphdb.ErrReadOnlyReplica) {
			status = 503
		}
		writeError(w, status, delErr.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

// ---------------------------------------------------------------------------
// Cursor Pagination
// ---------------------------------------------------------------------------

func (s *Server) handleListNodesCursor(w http.ResponseWriter, r *http.Request) {
	cursor := uint64Query(r, "cursor", 0)
	limit := intQuery(r, "limit", 50)

	page, err := s.db.ListNodes(graphdb.NodeID(cursor), limit)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	type nodeJSON struct {
		ID     uint64         `json:"id"`
		Labels []string       `json:"labels,omitempty"`
		Props  map[string]any `json:"props"`
	}

	nodes := make([]nodeJSON, len(page.Nodes))
	for i, n := range page.Nodes {
		nodes[i] = nodeJSON{ID: uint64(n.ID), Labels: n.Labels, Props: n.Props}
	}

	writeJSON(w, 200, map[string]any{
		"nodes":       nodes,
		"next_cursor": page.NextCursor,
		"has_more":    page.HasMore,
		"limit":       limit,
	})
}

func (s *Server) handleListEdgesCursor(w http.ResponseWriter, r *http.Request) {
	cursor := uint64Query(r, "cursor", 0)
	limit := intQuery(r, "limit", 50)

	page, err := s.db.ListEdges(graphdb.EdgeID(cursor), limit)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	type edgeJSON struct {
		ID    uint64         `json:"id"`
		From  uint64         `json:"from"`
		To    uint64         `json:"to"`
		Label string         `json:"label"`
		Props map[string]any `json:"props,omitempty"`
	}

	edges := make([]edgeJSON, len(page.Edges))
	for i, e := range page.Edges {
		edges[i] = edgeJSON{ID: uint64(e.ID), From: uint64(e.From), To: uint64(e.To), Label: e.Label, Props: e.Props}
	}

	writeJSON(w, 200, map[string]any{
		"edges":       edges,
		"next_cursor": page.NextCursor,
		"has_more":    page.HasMore,
		"limit":       limit,
	})
}

// ---------------------------------------------------------------------------
// Prometheus Metrics
// ---------------------------------------------------------------------------

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	m := s.db.Metrics()
	if m == nil {
		writeError(w, 500, "metrics not initialized")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m.WritePrometheus(w)
}

func (s *Server) handleMetricsJSON(w http.ResponseWriter, _ *http.Request) {
	m := s.db.Metrics()
	if m == nil {
		writeError(w, 500, "metrics not initialized")
		return
	}
	writeJSON(w, 200, m.Snapshot())
}

func (s *Server) handleSlowQueries(w http.ResponseWriter, r *http.Request) {
	limit := intQuery(r, "limit", 50)
	entries := s.db.SlowQueries(limit)
	if entries == nil {
		entries = []graphdb.SlowQueryEntry{}
	}
	writeJSON(w, 200, map[string]any{
		"queries": entries,
		"count":   len(entries),
	})
}

// ---------------------------------------------------------------------------
// Cluster Status
// ---------------------------------------------------------------------------

// handleClusterStatus returns the current cluster state of this node.
// In standalone mode (no cluster configured), returns mode=standalone.
// In cluster mode, returns the node ID, role, leader ID, replication state, and addresses.
func (s *Server) handleClusterStatus(w http.ResponseWriter, _ *http.Request) {
	if s.cluster == nil {
		writeJSON(w, 200, map[string]any{
			"mode": "standalone",
			"role": s.db.Role(),
		})
		return
	}

	role := "follower"
	if s.cluster.IsLeader() {
		role = "leader"
	}

	writeJSON(w, 200, map[string]any{
		"mode":         "cluster",
		"node_id":      s.cluster.NodeID(),
		"role":         role,
		"leader_id":    s.cluster.LeaderID(),
		"db_role":      s.db.Role(),
		"http_addr":    s.cluster.HTTPAddr(),
		"grpc_addr":    s.cluster.GRPCAddr(),
		"raft_addr":    s.cluster.RaftAddr(),
		"wal_last_lsn": s.cluster.WALLastLSN(),
		"applied_lsn":  s.cluster.AppliedLSN(),
	})
}

// ---------------------------------------------------------------------------
// Cluster Nodes Aggregator
// ---------------------------------------------------------------------------

// clusterNodeInfo is the per-node JSON returned by the aggregator endpoint.
type clusterNodeInfo struct {
	NodeID      string         `json:"node_id"`
	Role        string         `json:"role"`
	Status      string         `json:"status"`
	Readable    bool           `json:"readable"`
	Writable    bool           `json:"writable"`
	HTTPAddr    string         `json:"http_addr"`
	GRPCAddr    string         `json:"grpc_addr,omitempty"`
	RaftAddr    string         `json:"raft_addr,omitempty"`
	Stats       map[string]any `json:"stats,omitempty"`
	Replication map[string]any `json:"replication,omitempty"`
	Metrics     map[string]any `json:"metrics,omitempty"`
	Reachable   bool           `json:"reachable"`
	Error       string         `json:"error,omitempty"`
}

// handleClusterNodes aggregates health, stats, metrics, and replication info
// from all known cluster peers. It proxies HTTP calls to each peer's API and
// returns a combined response — so the UI only needs to talk to one node.
//
// In standalone mode, returns only this node's information.
func (s *Server) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	// Standalone mode — return just this node.
	if s.cluster == nil {
		stats, _ := s.db.Stats()
		var metricsData map[string]any
		if m := s.db.Metrics(); m != nil {
			metricsData = m.Snapshot()
		}
		var statsData map[string]any
		if stats != nil {
			statsData = map[string]any{
				"node_count":      stats.NodeCount,
				"edge_count":      stats.EdgeCount,
				"shard_count":     stats.ShardCount,
				"disk_size_bytes": stats.DiskSizeBytes,
			}
		}
		node := clusterNodeInfo{
			NodeID:    "standalone",
			Role:      "standalone",
			Status:    "ok",
			Readable:  true,
			Writable:  true,
			Stats:     statsData,
			Metrics:   metricsData,
			Reachable: true,
		}
		writeJSON(w, 200, map[string]any{
			"mode":      "standalone",
			"self":      "standalone",
			"leader_id": "",
			"nodes":     []clusterNodeInfo{node},
		})
		return
	}

	// Cluster mode — aggregate from all peers.
	selfID := s.cluster.NodeID()
	leaderID := s.cluster.LeaderID()
	peerAddrs := s.cluster.PeerHTTPAddrs()

	// Use a short timeout for peer requests.
	httpClient := &http.Client{Timeout: 3 * time.Second}

	type result struct {
		nodeID string
		info   clusterNodeInfo
	}

	var wg sync.WaitGroup
	results := make(chan result, len(peerAddrs))

	for nodeID, httpAddr := range peerAddrs {
		wg.Add(1)
		go func(nid, addr string) {
			defer wg.Done()
			info := s.fetchPeerInfo(r.Context(), httpClient, nid, addr, leaderID)
			results <- result{nodeID: nid, info: info}
		}(nodeID, httpAddr)
	}

	wg.Wait()
	close(results)

	nodes := make([]clusterNodeInfo, 0, len(peerAddrs))
	for res := range results {
		nodes = append(nodes, res.info)
	}

	// Sort nodes: leader first, then by node ID.
	sortClusterNodes(nodes, leaderID)

	writeJSON(w, 200, map[string]any{
		"mode":      "cluster",
		"self":      selfID,
		"leader_id": leaderID,
		"nodes":     nodes,
	})
}

// fetchPeerInfo fetches health, stats, and metrics from a single peer node.
func (s *Server) fetchPeerInfo(ctx context.Context, client *http.Client, nodeID, httpAddr, leaderID string) clusterNodeInfo {
	info := clusterNodeInfo{
		NodeID:   nodeID,
		HTTPAddr: httpAddr,
	}

	if httpAddr == "" {
		info.Status = "unreachable"
		info.Error = "no HTTP address configured"
		return info
	}

	// Normalize HTTP address.
	base := httpAddr
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}

	// Fetch health.
	healthData, err := s.fetchPeerJSON(ctx, client, base+"/api/health")
	if err != nil {
		info.Status = "unreachable"
		info.Reachable = false
		info.Error = err.Error()
		return info
	}
	info.Reachable = true

	// Parse health response.
	if status, ok := healthData["status"].(string); ok {
		info.Status = status
	}
	if role, ok := healthData["role"].(string); ok {
		info.Role = role
	}
	if readable, ok := healthData["readable"].(bool); ok {
		info.Readable = readable
	}
	if writable, ok := healthData["writable"].(bool); ok {
		info.Writable = writable
	}

	// Fetch cluster status (for replication info and addresses).
	clusterData, err := s.fetchPeerJSON(ctx, client, base+"/api/cluster")
	if err == nil {
		repl := make(map[string]any)
		if walLSN, ok := clusterData["wal_last_lsn"]; ok {
			repl["wal_last_lsn"] = walLSN
		}
		if appliedLSN, ok := clusterData["applied_lsn"]; ok {
			repl["applied_lsn"] = appliedLSN
		}
		info.Replication = repl

		if grpc, ok := clusterData["grpc_addr"].(string); ok {
			info.GRPCAddr = grpc
		}
		if raft, ok := clusterData["raft_addr"].(string); ok {
			info.RaftAddr = raft
		}
	}

	// Fetch stats.
	statsData, err := s.fetchPeerJSON(ctx, client, base+"/api/stats")
	if err == nil {
		info.Stats = statsData
	}

	// Fetch metrics.
	metricsData, err := s.fetchPeerJSON(ctx, client, base+"/api/metrics")
	if err == nil {
		info.Metrics = metricsData
	}

	return info
}

// fetchPeerJSON makes a GET request to a peer and returns the decoded JSON body.
func (s *Server) fetchPeerJSON(ctx context.Context, client *http.Client, url string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit
	if err != nil {
		return nil, err
	}

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	return data, nil
}

// sortClusterNodes sorts cluster nodes: leader first, then alphabetically by node ID.
func sortClusterNodes(nodes []clusterNodeInfo, leaderID string) {
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			swap := false
			if nodes[j].NodeID == leaderID && nodes[i].NodeID != leaderID {
				swap = true
			} else if nodes[i].NodeID != leaderID && nodes[j].NodeID != leaderID && nodes[i].NodeID > nodes[j].NodeID {
				swap = true
			}
			if swap {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Health / Readiness
// ---------------------------------------------------------------------------

// handleHealth returns a health check response suitable for load balancers.
//
// Response codes:
//   - 200 OK: node is healthy and ready to serve traffic
//   - 503 Service Unavailable: node is completely unavailable (DB closed)
//
// The response body includes the node status and role for intelligent routing:
//
//   - status="ok"       — fully operational (reads + writes)
//   - status="readonly" — reads work, writes unavailable (no leader / quorum lost)
//   - role="leader"     — accepts both reads and writes
//   - role="follower"   — accepts reads, forwards writes to leader
//   - role="standalone" — accepts both reads and writes (no cluster)
//
// Load balancer configuration examples (HAProxy):
//
//	# Route writes only to the leader:
//	backend graphdb_write
//	  option httpchk GET /api/health
//	  http-check expect string "leader"
//
//	# Route reads to any healthy node (including leaderless followers):
//	backend graphdb_read
//	  option httpchk GET /api/health
//	  http-check expect rstatus 200
//
//	# Route reads only to fully-operational nodes:
//	backend graphdb_read_strict
//	  option httpchk GET /api/health
//	  http-check expect string "\"status\":\"ok\""
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	// Check if DB is open and operational.
	if s.db.IsClosed() {
		writeJSON(w, 503, map[string]any{
			"status": "unavailable",
			"reason": "database is closed",
		})
		return
	}

	role := "standalone"
	nodeID := ""
	leaderID := ""
	status := "ok"
	readable := true
	writable := true

	if s.cluster != nil {
		nodeID = s.cluster.NodeID()
		leaderID = s.cluster.LeaderID()
		if s.cluster.IsLeader() {
			role = "leader"
		} else {
			role = "follower"
		}

		// No leader means writes cannot be processed (no forwarding target),
		// but reads still work from local data.
		if leaderID == "" {
			status = "readonly"
			writable = false
		}
	}

	resp := map[string]any{
		"status":   status,
		"role":     role,
		"readable": readable,
		"writable": writable,
	}
	if nodeID != "" {
		resp["node_id"] = nodeID
	}
	if leaderID != "" {
		resp["leader_id"] = leaderID
	}

	writeJSON(w, 200, resp)
}

// ---------------------------------------------------------------------------
// Forwarded Write Operations (cluster mode)
// ---------------------------------------------------------------------------

// writeOpRequest is the JSON payload sent by a follower's Router when
// forwarding a write operation to the leader's HTTP API.
type writeOpRequest struct {
	Op    string         `json:"op"`
	ID    uint64         `json:"id,omitempty"`
	From  uint64         `json:"from,omitempty"`
	To    uint64         `json:"to,omitempty"`
	Label string         `json:"label,omitempty"`
	Props map[string]any `json:"props,omitempty"`
}

// writeOpResponse is the JSON response returned to the forwarding router.
type writeOpResponse struct {
	ID    uint64 `json:"id,omitempty"`
	Error string `json:"error,omitempty"`
}

// handleForwardedWrite handles write operations forwarded from follower nodes.
// This endpoint ALWAYS executes against the local DB — it is the "landing zone"
// for writes that a follower's Router forwarded to the leader. This avoids
// any re-routing loops since we bypass the Router entirely.
func (s *Server) handleForwardedWrite(w http.ResponseWriter, r *http.Request) {
	var req writeOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, writeOpResponse{Error: "invalid JSON body"})
		return
	}

	var respID uint64
	var opErr error

	switch req.Op {
	case "AddNode":
		var id graphdb.NodeID
		id, opErr = s.db.AddNode(req.Props)
		respID = uint64(id)

	case "AddEdge":
		var id graphdb.EdgeID
		id, opErr = s.db.AddEdge(graphdb.NodeID(req.From), graphdb.NodeID(req.To), req.Label, req.Props)
		respID = uint64(id)

	case "DeleteNode":
		opErr = s.db.DeleteNode(graphdb.NodeID(req.ID))

	case "DeleteEdge":
		opErr = s.db.DeleteEdge(graphdb.EdgeID(req.ID))

	case "UpdateNode":
		opErr = s.db.UpdateNode(graphdb.NodeID(req.ID), req.Props)

	case "UpdateEdge":
		opErr = s.db.UpdateEdge(graphdb.EdgeID(req.ID), req.Props)

	default:
		writeJSON(w, 400, writeOpResponse{Error: fmt.Sprintf("unknown write op: %s", req.Op)})
		return
	}

	if opErr != nil {
		writeJSON(w, 500, writeOpResponse{Error: opErr.Error()})
		return
	}

	writeJSON(w, 200, writeOpResponse{ID: respID})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func uint64Query(r *http.Request, key string, fallback uint64) uint64 {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func intQuery(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

// ---------------------------------------------------------------------------
// Unique Constraints
// ---------------------------------------------------------------------------

// handleListConstraints returns all registered unique constraints.
func (s *Server) handleListConstraints(w http.ResponseWriter, _ *http.Request) {
	constraints := s.db.ListUniqueConstraints()
	writeJSON(w, 200, map[string]any{
		"constraints": constraints,
	})
}

// handleCreateConstraint creates a unique constraint on (label, property).
func (s *Server) handleCreateConstraint(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Label    string `json:"label"`
		Property string `json:"property"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON body"})
		return
	}
	if body.Label == "" || body.Property == "" {
		writeJSON(w, 400, map[string]string{"error": "label and property are required"})
		return
	}

	if err := s.db.CreateUniqueConstraint(body.Label, body.Property); err != nil {
		writeJSON(w, 409, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "created", "label": body.Label, "property": body.Property})
}

// handleDropConstraint drops a unique constraint on (label, property).
func (s *Server) handleDropConstraint(w http.ResponseWriter, r *http.Request) {
	label := r.URL.Query().Get("label")
	property := r.URL.Query().Get("property")
	if label == "" || property == "" {
		writeJSON(w, 400, map[string]string{"error": "label and property query params are required"})
		return
	}

	if err := s.db.DropUniqueConstraint(label, property); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "dropped", "label": label, "property": property})
}

// FileSize returns the on-disk size of a file, or 0 on error.
func FileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
