// Package server provides an HTTP/JSON API for the graphdb management UI.
package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
// Server
// ---------------------------------------------------------------------------

// Server wraps a graphdb.DB and exposes an HTTP/JSON API.
// It also serves the React SPA static files when uiDir is set.
type Server struct {
	db    *graphdb.DB
	mux   *http.ServeMux
	uiDir string // path to ui/dist (empty = API-only mode)

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
	s.mux.HandleFunc("GET /api/nodes/{id}", s.handleGetNode)
	s.mux.HandleFunc("GET /api/nodes/{id}/neighborhood", s.handleNodeNeighborhood)
	s.mux.HandleFunc("POST /api/nodes", s.handleCreateNode)
	s.mux.HandleFunc("DELETE /api/nodes/{id}", s.handleDeleteNode)

	// Edges
	s.mux.HandleFunc("POST /api/edges", s.handleCreateEdge)
	s.mux.HandleFunc("DELETE /api/edges/{id}", s.handleDeleteEdge)

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
	result, err := s.db.Cypher(req.Query)
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
		iter, err = s.db.CypherStreamWithParams(req.Query, req.Params)
	} else {
		iter, err = s.db.CypherStream(req.Query)
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
		result, err = s.db.ExecutePreparedWithParams(pq, req.Params)
	} else {
		result, err = s.db.ExecutePrepared(pq)
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
	id, err := s.db.AddNode(req.Props)
	if err != nil {
		writeError(w, 500, err.Error())
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
	if err := s.db.DeleteNode(graphdb.NodeID(id)); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "deleted"})
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
	id, err := s.db.AddEdge(graphdb.NodeID(req.From), graphdb.NodeID(req.To), req.Label, req.Props)
	if err != nil {
		writeError(w, 500, err.Error())
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
	if err := s.db.DeleteEdge(graphdb.EdgeID(id)); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// FileSize returns the on-disk size of a file, or 0 on error.
func FileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
