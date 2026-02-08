package replication

import (
	"encoding/json"
	"net/http"

	graphdb "github.com/mstrYoda/goraphdb"
)

// ---------------------------------------------------------------------------
// Write Forwarding Handler — leader side
//
// This HTTP handler accepts forwarded write operations from follower routers.
// It is mounted on the leader's HTTP server at /api/write.
//
// The handler deserializes the JSON request, executes the write operation
// on the leader's DB, and returns the result as JSON.
// ---------------------------------------------------------------------------

// WriteHandler returns an http.HandlerFunc that handles forwarded writes.
// Mount this on the leader's HTTP server at /api/write.
func WriteHandler(db *graphdb.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req writeOpRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, writeOpResponse{Error: "invalid request: " + err.Error()})
			return
		}

		var resp writeOpResponse

		switch req.Op {
		case "AddNode":
			id, err := db.AddNode(graphdb.Props(req.Props))
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.ID = uint64(id)
			}

		case "AddEdge":
			id, err := db.AddEdge(graphdb.NodeID(req.From), graphdb.NodeID(req.To), req.Label, graphdb.Props(req.Props))
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.ID = uint64(id)
			}

		case "DeleteNode":
			if err := db.DeleteNode(graphdb.NodeID(req.ID)); err != nil {
				resp.Error = err.Error()
			}

		case "DeleteEdge":
			if err := db.DeleteEdge(graphdb.EdgeID(req.ID)); err != nil {
				resp.Error = err.Error()
			}

		case "UpdateNode":
			if err := db.UpdateNode(graphdb.NodeID(req.ID), graphdb.Props(req.Props)); err != nil {
				resp.Error = err.Error()
			}

		case "UpdateEdge":
			if err := db.UpdateEdge(graphdb.EdgeID(req.ID), graphdb.Props(req.Props)); err != nil {
				resp.Error = err.Error()
			}

		default:
			resp.Error = "unknown operation: " + req.Op
		}

		writeJSON(w, resp)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
