# ============================================================================
# GoraphDB — Production Multi-Stage Dockerfile
#
# Three-stage build:
#   1. Node.js:  Build the React management UI (Vite + Tailwind)
#   2. Go:       Build the goraphdb-ui binary (HTTP server + Cypher engine
#                + Raft cluster + gRPC replication)
#   3. Runtime:  Minimal image with just the binary + static UI assets
#
# The final image contains:
#   /usr/local/bin/graphdb-ui  — the server binary
#   /ui/dist/                  — built React SPA static files
#   /data/db/                  — default data directory (mount a PVC here)
#
# Usage:
#   docker build -t goraphdb:latest .
#   docker run -p 7474:7474 -v goraphdb-data:/data/db goraphdb:latest
#
# Cluster mode (Kubernetes operator passes these via entrypoint script):
#   docker run -p 7474:7474 -p 7000:7000 -p 7001:7001 \
#     goraphdb:latest \
#     -db /data/db -addr :7474 -shards 4 \
#     -node-id node1 -raft-addr 0.0.0.0:7000 -grpc-addr 0.0.0.0:7001 \
#     -bootstrap -peers "node2@..."
# ============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Build the React management UI
#
# The UI is a Vite + React + Tailwind SPA that provides:
#   - Cypher query editor with CodeMirror
#   - Graph visualization with Cytoscape.js
#   - Node/edge CRUD, index management, cluster status
#
# Build output: /ui/dist/ (static HTML/JS/CSS)
# ---------------------------------------------------------------------------
FROM node:22-alpine AS ui-builder

WORKDIR /ui

# Cache npm dependencies separately from source code.
# This layer is only rebuilt when package.json or package-lock.json change.
COPY ui/package.json ui/package-lock.json* ./
RUN npm ci --prefer-offline 2>/dev/null || npm install

# Copy UI source and build.
COPY ui/ ./
RUN npm run build

# ---------------------------------------------------------------------------
# Stage 2: Build the Go binary
#
# Compiles cmd/graphdb-ui which includes:
#   - HTTP/JSON API server (server/server.go)
#   - Cypher lexer, parser, executor (cypher_*.go)
#   - bbolt storage engine with sharding (storage.go, shard.go)
#   - Hashicorp Raft leader election (replication/election.go)
#   - gRPC WAL replication (replication/server.go, client.go)
#   - Prometheus metrics (metrics.go)
#   - Write-ahead log (wal.go)
#
# CGO_ENABLED=0 produces a fully static binary (no libc dependency),
# which is required for the distroless/scratch runtime image.
# ---------------------------------------------------------------------------
FROM golang:1.24-alpine AS go-builder

WORKDIR /src

# Cache Go module downloads separately from source code.
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree and build.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /usr/local/bin/graphdb-ui \
    ./cmd/graphdb-ui

# ---------------------------------------------------------------------------
# Stage 3: Production runtime image
#
# Uses distroless for minimal attack surface:
#   - No shell, no package manager, no libc
#   - Only the binary, static UI files, and CA certificates
#   - ~5MB base image (vs ~130MB for alpine, ~1GB for ubuntu)
#
# For debugging, temporarily switch to gcr.io/distroless/base-debian12:debug
# which includes a busybox shell at /busybox/sh.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot

# Copy the Go binary from Stage 2.
COPY --from=go-builder /usr/local/bin/graphdb-ui /usr/local/bin/graphdb-ui

# Copy the built UI static files from Stage 1.
COPY --from=ui-builder /ui/dist /ui/dist

# Create the default data directory.
# In Kubernetes, this is overlaid by a PVC mount.
# The directory must exist for GoraphDB to start.
COPY --from=go-builder /tmp /tmp

# Expose the three GoraphDB ports:
#   7474 — HTTP API, Cypher queries, health checks (/api/health),
#          Prometheus metrics (/metrics), management UI
#   7000 — Raft transport (leader election, heartbeats)
#   7001 — gRPC WAL replication (leader → follower streaming)
EXPOSE 7474 7000 7001

# Run as non-root (UID 65532 = distroless "nonroot" user).
USER 65532:65532

# Default entrypoint: standalone mode with UI enabled.
# The Kubernetes operator overrides this via the entrypoint.sh ConfigMap
# which adds cluster flags (-node-id, -raft-addr, -grpc-addr, -peers).
ENTRYPOINT ["/usr/local/bin/graphdb-ui"]
CMD ["-db", "/data/db", "-addr", ":7474", "-ui", "/ui/dist"]
