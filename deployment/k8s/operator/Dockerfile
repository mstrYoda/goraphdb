# ---------------------------------------------------------------------------
# GoraphDB Operator — Multi-stage Build
#
# Stage 1: Build the operator binary using the Go toolchain.
# Stage 2: Copy into a minimal distroless image for production.
#
# Design decision: We use distroless rather than alpine because the operator
# is a pure Go binary with no shell dependencies. Distroless has a smaller
# attack surface and is recommended by Google for production containers.
# ---------------------------------------------------------------------------

# Build stage
FROM golang:1.24 AS builder

WORKDIR /workspace

# Cache dependency downloads separately from source code.
# This layer is only rebuilt when go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /workspace/manager \
    main.go

# Runtime stage
FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY --from=builder /workspace/manager .

# Run as non-root user (UID 65532 is the standard distroless nonroot user).
USER 65532:65532

ENTRYPOINT ["/manager"]
