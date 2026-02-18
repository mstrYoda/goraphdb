# ---------------------------------------------------------------------------
# GoraphDB Operator — Build & Deploy Targets
#
# Usage:
#   make build          Build the operator binary
#   make docker-build   Build the Docker image
#   make docker-push    Push to container registry
#   make install        Install CRDs into the cluster
#   make deploy         Deploy operator + CRDs to the cluster
#   make undeploy       Remove operator + CRDs from the cluster
#   make run            Run the operator locally (outside cluster)
#   make fmt            Format Go source files
#   make vet            Run Go vet
#   make test           Run unit tests
# ---------------------------------------------------------------------------

# Container image coordinates
IMG ?= ghcr.io/mstryoda/goraphdb-operator:latest
GORAPHDB_IMG ?= ghcr.io/mstryoda/goraphdb:latest

# Kubernetes namespace for the operator
NAMESPACE ?= goraphdb

.PHONY: all
all: fmt vet build

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

.PHONY: run
run: fmt vet ## Run the operator locally (outside cluster, uses ~/.kube/config).
	go run ./main.go

##@ Build

.PHONY: build
build: fmt vet ## Build the operator binary.
	go build -o bin/manager main.go

.PHONY: docker-build
docker-build: ## Build the operator Docker image.
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push: ## Push the operator Docker image.
	docker push $(IMG)

##@ Deployment

.PHONY: install
install: ## Install CRDs into the active cluster.
	kubectl apply -f config/crd/

.PHONY: uninstall
uninstall: ## Uninstall CRDs from the active cluster.
	kubectl delete -f config/crd/

.PHONY: deploy
deploy: install ## Deploy the operator to the active cluster.
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f config/rbac/ -n $(NAMESPACE)
	kubectl apply -f config/manager/ -n $(NAMESPACE)

.PHONY: undeploy
undeploy: ## Remove the operator from the active cluster.
	kubectl delete -f config/manager/ -n $(NAMESPACE) --ignore-not-found
	kubectl delete -f config/rbac/ -n $(NAMESPACE) --ignore-not-found
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

.PHONY: deploy-samples
deploy-samples: ## Deploy sample GoraphDBCluster resources.
	kubectl apply -f config/samples/

.PHONY: deploy-monitoring
deploy-monitoring: ## Deploy Prometheus ServiceMonitor and Grafana dashboard.
	kubectl apply -f config/monitoring/
