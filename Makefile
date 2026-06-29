.PHONY: build test race vet lint fmt run tidy deploy proto e2e all

DISCOVERY_BIN := bin/discovery-server
DISCOVERY_PKG := ./discovery/cmd/discovery-server
CHUNK_BIN := bin/trove-chunk
CHUNK_PKG := ./client/cmd/trove-chunk
PEER_BIN := bin/trove-peer
PEER_PKG := ./client/cmd/trove-peer

PROTOC_GEN_GO_VERSION := v1.36.8
WIRE_DIR := client/internal/wire

all: fmt vet test build

build:
	go build -trimpath -o $(DISCOVERY_BIN) $(DISCOVERY_PKG)
	go build -trimpath -o $(CHUNK_BIN) $(CHUNK_PKG)
	go build -trimpath -o $(PEER_BIN) $(PEER_PKG)

fmt:
	gofmt -w .

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

run:
	go run $(DISCOVERY_PKG)

tidy:
	go mod tidy

# proto regenerates the committed wire *.pb.go. Requires protoc on PATH; installs
# the pinned protoc-gen-go into GOBIN. Contributors only need this when editing .proto.
proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc \
		--proto_path=$(WIRE_DIR) \
		--go_out=$(WIRE_DIR)/wirepb --go_opt=paths=source_relative \
		$(WIRE_DIR)/wire.proto

deploy:
	bash discovery/deploy/deploy.sh

# e2e runs the NAT hole-punch matrix: privileged containers with a Linux netns +
# nftables topology emulate the NAT types, with the discovery server as coordinator, plus
# multi-peer acceptance gates (offline catch-up, bidirectional, holder, and
# member/unencrypted recovery). Cells and gates run in parallel. Runs on every PR via
# .github/workflows/e2e.yml; also runnable locally. Requires Docker.
e2e:
	bash client/test/e2e/matrix.sh
