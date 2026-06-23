.PHONY: build test race vet lint fmt run tidy deploy all

DISCOVERY_BIN := bin/discovery-server
DISCOVERY_PKG := ./discovery/cmd/discovery-server
CHUNK_BIN := bin/trove-chunk
CHUNK_PKG := ./client/cmd/trove-chunk

all: fmt vet test build

build:
	go build -trimpath -o $(DISCOVERY_BIN) $(DISCOVERY_PKG)
	go build -trimpath -o $(CHUNK_BIN) $(CHUNK_PKG)

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

deploy:
	bash discovery/deploy/deploy.sh
