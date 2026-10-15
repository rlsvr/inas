MODULE        := github.com/rlsvr/inas
GOLANGCI_LINT := golangci-lint

.PHONY: all build test test-verbose bench fmt lint lint-fix check tidy clean

## all: build + test + lint
all: build test lint

## build: compile all packages
build:
	go build ./...

## test: run all tests with race detector
test:
	go test -race ./...

## test-verbose: run all tests in verbose mode
test-verbose:
	go test -race -v ./...

## bench: run all benchmarks
bench:
	go test -bench=. -benchmem ./...

## fmt: format go source files
fmt:
	go fmt ./...

## lint: report lint issues (no fix)
lint:
	$(GOLANGCI_LINT) run ./...

## lint-fix: apply auto-fixable lint issues
lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

## tidy: tidy go.mod / go.sum
tidy:
	go mod tidy

## check: fmt + lint + tidy
check: fmt lint tidy

## clean: remove build artifacts
clean:
	rm -rf bin/
