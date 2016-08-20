PREFIX?=$(shell pwd)

PKGS := $(shell go list ./... | grep -v ^github.com/combaine/combaine/vendor/)

.PHONY: clean all fmt vet lint build test

build: ${PREFIX}/build/combainer ${PREFIX}/build/agave ${PREFIX}/build/aggregating ${PREFIX}/build/parsing ${PREFIX}/build/graphite ${PREFIX}/build/razladki ${PREFIX}/build/cbb ${PREFIX}/build/solomon

${PREFIX}/build/combainer: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/combainer/main.go

${PREFIX}/build/aggregating: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/aggregating/main.go

${PREFIX}/build/parsing: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/parsing/main.go

${PREFIX}/build/agave: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/agave/main.go

${PREFIX}/build/graphite: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/graphite/main.go

${PREFIX}/build/razladki: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/razladki/main.go

${PREFIX}/build/cbb: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/cbb/main.go

${PREFIX}/build/solomon: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/solomon/main.go

fixture:
	go run tests/fixtures/gen_fixtures.go

clean:
	@echo "+ $@"
	rm -rf ${PREFIX}/build/ || true

vet:
	@echo "+ $@"
	@go vet $(PKGS)

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l . 2>&1 | grep -v ^vendor/ | tee /dev/stderr)" || \
		(echo >&2 "+ please format Go code with 'gofmt -s'" && false)

lint:
	@echo "+ $@"
	@test -z "$$(golint ./... 2>&1 | grep -v ^vendor/ | tee /dev/stderr)"

test: vet fmt
	@echo "+ $@"
	@echo "" > coverage.txt
	@set -e; for pkg in $(PKGS); do go test -coverprofile=profile.out -covermode=atomic $$pkg; \
	if [ -f profile.out ]; then \
		cat profile.out >> coverage.txt; rm  profile.out; \
	fi done; \
