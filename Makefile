export PATH := /usr/local/go/bin:$(PATH)
PREFIX?=$(shell pwd)
DIR := ${PREFIX}/build

PKGS := $(shell bash -c "go list ./...|egrep -v '^github.com/combaine/combaine/(tests|vendor/)'")

.PHONY: clean all fmt vet lint build test proto

build: ${DIR}/combainer ${DIR}/agave ${DIR}/worker ${DIR}/graphite \
	   ${DIR}/razladki ${DIR}/cbb ${DIR}/solomon ${DIR}/juggler

${DIR}/combainer: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/combainer/main.go

${PREFIX}/build/worker: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/worker/main.go

${DIR}/agave: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/agave/main.go

${DIR}/graphite: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/graphite/main.go

${DIR}/razladki: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/razladki/main.go

${DIR}/cbb: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/cbb/main.go

${DIR}/solomon: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/solomon/main.go

${DIR}/juggler: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/juggler/main.go

proto: ${PREFIX}/rpc/rpc.pb.go

${PREFIX}/rpc/rpc.pb.go: $(wildcard **/*.proto)
	@echo "+ $@"
	protoc -I rpc/ rpc/rpc.proto --go_out=plugins=grpc:rpc

clean:
	@echo "+ $@"
	rm -rf ${DIR}/ || true

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
