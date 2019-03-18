export PATH := ~/go/bin:/usr/local/go/bin:$(PATH)
export GO111MODULE := on
export CGO_ENABLED := 0

PREFIX?=$(shell pwd)
DIR := ${PREFIX}/build

.PHONY: clean all fmt vet lint build test fast-test proto docker docker-image

docker: clean build docker-image

docker-image:
	docker build . -t combainer

build: ${DIR}/combainer ${DIR}/agave ${DIR}/worker ${DIR}/graphite \
	   ${DIR}/razladki ${DIR}/cbb ${DIR}/solomon ${DIR}/juggler \
	   ${DIR}/monder

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

${DIR}/monder: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/monder/main.go

proto: ${PREFIX}/rpc/rpc.pb.go

${PREFIX}/rpc/rpc.pb.go: $(wildcard **/*.proto)
	@echo "+ $@"
	protoc -I rpc/ rpc/rpc.proto --go_out=plugins=grpc:rpc

clean:
	@echo "+ $@"
	rm -rf ${DIR}/ || true

vet:
	@echo "+ $@"
	@go vet ./...

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l . 2>&1 | tee /dev/stderr)" || \
		(echo >&2 "+ please format Go code with 'gofmt -s'" && false)

lint:
	@echo "+ $@"
	@test -z "$$(golint ./... 2>&1 | tee /dev/stderr)"


fast-test:
	@echo "+ $@"
	@go test ./...

test: vet fmt
	@echo "+ $@"
	@echo "" > coverage.txt
	CGO_ENABLED=1 go test ./... -race -coverprofile=coverage.txt -covermode=atomic
