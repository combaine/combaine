export PATH := ~/go/bin:/usr/local/go/bin:$(PATH)
export GO111MODULE := on
export CGO_ENABLED := 0

PREFIX?=$(shell pwd)
DIR := ${PREFIX}/build

.PHONY: clean all fmt vet lint build test fast-test proto docker docker-image

docker: clean build docker-image

docker-image:
	docker build . -t combainer

build: proto ${DIR}/combainer ${DIR}/worker ${DIR}/graphite \
	   ${DIR}/solomon ${DIR}/juggler \

${DIR}/combainer: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/combainer/main.go

${PREFIX}/build/worker: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/worker/main.go

${DIR}/graphite: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/graphite/main.go

${DIR}/solomon: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/solomon/main.go

${DIR}/juggler: $(wildcard **/*.go)
	@echo "+ $@"
	go build -o $@ ./cmd/juggler/main.go

proto: rpc/aggregator.proto rpc/timeframe.proto rpc/worker.proto rpc/senders.proto
	@echo "+ $@"
	protoc -I rpc/ rpc/aggregator.proto --go_out=plugins=grpc:worker
	protoc -I rpc/ rpc/worker.proto --go_out=plugins=grpc:worker
	protoc -I rpc/ rpc/timeframe.proto --go_out=plugins=grpc:worker
	protoc -I rpc/ rpc/senders.proto --go_out=plugins=grpc:senders
	python3 -m grpc_tools.protoc -I rpc --python_out=aggregator --grpc_python_out=aggregator rpc/timeframe.proto rpc/aggregator.proto


clean:
	@echo "+ $@"
	rm -rf ${DIR}/ || true
	find . -name '__pycache__' -exec rm -vrf {} +
	rm -vf aggregator/*_pb2_grpc.py aggregator/*_pb2.py || true
	rm -vf worker/*.pb.go || true
	rm -vf senders/*.pb.go || true

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

test: vet fmt proto
	@echo "+ $@"
	@echo "" > coverage.txt
	CGO_ENABLED=1 go test ./... -race -coverprofile=coverage.txt -covermode=atomic
