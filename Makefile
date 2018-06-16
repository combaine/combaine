export PATH := ~/go/bin:/usr/local/go/bin:$(PATH)
PREFIX?=$(shell pwd)
DIR := ${PREFIX}/build

PKGS := $(shell PATH="$(PATH)" bash -c "vgo list ./...|fgrep -v combaine/tests")

.PHONY: clean all fmt vet lint build test proto

docker: vet fmt fast-test build
	docker build . -t combainer
	docker tag combainer:latest uo0ya/combainer:latest
	docker push uo0ya/combainer:latest


build: ${DIR}/combainer ${DIR}/agave ${DIR}/worker ${DIR}/graphite \
	   ${DIR}/razladki ${DIR}/cbb ${DIR}/solomon ${DIR}/juggler

${DIR}/combainer: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/combainer/main.go

${PREFIX}/build/worker: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/worker/main.go

${DIR}/agave: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/agave/main.go

${DIR}/graphite: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/graphite/main.go

${DIR}/razladki: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/razladki/main.go

${DIR}/cbb: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/cbb/main.go

${DIR}/solomon: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/solomon/main.go

${DIR}/juggler: $(wildcard **/*.go)
	@echo "+ $@"
	vgo build -o $@ ./cmd/juggler/main.go

proto: ${PREFIX}/rpc/rpc.pb.go

${PREFIX}/rpc/rpc.pb.go: $(wildcard **/*.proto)
	@echo "+ $@"
	protoc -I rpc/ rpc/rpc.proto --go_out=plugins=grpc:rpc

clean:
	@echo "+ $@"
	rm -rf ${DIR}/ || true

vet:
	@echo "+ $@"
	@vgo vet $(PKGS)

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l . 2>&1 | tee /dev/stderr)" || \
		(echo >&2 "+ please format Go code with 'gofmt -s'" && false)

lint:
	@echo "+ $@"
	@test -z "$$(golint ./... 2>&1 | tee /dev/stderr)"


fast-test:
	@echo "+ $@"
	@set -e; for pkg in $(PKGS); do vgo test $$pkg; done

test: vet fmt
	@echo "+ $@"
	@echo "" > coverage.txt
	@set -e; for pkg in $(PKGS); do vgo test -race -coverprofile=profile.out -covermode=atomic $$pkg; \
	if [ -f profile.out ]; then \
		cat profile.out >> coverage.txt; rm  profile.out; \
	fi done; \
