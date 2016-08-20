# pwd
CURDIR:=$(shell pwd)

CMD_DIR=$(CURDIR)/cmd

# packages for go
PACKAGE_PATH=$(CURDIR)/src/github.com/Combaine

# build dir
BUILD_DIR=$(CURDIR)/build

#export GOPATH=$(CURDIR)

OS := $(shell uname)
ifeq ($(OS), Darwin)
	export CGO_CFLAGS=-I/usr/local/include/zookeeper
endif

PKGS := $(shell go list ./... | grep -v ^github.com/Combaine/Combaine/vendor/)

.PHONY: clean combainer

build: combainer agave aggregating parsing graphite razladki cbb solomon

combainer:
	go build -o $(BUILD_DIR)/combainer $(CMD_DIR)/combainer/main.go

aggregating:
	go build -o $(BUILD_DIR)/aggregate-core $(CMD_DIR)/aggregating/main.go

parsing:
	go build -o $(BUILD_DIR)/parsing-core $(CMD_DIR)/parsing/main.go

agave:
	go build -o $(BUILD_DIR)/agave $(CMD_DIR)/agave/main.go

graphite:
	go build -o $(BUILD_DIR)/graphite $(CMD_DIR)/graphite/main.go

razladki:
	go build -o $(BUILD_DIR)/razladki $(CMD_DIR)/razladki/main.go

cbb:
	go build -o $(BUILD_DIR)/cbb $(CMD_DIR)/cbb/main.go

solomon:
	go build -o $(BUILD_DIR)/solomon $(CMD_DIR)/solomon/main.go

fixture:
	go run tests/fixtures/gen_fixtures.go

clean::
	rm -rf $(BUILD_DIR) || true

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
