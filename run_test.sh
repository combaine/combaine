#!/usr/bin/env bash

#go test github.com/noxiouz/Combaine/common/httpclient
#go test -v github.com/noxiouz/Combaine/combainer/

function interface {
	echo "Running interface tests between go and python..."
    make -f file_for_make fixture && python setup.py nosetests
}

function gopackages {
	echo "Running go tests..."
	go test -cover github.com/noxiouz/Combaine/combainer/...
	go test -cover github.com/noxiouz/Combaine/common/...
	go test -cover github.com/noxiouz/Combaine/senders/...
	go test -cover github.com/noxiouz/Combaine/fetchers/...
	go test -cover github.com/noxiouz/Combaine/parsing
}

interface
gopackages
