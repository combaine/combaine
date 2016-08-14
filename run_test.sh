#!/usr/bin/env bash

#go test github.com/Combaine/Combaine/common/httpclient
#go test -v github.com/Combaine/Combaine/combainer/

function interface {
    echo "Running interface tests between go and python..."
    make -f file_for_make fixture && python setup.py nosetests
}

function gopackages {
    echo "Running go tests..."
    go test -cover github.com/Combaine/Combaine/combainer/...
    go test -cover github.com/Combaine/Combaine/common/...
    go test -cover github.com/Combaine/Combaine/senders/...
    go test -cover github.com/Combaine/Combaine/fetchers/...
    go test -cover github.com/Combaine/Combaine/parsing
    go test -cover github.com/Combaine/Combaine/aggregating
}

interface
gopackages
