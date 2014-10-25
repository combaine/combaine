#!/usr/bin/env bash

#go test github.com/noxiouz/Combaine/common/httpclient
#go test -v github.com/noxiouz/Combaine/combainer/

function interface {
    make -f file_for_make fixture && python setup.py nosetests
}


interface
