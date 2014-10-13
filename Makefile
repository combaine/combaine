
# pwd
CURDIR:=$(shell pwd)

# packages for go
PACKAGE_PATH=$(CURDIR)/src/github.com/noxiouz

# build dir
BUILD_DIR=$(CURDIR)/build

export GOPATH=$(CURDIR)


all: combainer_ agave_ cfgmanager_ parsing_ graphite_

deps:
	go get launchpad.net/gozk/zookeeper
	go get launchpad.net/goyaml
	go get github.com/cocaine/cocaine-framework-go/cocaine
	go get github.com/howeyc/fsnotify
	go get github.com/Sirupsen/logrus
	go get github.com/mitchellh/mapstructure
	mkdir -vp $(PACKAGE_PATH)
	if [ ! -d $(CURDIR)/src/github.com/noxiouz/Combaine ];then\
		ln -vs $(CURDIR) $(CURDIR)/src/github.com/noxiouz/Combaine; fi;

combainer_:
	go build -o $(BUILD_DIR)/main_combainer $(CURDIR)/combainer_main.go

parsing_:
	go build -o $(BUILD_DIR)/main_parsing-core $(CURDIR)/parsing_main.go

cfgmanager_:
	go build -o $(BUILD_DIR)/main_cfgmanager $(CURDIR)/cfgmanager_main.go

agave_:
	go build -o $(BUILD_DIR)/main_agave $(CURDIR)/agave_main.go

graphite_:
	go build -o $(BUILD_DIR)/main_graphite $(CURDIR)/graphite_main.go

clean::
	rm -rf $(BUILD_DIR) || true
