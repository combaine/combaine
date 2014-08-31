
# pwd
CURDIR:=$(shell pwd)

# packages for go
PACKAGE_PATH=$(CURDIR)/src/github.com/noxiouz

# build dir
BUILD_DIR=$(CURDIR)/build

export GOPATH=$(CURDIR)


all: combaine agave timetail cfgmanager parsing graphite

prepare:
	mkdir -p $(PACKAGE_PATH) || true
	mkdir -p $(BUILD_DIR) || true

	ln -s $(CURDIR) $(PACKAGE_PATH)/Combaine

	go get launchpad.net/goyaml
	go get github.com/cocaine/cocaine-framework-go/cocaine
	go get github.com/howeyc/fsnotify
	go get github.com/Sirupsen/logrus

combaine: prepare
	go get launchpad.net/gozk/zookeeper
	go build -o $(BUILD_DIR)/main_combainer $(CURDIR)/combainer_main.go

parsing: prepare
	go build -o $(BUILD_DIR)/main_parsing-core $(CURDIR)/parsing_main.go

cfgmanager: prepare
	go build -o $(BUILD_DIR)/main_cfgmanager $(CURDIR)/cfgmanager_main.go

# timetail: prepare
# 	go build -o $(BUILD_DIR)/main_timetail $(CURDIR)/timetail_main.go

agave: prepare
	go build -o $(BUILD_DIR)/main_agave $(CURDIR)/agave_main.go

graphite: prepare
	go build -o $(BUILD_DIR)/main_graphite $(CURDIR)/graphite_main.go

clean::
	rm -rf $(PACKAGE_PATH)/Combaine
	rm -rf $(BUILD_DIR)
