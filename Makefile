
STOREDIR = $(CURDIR)/storage
FLAGS    = -ldflags "-X genericsmr.storage=$(STOREDIR)"

all: compile

deps:
	GOPATH=`pwd` go get "github.com/google/uuid"
	GOPATH=`pwd` go get "github.com/emirpasic/gods/maps/treemap"
	GOPATH=`pwd` go get "github.com/orcaman/concurrent-map"
	GOPATH=`pwd` go get "github.com/sasha-s/go-deadlock/..."

compile: | $(STOREDIR)
compile: deps
	GOPATH=`pwd` go install $(FLAGS) master
	GOPATH=`pwd` go install $(FLAGS) server
	GOPATH=`pwd` go install $(FLAGS) client

$(STOREDIR):
	mkdir -p $(STOREDIR)

race: FLAGS += -race
race: compile

mrproper:
	rm -rf pkg logs $(STOREDIR) bin/master bin/server bin/client \
           src/github.com

test: compile
	./bin/test.sh
