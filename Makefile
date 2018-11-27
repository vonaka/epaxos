all: compile

deps:
	 GOPATH=`pwd` go get github.com/google/uuid
	 GOPATH=`pwd` go get github.com/emirpasic/gods/maps/treemap

compile: deps
	GOPATH=`pwd` go install $(FLAGS) master
	GOPATH=`pwd` go install $(FLAGS) server
	GOPATH=`pwd` go install $(FLAGS) client

race: FLAGS += -race
race: compile

mrproper:
	rm -rf pkg logs stable-store* bin/master bin/server bin/client \
           src/github.com

test: compile
	./bin/test.sh
