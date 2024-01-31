GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
BINARY_NAME=isis_txn_node


all: build

build:
	$(GOBUILD) -o $(BINARY_NAME) src/node.go src/ISISAlgo.go src/pq.go

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)