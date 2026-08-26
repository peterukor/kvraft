package main

import (
	"fmt"
	"log"
	"os"

	"github.com/peterukor/kvraft/internal/raft"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: go run ./cmd/kvraft-server <node-id>\n<node-id>: {A, B, C, D, E}")
	}

	id := os.Args[1]

	peers := map[string]string{
		"A": "localhost:8001",
		"B": "localhost:8002",
		"C": "localhost:8003",
		"D": "localhost:8004",
		"E": "localhost:8005",
	}

	address, ok := peers[id]
	if !ok {
		log.Fatalf("unknown node ID: %s", id)
	}

	node := raft.NewRaft(id, address)

	// Configure all nodes in the cluster.
	node.ConfigurePeers(peers)

	// Start HTTP server.
	go func() {
		fmt.Printf("Node %s listening on %s\n", id, address)

		if err := node.StartHttpServer(); err != nil {
			log.Fatal(err)
		}
	}()

	// Start election timer.
	go node.RunElectionTimer()

	fmt.Printf("Node %s started\n", id)

	// Keep process alive.
	select {}
}
