package main

import (
	"fmt"
	"time"

	"github.com/peterukor/kvraft/internal/raft"
)

func main() {
	fmt.Println("kvraft node starting...")
	r1 := raft.NewRaft("A")
	fmt.Println("Initial Term:", r1.CurrentTerm)
	fmt.Println("Node ID:", r1.ID)
	fmt.Println("Initial Role:", r1.Role)
	go r1.RunElectionTimer()
	fmt.Println("Setting up sleeper")
	timer := time.NewTimer(3 * time.Second)
	<- timer.C
	fmt.Println("Current Role:", r1.Role)
	fmt.Println("Current Term:", r1.CurrentTerm)
	fmt.Println("Voted For:", r1.VotedFor)
}
