package main

import (
	"fmt"
	"github.com/peterukor/kvraft/internal/raft"
)

func main() {
	testRequestVote(5)
}

func testRequestVote(n int) {
	if n > 26 {
		n = 26
	}
	if n%2 == 0 {
		fmt.Println("adding one more node for odd nodes")
		n++
	}

	raftNodes := []*raft.Raft{}
	for i := 0; i < n; i++ {
		nodeID := string(rune(65 + i))
		raftNodes = append(raftNodes, raft.NewRaft(nodeID))
		fmt.Println("created Node", nodeID)
	}
	raft.DiscoverPeers(raftNodes)

	// for _, i := range raftNodes {
	// 	fmt.Printf("Node %s peers: [", i.ID)
	// 	for _, j := range i.Peers {
	// 		fmt.Printf(" %s,", j.ID)
	// 	}
	// 	fmt.Println("]")
	// }

	// fmt.Println("Run election timer for first node")
	// candidate := raftNodes[0]
	// candidate.RunElectionTimer()
	// // build rquestVoteRepls
	// for _, i := range raftNodes {
	// 	printInitialNodeInfo(i)
	// }

	// candidate.RequestVote()
	// for _, i := range raftNodes {
	// 	printAfterNodeInfo(i)
	// }

	//if node doesn't get majority
	// Make D and E reject A's vote request
	raftNodes[2].CurrentTerm = 1
	raftNodes[2].VotedFor = "X"

	raftNodes[3].CurrentTerm = 1
	raftNodes[3].VotedFor = "X"

	raftNodes[4].CurrentTerm = 1
	raftNodes[4].VotedFor = "X"

	// A starts the election
	candidate := raftNodes[0]
	candidate.RunElectionTimer()
	candidate.RequestVote()

	fmt.Println("----- Election Result -----")
	fmt.Println("Node:", candidate.ID)
	fmt.Println("Role:", candidate.Role)
	fmt.Println("Term:", candidate.CurrentTerm)
	fmt.Println("Voted For:", candidate.VotedFor)
}

func testHandleRequest() {
	fmt.Println("kvraft node starting...")
	r1 := raft.NewRaft("A")
	fmt.Println("created node A")
	r2 := raft.NewRaft("B")
	fmt.Println("created node B")

	// testing
	fmt.Println("testing RunElectionTimer and HandleRequestVote")
	fmt.Println("initial node A:")
	printInitialNodeInfo(r1)
	fmt.Println("initial node B:")
	printInitialNodeInfo(r2)
	r1.RunElectionTimer()
	reply := r2.HandleRequestVote(r1.BuildRequestVoteArgs())
	fmt.Println("r2 current term:", reply.CurrentTerm, "r2 voted for:", reply.VoteGranted)
	fmt.Println("after node A:")
	printAfterNodeInfo(r1)
	fmt.Println("after node B:")
	printAfterNodeInfo(r2)
	r1.RunElectionTimer()
}

func printInitialNodeInfo(r *raft.Raft) {
	fmt.Println("r1 Node ID:", r.ID)
	fmt.Println("r1 Initial Term:", r.CurrentTerm)
	fmt.Println("r1 Initial Role:", r.Role)
}

func printAfterNodeInfo(r *raft.Raft) {
	fmt.Println("After Role:", r.Role)
	fmt.Println("After Term:", r.CurrentTerm)
	fmt.Println("After Voted For:", r.VotedFor)
}
