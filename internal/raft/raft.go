package raft

import "sync"

// role type
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// log entry
type LogEntry struct {
	Index   int
	Term    int
	Command string
}

// raft struct
type Raft struct {
	mu sync.Mutex

	ID            string
	Peers         []*Raft
	CurrentTerm   int
	Role          Role
	VotedFor      string
	Log           []LogEntry
	CommitIndex   int
	LastApplied   int
	HeartbeatCh   chan bool
}

// new raft constructor
func NewRaft(id string) *Raft {
	return &Raft{
		ID:            id,
		CurrentTerm:   0,
		Peers:         []*Raft{},
		Role:          Follower,
		VotedFor:      "",
		Log:           []LogEntry{},
		CommitIndex:   0,
		LastApplied:   0,
		HeartbeatCh:   make(chan bool),
	}
}

func DiscoverPeers(raftNodes []*Raft) {
	var wg sync.WaitGroup
	for _, r := range raftNodes {
		wg.Add(1)
		go func(r *Raft) {
			defer wg.Done()

			// build the slice once so mutex is locked and unlocked once
			var newPeers []*Raft
			for _, n := range raftNodes {
				if r.ID == n.ID{
					continue
				}
				// build the slice
				newPeers = append(newPeers, n)
			}

			// add to the node
			r.mu.Lock()
			r.Peers = newPeers
			r.mu.Unlock()
		}(r)
	}
	wg.Wait()
}
