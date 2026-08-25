package raft

import (
	"sync"
)

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

type Peer struct {
	ID           string
	Address      string
	Next         int
	Match        int
	KnownEntry   int
	UnknownEntry int
}

// raft struct
type Raft struct {
	mu sync.Mutex

	ID          string
	Address     string
	Peers       map[string]*Peer
	CurrentTerm int
	Role        Role
	VotedFor    string
	Log         []LogEntry
	CommitIndex int
	LastApplied int
	HeartbeatCh chan bool
}

// new raft constructor
func NewRaft(id string, address string) *Raft {
	return &Raft{
		ID:          id,
		Address:     address,
		CurrentTerm: 0,
		Peers:       map[string]*Peer{},
		Role:        Follower,
		VotedFor:    "",
		Log:         []LogEntry{{0, 0, ""}},
		CommitIndex: 0,
		LastApplied: 0,
		HeartbeatCh: make(chan bool),
	}
}

// accepts an ID: address map/dict of all nodes and their addresses
func (r *Raft) ConfigurePeers(peers map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, value := range peers {
		if key == r.ID {
			continue
		}
		// create a peer struct and append ID and address
		r.Peers[key] = &Peer{
			ID:      key,
			Address: value,
			Next:    len(r.Log),
			Match:   0,
		}
	}
}

func (r *Raft) lastLogIndexAndTerm() (int, int) {
	var (
		lastLogIndex int
		lastLogTerm  int
	)
	lastLog := len(r.Log) - 1

	lastLogIndex = r.Log[lastLog].Index
	lastLogTerm = r.Log[lastLog].Term
	return lastLogIndex, lastLogTerm
}
