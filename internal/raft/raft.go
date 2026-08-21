package raft

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
	ID          string
	CurrentTerm int
	Role        Role
	VotedFor    string
	Log         []LogEntry
	CommitIndex int
	LastApplied int
	HeartbeatCh chan bool
}

// new raft constructor
func NewRaft(id string) *Raft {
	return &Raft{
		ID:          id,
		CurrentTerm: 0,
		Role:        Follower,
		VotedFor:    "",
		Log:         []LogEntry{},
		CommitIndex: 0,
		LastApplied: 0,
		HeartbeatCh: make(chan bool),
	}
}
