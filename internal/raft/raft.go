package raft


type LogEntry struct {
	Index int
	Term int
	Command string
}

type Raft struct {
	CurrentTerm int
	VotedFor string
	Log []LogEntry
	CommitIndex int
	LastApplied int
}

func NewRaft() *Raft {
	return &Raft {
		CurrentTerm: 0,
		VotedFor: "",
		Log: []LogEntry{},
		CommitIndex: 0,
		LastApplied: 0,
	}
}
