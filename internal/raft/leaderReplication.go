package raft

type AppendEntriesArgs struct {
	ID                string
	LeaderCurrentTerm int
	Entries           []LogEntry
	PrevLogIndex      int
	PrevLogTerm       int
}

func (r *Raft) BuildAppendEntriesArgs(next int, match int) *AppendEntriesArgs {

	return &AppendEntriesArgs{
		ID:                r.ID,
		LeaderCurrentTerm: r.CurrentTerm,
		Entries:           r.Log[next:],
		PrevLogIndex:      r.Log[match].Index,
		PrevLogTerm:       r.Log[match].Term,
	}
}

