package raft

type AppendEntriesArgs struct {
	ID                string
	LeaderCurrentTerm int
	Entries           []LogEntry
	CommitIndex       int
	PrevLogIndex      int
	PrevLogTerm       int
}

func (r *Raft) BuildAppendEntriesArgs(next int, match int) *AppendEntriesArgs {
	r.mu.Lock()
	defer r.mu.Unlock()
	// send an empty entry if no new entry to send
	if next >= len(r.Log) {
		return &AppendEntriesArgs{
			ID:                r.ID,
			LeaderCurrentTerm: r.CurrentTerm,
			CommitIndex:       r.CommitIndex,
			PrevLogIndex:      r.Log[match].Index,
			PrevLogTerm:       r.Log[match].Term,
		}
	}

	// build the entry before sending
	Entries := make([]LogEntry, len(r.Log[next:]))
	copy(Entries, r.Log[next:])
	return &AppendEntriesArgs{
		ID:                r.ID,
		LeaderCurrentTerm: r.CurrentTerm,
		Entries:           Entries,
		CommitIndex:       r.CommitIndex,
		PrevLogIndex:      r.Log[match].Index,
		PrevLogTerm:       r.Log[match].Term,
	}
}

func (r *Raft) AppendEntries(peer *Peer) {
	r.mu.Lock()
	next, match := peer.Next, peer.Match
	r.mu.Unlock()
	args := r.BuildAppendEntriesArgs(next, match)
	Reply := r.sendAppendEntries(peer, args)
	if Reply == nil {
		Reply = &AppendEntriesReply{
			FollowerID: peer.ID,
			Success:    false,
		}
	}
	// each go routine handles it's own response
	r.HandleAppendEntriesResponse(Reply)
}

func (r *Raft) HandleAppendEntriesResponse(aer *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if aer.Stale {
		r.CurrentTerm = aer.FollowerCurrentTerm
		r.VotedFor = ""
		r.Role = Follower
		// restart timer
		select {
		case r.HeartbeatCh <- true:
		default:
		}
	}
	if aer.Mismatch {
		r.Peers[aer.FollowerID].Match = aer.ConflictIndex - 1
		r.Peers[aer.FollowerID].Next = aer.ConflictIndex
	}
	if aer.Success {
		r.Peers[aer.FollowerID].Match = len(r.Log) - 1
		r.Peers[aer.FollowerID].Next = len(r.Log)
	}
}
