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

func (r *Raft) AppendEntries() {
	r.mu.Lock()
	clusterSize := len(r.Peers)
	// build peers array in the lock
	// make(slice type, length, capacity)
	peers := make([]*Peer, 0, clusterSize)

	for _, p := range r.Peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()

	// create a channel to receive responses
	AppendCh := make(chan *AppendEntriesReply, clusterSize)

	// loop through peers and send each node entries
	for _, peer := range peers {
		go func(peer *Peer) {
			args := r.BuildAppendEntriesArgs(peer.Next, peer.Match)
			Reply := r.sendAppendEntries(peer, args)
			if Reply != nil {
				AppendCh <- Reply
			} else {
				AppendCh <- &AppendEntriesReply{
					FollowerID: peer.ID,
					Success:    false,
				}
			}
		}(peer)
	}

	for range clusterSize {
		Reply := <-AppendCh
		r.HandleAppendEntriesResponse(Reply)
	}
}

func (r *Raft) HandleAppendEntriesResponse(aer *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if aer.Stale {
		r.CurrentTerm = aer.FollowerCurrentTerm
		r.VotedFor = ""
		r.Role = Follower
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
