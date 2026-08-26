package raft

type AppendEntriesReply struct {
	FollowerID          string
	FollowerCurrentTerm int
	Success             bool
	Stale               bool
	Mismatch            bool
	ConflictIndex       int
}

func (r *Raft) findPrevTermFirstIndex(prevLogIndex int) int {

	// while prev > 0
	for prevLogIndex > 1 {
		if r.Log[prevLogIndex].Term != r.Log[prevLogIndex-1].Term {
			break
		}
		prevLogIndex--
	}
	return prevLogIndex
}

func (r *Raft) HandleAppendEntries(ae *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	lastLogIndex := len(r.Log) - 1

	// update follower node if stale
	if ae.LeaderCurrentTerm > r.CurrentTerm {
		r.Role = Follower
		r.CurrentTerm = ae.LeaderCurrentTerm

		// reject if follower current term is ahead
	} else if ae.LeaderCurrentTerm < r.CurrentTerm {

		return &AppendEntriesReply{
			FollowerID:          r.ID,
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             false,
			Stale:               true,
		}

	}

	// leader is not stale
	// send heartbeat to reset
	select {
	case r.HeartbeatCh <- true:
	default:
	}

	// handle a shorter log entry size
	// against an index out of error
	if ae.PrevLogIndex > lastLogIndex {
		conflictIndex := r.findPrevTermFirstIndex(lastLogIndex)

		return &AppendEntriesReply{
			FollowerID:          r.ID,
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             false,
			Mismatch:            true,
			ConflictIndex:       conflictIndex,
		}
	}

	// append an older entry match
	if r.Log[ae.PrevLogIndex].Term == ae.PrevLogTerm {
		followerIndex := ae.PrevLogIndex + 1
		entryIndex := 0

		// skip over exsisting entries
		for {
			// handle index bigger than log entry size
			// against an index out of range error
			if followerIndex >= len(r.Log) ||
				entryIndex >= len(ae.Entries) {
				break
			}

			// this never runs if index == len of any
			if r.Log[followerIndex] != ae.Entries[entryIndex] {
				break
			}
			followerIndex++
			entryIndex++
		}

		// slice[:len(slice)] doesn't cause an error
		r.Log = append(r.Log[:followerIndex], ae.Entries[entryIndex:]...)
		r.CommitIndex = ae.CommitIndex
		return &AppendEntriesReply{
			FollowerID:          r.ID,
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             true,
		}

	} else {

		conflictIndex := r.findPrevTermFirstIndex(ae.PrevLogIndex)
		// if the index doesn't match
		// tell the leader "as at this entry, this is was my leadership cycle"
		return &AppendEntriesReply{
			FollowerID:          r.ID,
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             false,
			Mismatch:            true,
			ConflictIndex:       conflictIndex,
		}
	}
}
