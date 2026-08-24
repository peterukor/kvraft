package raft

type AppendEntriesReply struct {
	FollowerCurrentTerm int
	Success             bool
	ConflictTerm        int
	ConflictIndex       int
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
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             false,
		}

	}

	// handle a shorter log entry size
	// against an index out of error
	if ae.PrevLogIndex > lastLogIndex {

		return &AppendEntriesReply{
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             false,
			ConflictIndex:       lastLogIndex,
			ConflictTerm:        r.Log[lastLogIndex].Term,
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
		return &AppendEntriesReply{
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             true,
		}

	} else {
		// if the index doesn't match
		// tell the leader "as at this entry, this is was my leadership cycle"
		return &AppendEntriesReply{
			FollowerCurrentTerm: r.CurrentTerm,
			Success:             false,
			ConflictIndex:       ae.PrevLogIndex,
			ConflictTerm:        r.Log[ae.PrevLogIndex].Term,
		}
	}
}
