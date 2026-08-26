package raft

// reply sent for a vote requested
type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

// handle requestVote request
func (r *Raft) HandleRequestVote(rv *RequestVoteArgs) *RequestVoteReply {
	// lock incase multiple nodes request votes
	r.mu.Lock()
	defer r.mu.Unlock()
	vote := false

	// get last log index and term
	lastIndex, lastTerm := r.lastLogIndexAndTerm()

	// update current term if candidate term is ahead
	if rv.CandidateTerm > r.CurrentTerm {
		r.CurrentTerm = rv.CandidateTerm
		r.Role = Follower
		r.VotedFor = ""

		// reject vote if candidate term is behind current term
	} else if rv.CandidateTerm < r.CurrentTerm {
		return &RequestVoteReply{
			CurrentTerm: r.CurrentTerm,
			VoteGranted: vote,
		}

		// reject vote if node has voted for a different node in current term
	} else if (r.VotedFor != rv.CandidateID) && (r.VotedFor != "") {
		return &RequestVoteReply{
			CurrentTerm: r.CurrentTerm,
			VoteGranted: vote,
		}
	}

	// compare last log term and last log index
	if rv.CandidateLastLogTerm > lastTerm {
		r.VotedFor = rv.CandidateID
		vote = true

	} else if rv.CandidateLastLogTerm == lastTerm {
		// compare last log index if last log term matches
		if rv.CandidateLastLogIndex >= lastIndex {
			r.VotedFor = rv.CandidateID
			vote = true
		}
	}

	// leader found
	// send heartbeat to reset timer
	if vote {
		select {
		case r.HeartbeatCh <- true:
		default:
		}
	}
	return &RequestVoteReply{
		CurrentTerm: r.CurrentTerm,
		VoteGranted: vote,
	}
}
