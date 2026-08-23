package raft

import "time"

// struct sent to other nodes to request vote
type RequestVoteArgs struct {
	CandidateID           string
	CandidateTerm         int
	CandidateLastLogIndex int
	CandidateLastLogTerm  int
}

// reply sent for a vote requested
type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

// increment currentTerm, become a candidate, and vote for itself
func (r *Raft) BecomeCandidate() {
	r.CurrentTerm++
	r.Role = Candidate
	r.VotedFor = r.ID
}

// returns a nodes last log index and term
func (r *Raft) lastLogIndexAndTerm() (int, int) {
	var (
		lastLogIndex int
		lastLogTerm  int
	)
	lastLog := len(r.Log) - 1
	if lastLog < 0 {
		lastLogIndex = 0
		lastLogTerm = 0
	} else {
		lastLogIndex = r.Log[lastLog].Index
		lastLogTerm = r.Log[lastLog].Term
	}
	return lastLogIndex, lastLogTerm
}

// initialize an election countdown timer that resets once the node recieves a heartbeat
func (r *Raft) RunElectionTimer() {
	timer := time.NewTimer(2 * time.Second)
	for {
		select {
		case <-timer.C:
			r.BecomeCandidate()
			return
		case <-r.HeartbeatCh:

			// handle if the timer has fired or is about to fire then reset the timer
			// returns false if the timer has ended and flushes the channel
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(2 * time.Second)
		}

	}
}

// build the requestVoteArgs and returns the address of the struct
func (r *Raft) BuildRequestVoteArgs() *RequestVoteArgs {

	// get last log index and term
	lastIndex, lastTerm := r.lastLogIndexAndTerm()

	return &RequestVoteArgs{
		CandidateID:           r.ID,
		CandidateTerm:         r.CurrentTerm,
		CandidateLastLogIndex: lastIndex,
		CandidateLastLogTerm:  lastTerm,
	}
}

// handle requestVote request
func (r *Raft) HandleRequestVote(rv *RequestVoteArgs) *RequestVoteReply {
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
			vote = true
			r.VotedFor = rv.CandidateID
		}
	}
	return &RequestVoteReply{
		CurrentTerm: r.CurrentTerm,
		VoteGranted: vote,
	}
}
