package raft

import "time"

type RequestVoteArgs struct {
	CandidateID   string
	CandidateTerm int
	LastLogIndex  int
	LastLogTerm   int
}

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

func (r *Raft) BuildRequestVoteArgs() *RequestVoteArgs {
	var lastIndex int
	var lastTerm int

	// check if log is empty and initialize to 0 if it is
	lastLog := len(r.Log) - 1
	if lastLog < 0 {
		lastIndex = 0
		lastTerm = 0
	} else {
		lastIndex = r.Log[lastLog].Index
		lastTerm = r.Log[lastLog].Term
	}
	return &RequestVoteArgs{
		CandidateID:   r.ID,
		CandidateTerm: r.CurrentTerm,
		LastLogIndex:  lastIndex,
		LastLogTerm:   lastTerm,
	}
}
