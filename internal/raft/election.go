package raft

import (
	"time"
)

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

// increment currentTerm, become a candidate, and vote for itself
func (r *Raft) BecomeCandidate() {
	r.CurrentTerm++
	r.Role = Candidate
	r.VotedFor = r.ID
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
	return &RequestVoteReply{
		CurrentTerm: r.CurrentTerm,
		VoteGranted: vote,
	}
}

// process vote reply
func (r *Raft) ProcessVotes(rvr RequestVoteReply) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// ignore if vote is from a previous term
	if rvr.CurrentTerm < r.CurrentTerm {
		return false
	}

	// ignore vote if node is a follower
	if r.Role != Candidate {
		return false
	}

	// update the node's term if it is behind
	if rvr.CurrentTerm > r.CurrentTerm {
		r.CurrentTerm = rvr.CurrentTerm
		r.Role = Follower
		r.VotedFor = ""

	} else if rvr.VoteGranted == true {
		return true
	}
	return false
}

func (r *Raft) RequestVote() {

	// lock incase to prevent race condition from new nodes added
	r.mu.Lock()
	peers := r.Peers
	r.mu.Unlock()

	nodeCount := len(peers)
	if nodeCount == 0 {
		r.Role = Leader
		return
	}

	// channels to listen for election replies
	votesCh := make(chan RequestVoteReply, nodeCount)

	// build the requestVoteArgs
	nodeArgs := r.BuildRequestVoteArgs()

	for _, raftNode := range peers {
		if raftNode.ID == r.ID {
			continue
		}

		go func(raftNode *Raft) {

			// a lock inside HandleRequestVote handles
			// multiple RequestVote calls
			voteReply := raftNode.HandleRequestVote(nodeArgs)
			if voteReply != nil {

				// send reply over channel
				votesCh <- *voteReply
			}
		}(raftNode)
	}

	// increments loop as replies arrive in channels
	majority := nodeCount/2 + 1
	accepted := 1
	rejected := 0

	for range nodeCount {
		voteReply := <-votesCh
		voteResponse := r.ProcessVotes(voteReply)

		// once majority is reached stop waiting
		if voteResponse == true {
			accepted++
			if accepted == majority {
				r.Role = Leader
				return
			}

		} else {
			rejected++
			if rejected == majority {
				r.Role = Follower
				return
			}
		}

	}
}
