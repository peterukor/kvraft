package raft

import "maps"

// struct sent to other nodes to request vote
type RequestVoteArgs struct {
	CandidateID           string
	CandidateTerm         int
	CandidateLastLogIndex int
	CandidateLastLogTerm  int
}

// increment currentTerm, become a candidate, and vote for itself
func (r *Raft) BecomeCandidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
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

func (r *Raft) RequestVote() {

	// lock incase to prevent race condition from new nodes added
	r.mu.Lock()
	clusterSize := len(r.Peers)
	if clusterSize == 0 {
		r.Role = Leader
		r.mu.Unlock()
		return
	}
	// build the requestVoteArgs
	nodeArgs := r.BuildRequestVoteArgs()
	// build peers array in the lock
	peersMap := make(map[string]*Peer, clusterSize)
	maps.Copy(peersMap, r.Peers)
	r.mu.Unlock()

	// channels to listen for election replies
	votesCh := make(chan *RequestVoteReply, clusterSize)

	for _, peer := range peersMap {
		// send requestVote over http
		go func(peer *Peer) { 
			voteReply := r.sendRequestVote(peer, nodeArgs)
			if voteReply != nil {
				// send reply over channel
				votesCh <- voteReply
			} else {
				votesCh <- &RequestVoteReply{
					CurrentTerm: -1,
					VoteGranted: false,
				}
			}
		}(peer)
	}

	// calculation for majority
	majority := clusterSize/2 + 1
	accepted := 1
	rejected := 0

	// increments loop as replies arrive in channels
	for range clusterSize {
		// waits until a reply is received on the channel
		voteReply := <-votesCh
		r.mu.Lock()
		voteResponse := r.ProcessVotes(voteReply)

		// once majority is reached stop waiting
		if voteResponse == true {
			accepted++
			if accepted == majority {
				r.Role = Leader
				r.mu.Unlock()
				return
			}

		} else {
			rejected++
			if rejected == majority {
				r.Role = Follower
				r.mu.Unlock()
				return
			}
		}
		r.mu.Unlock()

	}
}

// process vote reply
func (r *Raft) ProcessVotes(rvr *RequestVoteReply) bool {
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
