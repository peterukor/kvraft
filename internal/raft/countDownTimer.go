package raft

import (
	"maps"
	"math/rand/v2"
	"time"
)

var electionMin = 800 * time.Millisecond
var electionMax = 1500 * time.Millisecond

var leaderTimer = 100 * time.Millisecond

func (r *Raft) NodeRandomTimer(min time.Duration, max time.Duration) time.Duration {
	diff := max - min
	random := rand.Int64N(int64(diff))
	return min + time.Duration(random)
}

// initialize an election countdown timer that resets once the node recieves a heartbeat
func (r *Raft) RunElectionTimer() {
	timer := time.NewTimer(r.NodeRandomTimer(electionMin, electionMax))
	for {
		select {
		case <-timer.C:
			r.mu.Lock()
			if r.Role == Leader {
				// a leader's own timer firing means nothing -- ignore it
				r.mu.Unlock()
				timer.Reset(r.NodeRandomTimer(electionMin, electionMax))
				continue
			}
			r.mu.Unlock()

			r.BecomeCandidate()
			r.RequestVote()

			r.mu.Lock()
			isLeader := r.Role == Leader
			r.mu.Unlock()
			if isLeader {
				r.resetPeersState()
				r.StartLeaderReplication()
			}
			timer.Reset(r.NodeRandomTimer(electionMin, electionMax))

		case <-r.HeartbeatCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(r.NodeRandomTimer(electionMin, electionMax))
		}
	}
}


func (r *Raft) resetPeersState() {
	r.mu.Lock()
	defer r.mu.Unlock()
	lastLogIndex := len(r.Log)
	for _, peer := range r.Peers {
		peer.Match = lastLogIndex - 1
		peer.Next = lastLogIndex
	}
}

func (r *Raft) StartLeaderReplication() {
	// build the map
	r.mu.Lock()
	myTerm := r.CurrentTerm
	peersMap := make(map[string]*Peer, len(r.Peers))
	maps.Copy(peersMap, r.Peers)
	r.mu.Unlock()

	// start Go routines that will run throughout leadership term
	for _, peer := range peersMap {
		go func(peer *Peer) {
			// keeps Go routine up
			for {
				r.mu.Lock()
				// check if it's still leader or term has changed
				if r.Role != Leader || r.CurrentTerm != myTerm{
					r.mu.Unlock()
					break
				}
				r.mu.Unlock()
				r.AppendEntries(peer)
				time.Sleep(leaderTimer)
			}
		}(peer)
	}
}
