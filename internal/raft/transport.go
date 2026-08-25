package raft

import (
	"net/http"
	"time"
)

var httpClient = &http.Client{
	Timeout: 4 * time.Second,
}

func (r *Raft) StartHttpServer() error {
	multiplexer := http.NewServeMux()

	multiplexer.HandleFunc("/request-vote", r.handleRequestVoteHTTP)
	multiplexer.HandleFunc("/append-entries", r.HandleAppendEntriesHTTP)

	return http.ListenAndServe(r.Address, multiplexer)
}

