package raft

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// send entries via POST method
func (r *Raft) sendAppendEntries(peer *Peer, args *AppendEntriesArgs) *AppendEntriesReply {

	// ENCODE
	// convert golang structs to json bytes
	data, err := json.Marshal(args)
	if err != nil {
		return nil
	}
	// wrap json bytes in a type Golangs io.Reader can process
	reader := bytes.NewReader(data)

	// SEND AND RECEIVE
	// send POST request and wait for response
	response, err := httpClient.Post(
		"http://"+peer.Address+"/append-entries",
		"application/json",
		reader,
	)
	if err != nil {
		return nil
	}

	// DECODE
	// close response body at the end
	defer response.Body.Close()
	// check status code
	if response.StatusCode != http.StatusOK {
		return nil
	}

	// err = decoder.Decode(&reply)
	// needs actual space to store the decoded data
	var reply AppendEntriesReply

	// creates a json byte decoder for the response stream
	decoder := json.NewDecoder(response.Body)
	// decode data from the stream to a golang struct
	err = decoder.Decode(&reply)
	if err != nil {
		return nil
	}
	return &reply

}

func (r *Raft) HandleAppendEntriesHTTP(response http.ResponseWriter, request *http.Request){

	// VERIFY AND DECODE
	// check erequest method
	if request.Method != http.MethodPost{
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// create actual space to receive data
	var args AppendEntriesArgs
	// creates a json byte decoder for the stream request
	decoder := json.NewDecoder(request.Body)
	// convert to a golang struct with the decoder
	err := decoder.Decode(&args)
	if err != nil{
		http.Error(response, "invalid request", http.StatusBadRequest)
		return
	}

	// PROCESS
	// returns a *AppendEntriesReply
	reply := r.HandleAppendEntries(&args)

	// PREPARE AND ENCODE
	// set response head
	response.Header().Set("Content-type", "application/json")
	// create a json byte encoder for the http stream response
	encoder := json.NewEncoder(response)
	// convert golang structs to json bytes
	err = encoder.Encode(reply)
	if err != nil{
		http.Error(response, "can not complete request", http.StatusInternalServerError)
		return
	}
}
