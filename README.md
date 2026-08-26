# kvraft

A distributed key-value store built from scratch using the **Raft consensus
algorithm** in Go.

## Current Progress

The Raft layer is currently working across **5 separate Go processes**.

Implemented so far:

- Follower, Candidate, and Leader roles
- Randomized election timers
- RequestVote and majority-based leader election
- HTTP communication between nodes
- AppendEntries heartbeats
- Leader failure detection
- Automatic leader re-election
- Complete log replication
- Handle log conflicts and follower recovery

The cluster has been tested by running five independent nodes and repeatedly
stopping leaders to verify that another node can become leader.

## Current Phase

### Log Replication**

The next step is to finish reliable log replication and conflict recovery.
After that, the Raft layer will be connected to the key-value state machine.

## Running the Project

The current test cluster contains five nodes:

| Node | Address |
|------|---------|
| A | `localhost:8001` |
| B | `localhost:8002` |
| C | `localhost:8003` |
| D | `localhost:8004` |
| E | `localhost:8005` |

Open **five terminals** in the project directory.

### Terminal 1

```bash
go run ./cmd/kvraft-server A
```

### Terminal 2

```bash
go run ./cmd/kvraft-server B
```

### Terminal 3

```bash
go run ./cmd/kvraft-server C
```

### Terminal 4

```bash
go run ./cmd/kvraft-server D
```

### Terminal 5

```bash
go run ./cmd/kvraft-server E
```

One node will eventually become the leader.

For example:

```text
[B] became CANDIDATE term=1
[B] became LEADER term=1
```

To test leader failure, stop the current leader with `Ctrl+C`.

The remaining nodes will eventually start a new election and elect another
leader.

## Raft Test Results

The following screenshot shows multiple five-node election and
leader-failure tests.

[Raft election test screenshot](https://github.com/user-attachments/assets/0e847170-c25f-4620-99da-280cfbadcf55?utm_source=chatgpt.com)

## Project Structure

```text
cmd/
└── kvraft-server/
    └── main.go

internal/raft/
├── candidateElection.go
├── countDownTimer.go
├── electionTransport.go
├── followerElection.go
├── followerReplication.go
├── leaderReplication.go
├── raft.go
├── replicationTransport.go
└── transport.go
```

## Next

- Commit replicated entries
- Build the key-value state machine
- Add persistence and snapshots
- Build the client API
- Build the CLI
