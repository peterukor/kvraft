# kvraft

A distributed key-value store built from scratch using the Raft consensus algorithms

## Status

🚧 Under development

## Goals

- Implement Raft consensus from scratch
- Build a replicated key-value state machine
- Support node failures and recovery
- Add persistence and snapshots
- Provide benchmarking and chaos testing

### Requirements

- Go 1.27.0+

### Run

```bash
go run ./cmd/kvraft
