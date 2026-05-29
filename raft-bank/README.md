# Raft Bank Leader Election Lab

This is a Go implementation of a 5-server Raft-style banking replication system for the assignment.

It includes:

- Raft leader election
- Randomized election timeout
- Heartbeats with request ID `0`
- Client requests starting at ID `1`
- Vote restriction by up-to-date logs
- Follower redirect to current leader
- Term-numbered log entries
- Majority commit
- Simulated server stop/restart
- Debug output files
- Raft JSON log files
- Docker support
- Scenario runner / testing server

## Files

```text
raft-bank/
  cmd/server/main.go        # Starts one Raft server
  cmd/client/main.go        # Sends client requests
  cmd/tester/main.go        # Runs scenario 1 or 2 and verifies logs
  internal/raft/            # Raft implementation
  internal/bank/            # Client request types
  internal/config/          # Server address config
  logs/                     # Generated debug and raft logs
  Dockerfile
  docker-compose.yml
  go.mod
```

## Run with Docker

Start the 5 servers:

```bash
docker compose up --build
```

In another terminal, run scenario 1:

```bash
go run ./cmd/tester --scenario=1
```

Run scenario 2:

```bash
go run ./cmd/tester --scenario=2
```

The servers are exposed on localhost ports `9001` to `9005`, so the tester can run from your host machine.

## Run without Docker

Open 5 terminals:

```bash
go run ./cmd/server --id=1
```

```bash
go run ./cmd/server --id=2
```

```bash
go run ./cmd/server --id=3
```

```bash
go run ./cmd/server --id=4
```

```bash
go run ./cmd/server --id=5
```

Then run:

```bash
go run ./cmd/tester --scenario=1
```

or:

```bash
go run ./cmd/tester --scenario=2
```

## Logs

Debug output files:

```text
logs/server1_debug.log
logs/server2_debug.log
logs/server3_debug.log
logs/server4_debug.log
logs/server5_debug.log
```

Raft log files:

```text
logs/server1_raft.log
logs/server2_raft.log
logs/server3_raft.log
logs/server4_raft.log
logs/server5_raft.log
```

## Required debug messages implemented

The code writes these assignment-required messages:

```text
CANDIDATE SERVER ${ID} SENDING A VOTING REQUEST
SERVER ${ID} VOTES VOTE FOR SERVER ${ID}
SERVER ${ID} DENIES VOTE FOR SERVER ${ID}
CANDIDATE SERVER ${ID} WINS THE ELECTION FOR TERM ${NUM}
CANDIDATE SERVER ${ID} LOSES THE ELECTION FOR TERM ${NUM}
ELECTION TIE, TIMEOUT BEGINS
LEADER SERVER ${ID} RECEIVES REQUEST ${REQUEST-ID} FROM CLIENT
LEADER SERVER ${ID} ADDS REQUEST ${REQUEST-ID} TO LOG ENTRY ${INDEX}
LEADER SENDS REQUEST ${REQUEST-ID} TO SERVER ${ID}
FOLLOWER ${ID} RECEIVES REQUEST ${REQUEST-ID}
LEADER RECEIVES ACK FROM SERVER ${ID} FOR REQUEST ${REQUEST-ID}
```

## Important note

This project uses simulated failures through RPC calls to `Kill` and `Restart`. The server process stays alive, but the Raft node behaves as failed: it stops voting, stops accepting AppendEntries, and stops starting elections until restarted.

That makes it easier to record the assignment scenarios while keeping Docker containers running.
