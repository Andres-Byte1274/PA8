package raft

import "raft-bank/internal/bank"

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

type LogEntry struct {
	Index     int    `json:"index"`
	Term      int    `json:"term"`
	RequestID int    `json:"request_id"`
	Command   string `json:"command"`
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	RequestID    int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

type ControlArgs struct{ ID int }
type ControlReply struct {
	OK      bool
	Message string
}

type StatusArgs struct{}
type StatusReply struct {
	ID          int
	State       State
	CurrentTerm int
	LeaderID    int
	Dead        bool
	CommitIndex int
	Log         []LogEntry
}

type ClientRequest = bank.ClientRequest
type ClientReply = bank.ClientReply
