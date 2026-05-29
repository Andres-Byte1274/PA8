package raft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Node struct {
	mu sync.Mutex

	ID          int
	State       State
	CurrentTerm int
	VotedFor    int
	LeaderID    int

	Log         []LogEntry
	CommitIndex int
	LastApplied int

	Peers map[int]string
	Addr  string
	Dead  bool

	nextIndex  map[int]int
	matchIndex map[int]int

	electionTimer *time.Timer
	debugFile     *os.File
	logDir        string
}

func NewNode(id int, addr string, peers map[int]string, logDir string) (*Node, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	df, err := os.OpenFile(filepath.Join(logDir, fmt.Sprintf("server%d_debug.log", id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	n := &Node{ID: id, State: Follower, VotedFor: 0, LeaderID: 0, Peers: peers, Addr: addr, logDir: logDir, debugFile: df, nextIndex: map[int]int{}, matchIndex: map[int]int{}}
	n.loadPersistentLog()
	return n, nil
}

func (n *Node) Serve() error {
	if err := rpc.RegisterName("Node", n); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", n.Addr)
	if err != nil {
		return err
	}
	n.resetElectionTimerLocked()
	go n.ticker()
	for {
		conn, err := ln.Accept()
		if err == nil {
			go rpc.ServeConn(conn)
		}
	}
}

func (n *Node) ticker() {
	for {
		time.Sleep(50 * time.Millisecond)
		n.mu.Lock()
		dead := n.Dead
		isLeader := n.State == Leader
		n.mu.Unlock()
		if dead {
			continue
		}
		if isLeader {
			n.sendHeartbeats()
			time.Sleep(220 * time.Millisecond)
		}
	}
}

func (n *Node) resetElectionTimerLocked() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	d := time.Duration(900+rand.Intn(900)) * time.Millisecond
	n.electionTimer = time.AfterFunc(d, func() {
		n.mu.Lock()
		if n.Dead || n.State == Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.startElection()
	})
}

func (n *Node) becomeFollowerLocked(term int) {
	n.State = Follower
	n.CurrentTerm = term
	n.VotedFor = 0
	n.resetElectionTimerLocked()
}

func (n *Node) lastLogIndexLocked() int {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Index
}

func (n *Node) lastLogTermLocked() int {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Term
}

func (n *Node) lastLogIndex() int { n.mu.Lock(); defer n.mu.Unlock(); return n.lastLogIndexLocked() }
func (n *Node) lastLogTerm() int  { n.mu.Lock(); defer n.mu.Unlock(); return n.lastLogTermLocked() }

func (n *Node) logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("%s %s\n", time.Now().Format(time.RFC3339Nano), msg)
	fmt.Print(line)
	if n.debugFile != nil {
		_, _ = n.debugFile.WriteString(line)
		_ = n.debugFile.Sync()
	}
}

func (n *Node) persistLogLocked() {
	path := filepath.Join(n.logDir, fmt.Sprintf("server%d_raft.log", n.ID))
	b, _ := json.MarshalIndent(n.Log, "", "  ")
	_ = os.WriteFile(path, b, 0644)
}

func (n *Node) loadPersistentLog() {
	path := filepath.Join(n.logDir, fmt.Sprintf("server%d_raft.log", n.ID))
	b, err := os.ReadFile(path)
	if err == nil && len(b) > 0 {
		_ = json.Unmarshal(b, &n.Log)
	}
}

func (n *Node) Kill(args ControlArgs, reply *ControlReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Dead = true
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	reply.OK = true
	reply.Message = fmt.Sprintf("server %d stopped", n.ID)
	return nil
}

func (n *Node) Restart(args ControlArgs, reply *ControlReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.loadPersistentLog()
	n.Dead = false
	n.State = Follower
	n.VotedFor = 0
	n.LeaderID = 0
	n.resetElectionTimerLocked()
	reply.OK = true
	reply.Message = fmt.Sprintf("server %d restarted", n.ID)
	return nil
}

func (n *Node) Status(args StatusArgs, reply *StatusReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply.ID = n.ID
	reply.State = n.State
	reply.CurrentTerm = n.CurrentTerm
	reply.LeaderID = n.LeaderID
	reply.Dead = n.Dead
	reply.CommitIndex = n.CommitIndex
	reply.Log = append([]LogEntry(nil), n.Log...)
	return nil
}
