package raft

import (
	"fmt"
	"sort"
	"time"
)

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.Dead || n.State != Leader {
		n.mu.Unlock()
		return
	}
	peers := make(map[int]string, len(n.Peers))
	for id, addr := range n.Peers {
		peers[id] = addr
	}
	n.mu.Unlock()
	for id, addr := range peers {
		if id == n.ID {
			continue
		}
		go n.sendAppendToPeer(id, addr, 0)
	}
}

func (n *Node) HandleClientRequest(req ClientRequest, reply *ClientReply) error {
	n.mu.Lock()
	if n.Dead {
		reply.Success = false
		reply.Message = "server down"
		n.mu.Unlock()
		return nil
	}
	if n.State != Leader {
		reply.Success = false
		reply.LeaderID = n.LeaderID
		reply.Message = fmt.Sprintf("not leader; leader is S%d", n.LeaderID)
		n.mu.Unlock()
		return nil
	}

	// Client retries can deliver the same request ID more than once. Raft must
	// treat request IDs as idempotent; otherwise every retry becomes a duplicate
	// permanent log entry. Heartbeats use request ID 0 and never enter n.Log.
	n.logf("LEADER SERVER %d RECEIVES REQUEST %d FROM CLIENT", n.ID, req.ID)
	entry, exists := n.findRequestLocked(req.ID)
	if !exists {
		entry = LogEntry{Index: len(n.Log) + 1, Term: n.CurrentTerm, RequestID: req.ID, Command: req.Command}
		n.Log = append(n.Log, entry)
		n.persistLogLocked()
		n.logf("LEADER SERVER %d ADDS REQUEST %d TO LOG ENTRY %d", n.ID, req.ID, entry.Index)
	}
	n.matchIndex[n.ID] = max(n.matchIndex[n.ID], entry.Index)
	n.nextIndex[n.ID] = max(n.nextIndex[n.ID], entry.Index+1)
	peers := make(map[int]string, len(n.Peers))
	for id, addr := range n.Peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	ackCh := make(chan bool, len(peers))
	ackCh <- true
	for id, addr := range peers {
		if id == n.ID {
			continue
		}
		go func(peerID int, peerAddr string) {
			ackCh <- n.sendAppendToPeer(peerID, peerAddr, entry.RequestID)
		}(id, addr)
	}

	acks := 0
	majority := len(n.Peers)/2 + 1
	for i := 0; i < len(peers); i++ {
		if <-ackCh {
			acks++
			if acks >= majority {
				break
			}
		}
	}

	n.mu.Lock()
	if n.State == Leader && acks >= majority && n.CommitIndex < entry.Index {
		n.CommitIndex = entry.Index
	}
	committed := n.CommitIndex >= entry.Index
	reply.Success = acks >= majority && committed
	reply.LeaderID = n.ID
	if reply.Success {
		reply.Message = "committed"
	} else {
		reply.Message = "not enough replicas"
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) findRequestLocked(requestID int) (LogEntry, bool) {
	if requestID == 0 {
		return LogEntry{}, false
	}
	for _, e := range n.Log {
		if e.RequestID == requestID {
			return e, true
		}
	}
	return LogEntry{}, false
}

// sendAppendToPeer sends either a heartbeat or all log entries the peer is missing.
// It retries by backing up nextIndex until PrevLogIndex/PrevLogTerm match.
func (n *Node) sendAppendToPeer(peerID int, peerAddr string, requestID int) bool {
	for attempts := 0; attempts < 20; attempts++ {
		n.mu.Lock()
		if n.Dead || n.State != Leader {
			n.mu.Unlock()
			return false
		}
		if n.nextIndex[peerID] <= 0 {
			n.nextIndex[peerID] = len(n.Log) + 1
		}
		if n.nextIndex[peerID] > len(n.Log)+1 {
			n.nextIndex[peerID] = len(n.Log) + 1
		}
		next := n.nextIndex[peerID]
		prevIdx := next - 1
		prevTerm := 0
		if prevIdx > 0 && prevIdx <= len(n.Log) {
			prevTerm = n.Log[prevIdx-1].Term
		}
		entries := append([]LogEntry(nil), n.Log[next-1:]...)
		args := AppendEntriesArgs{Term: n.CurrentTerm, LeaderID: n.ID, PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: n.CommitIndex, RequestID: requestID}
		if requestID != 0 {
			n.logf("LEADER SENDS REQUEST %d TO SERVER %d", requestID, peerID)
		}
		n.mu.Unlock()

		var reply AppendEntriesReply
		ok := call(peerAddr, "Node.AppendEntries", args, &reply)
		if !ok {
			return false
		}

		n.mu.Lock()
		if reply.Term > n.CurrentTerm {
			n.becomeFollowerLocked(reply.Term)
			n.mu.Unlock()
			return false
		}
		if n.State != Leader {
			n.mu.Unlock()
			return false
		}
		if reply.Success {
			n.matchIndex[peerID] = reply.MatchIndex
			n.nextIndex[peerID] = reply.MatchIndex + 1
			if requestID != 0 {
				n.logf("LEADER RECEIVES ACK FROM SERVER %d FOR REQUEST %d", peerID, requestID)
			}
			n.mu.Unlock()
			return true
		}
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		n.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Dead {
		reply.Term = n.CurrentTerm
		reply.Success = false
		return nil
	}
	if args.Term < n.CurrentTerm {
		reply.Term = n.CurrentTerm
		reply.Success = false
		return nil
	}
	if args.Term > n.CurrentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.State = Follower
	n.LeaderID = args.LeaderID
	n.resetElectionTimerLocked()

	if args.PrevLogIndex > len(n.Log) {
		reply.Term = n.CurrentTerm
		reply.Success = false
		reply.MatchIndex = len(n.Log)
		return nil
	}
	if args.PrevLogIndex > 0 && n.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		n.Log = n.Log[:args.PrevLogIndex-1]
		n.persistLogLocked()
		reply.Term = n.CurrentTerm
		reply.Success = false
		reply.MatchIndex = len(n.Log)
		return nil
	}

	changed := false
	for _, e := range args.Entries {
		if e.RequestID != 0 {
			n.logf("FOLLOWER %d RECEIVES REQUEST %d", n.ID, e.RequestID)
		}
		pos := e.Index - 1
		if pos < 0 {
			continue
		}
		if pos < len(n.Log) {
			if n.Log[pos].Term != e.Term || n.Log[pos].RequestID != e.RequestID || n.Log[pos].Command != e.Command {
				n.Log = n.Log[:pos]
				n.Log = append(n.Log, e)
				changed = true
			}
		} else if pos == len(n.Log) {
			n.Log = append(n.Log, e)
			changed = true
		}
	}
	if changed {
		n.persistLogLocked()
	}
	if args.LeaderCommit > n.CommitIndex {
		n.CommitIndex = min(args.LeaderCommit, len(n.Log))
	}
	reply.Term = n.CurrentTerm
	reply.Success = true
	reply.MatchIndex = len(n.Log)
	return nil
}

func (n *Node) ForceCatchUp(args StatusArgs, reply *ControlReply) error {
	n.mu.Lock()
	if n.Dead || n.State != Leader {
		n.mu.Unlock()
		reply.OK = false
		return nil
	}
	entries := append([]LogEntry(nil), n.Log...)
	term := n.CurrentTerm
	commit := n.CommitIndex
	leaderID := n.ID
	peers := make(map[int]string, len(n.Peers))
	for id, addr := range n.Peers {
		peers[id] = addr
	}
	n.mu.Unlock()
	for id, addr := range peers {
		if id == leaderID {
			continue
		}
		go func(peerID int, peerAddr string) {
			args := AppendEntriesArgs{Term: term, LeaderID: leaderID, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries, LeaderCommit: commit, RequestID: 0}
			var r AppendEntriesReply
			_ = call(peerAddr, "Node.ReplaceLog", args, &r)
		}(id, addr)
	}
	reply.OK = true
	reply.Message = "catch-up started"
	return nil
}

func (n *Node) ReplaceLog(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Dead {
		reply.Success = false
		return nil
	}
	if args.Term >= n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.State = Follower
		n.LeaderID = args.LeaderID
	}
	n.Log = append([]LogEntry(nil), args.Entries...)
	n.CommitIndex = min(args.LeaderCommit, len(n.Log))
	n.persistLogLocked()
	reply.Success = true
	reply.MatchIndex = len(n.Log)
	reply.Term = n.CurrentTerm
	return nil
}

func LogsEqual(logs [][]LogEntry) bool {
	if len(logs) == 0 {
		return true
	}
	base, _ := fmtLogs(logs[0])
	for _, l := range logs[1:] {
		s, _ := fmtLogs(l)
		if s != base {
			return false
		}
	}
	return true
}

func fmtLogs(l []LogEntry) (string, error) {
	copyLog := append([]LogEntry(nil), l...)
	sort.Slice(copyLog, func(i, j int) bool { return copyLog[i].Index < copyLog[j].Index })
	return fmt.Sprintf("%v", copyLog), nil
}
