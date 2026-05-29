package raft

import "sync"

func (n *Node) startElection() {
	n.mu.Lock()
	if n.Dead {
		n.mu.Unlock()
		return
	}
	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	term := n.CurrentTerm
	lastIdx := n.lastLogIndexLocked()
	lastTerm := n.lastLogTermLocked()
	n.resetElectionTimerLocked()
	n.logf("CANDIDATE SERVER %d SENDING A VOTING REQUEST", n.ID)
	n.mu.Unlock()

	var mu sync.Mutex
	votes := 1
	finished := 1
	majority := len(n.Peers)/2 + 1
	won := false

	for id, addr := range n.Peers {
		if id == n.ID {
			continue
		}
		go func(peerID int, peerAddr string) {
			args := RequestVoteArgs{Term: term, CandidateID: n.ID, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
			var reply RequestVoteReply
			ok := call(peerAddr, "Node.RequestVote", args, &reply)
			mu.Lock()
			defer mu.Unlock()
			finished++
			if !ok {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.CurrentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.State == Candidate && n.CurrentTerm == term && reply.VoteGranted {
				votes++
				if !won && votes >= majority {
					won = true
					n.State = Leader
					n.LeaderID = n.ID
					for pid := range n.Peers {
						n.nextIndex[pid] = n.lastLogIndexLocked() + 1
						n.matchIndex[pid] = 0
					}
					n.logf("CANDIDATE SERVER %d WINS THE ELECTION FOR TERM %d", n.ID, term)
					go n.sendHeartbeats()
				}
			}
			if finished == len(n.Peers) && !won && n.State == Candidate && n.CurrentTerm == term {
				n.logf("CANDIDATE SERVER %d LOSES THE ELECTION FOR TERM %d", n.ID, term)
				n.logf("ELECTION TIE, TIMEOUT BEGINS")
				n.resetElectionTimerLocked()
			}
		}(id, addr)
	}
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Dead {
		reply.Term = n.CurrentTerm
		reply.VoteGranted = false
		return nil
	}
	if args.Term < n.CurrentTerm {
		reply.Term = n.CurrentTerm
		reply.VoteGranted = false
		n.logf("SERVER %d DENIES VOTE FOR SERVER %d", n.ID, args.CandidateID)
		return nil
	}
	if args.Term > n.CurrentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	upToDate := args.LastLogTerm > n.lastLogTermLocked() || (args.LastLogTerm == n.lastLogTermLocked() && args.LastLogIndex >= n.lastLogIndexLocked())
	if (n.VotedFor == 0 || n.VotedFor == args.CandidateID) && upToDate {
		n.VotedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimerLocked()
		n.logf("SERVER %d VOTES VOTE FOR SERVER %d", n.ID, args.CandidateID)
	} else {
		reply.VoteGranted = false
		n.logf("SERVER %d DENIES VOTE FOR SERVER %d", n.ID, args.CandidateID)
	}
	reply.Term = n.CurrentTerm
	return nil
}
