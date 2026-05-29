package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"raft-bank/internal/bank"
	"raft-bank/internal/config"
	"raft-bank/internal/raft"
	"time"
)

type Tester struct {
	addrs map[int]string
	reqID int
}

func main() {
	scenario := flag.Int("scenario", 1, "scenario 1 or 2")
	docker := flag.Bool("docker", false, "use docker hostnames")
	flag.Parse()
	t := &Tester{addrs: config.AllAddresses(*docker), reqID: 1}
	t.waitForCluster()
	switch *scenario {
	case 1:
		t.scenario1()
	case 2:
		t.scenario2()
	default:
		log.Fatal("unknown scenario")
	}
}

func (t *Tester) dial(id int) (*rpc.Client, error) { return rpc.Dial("tcp", t.addrs[id]) }

func (t *Tester) waitForCluster() {
	for i := 1; i <= 40; i++ {
		ok := 0
		for id := 1; id <= config.NumServers; id++ {
			if c, err := t.dial(id); err == nil {
				ok++
				c.Close()
			}
		}
		if ok == config.NumServers {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (t *Tester) status(id int) (raft.StatusReply, bool) {
	c, err := t.dial(id)
	if err != nil {
		return raft.StatusReply{}, false
	}
	defer c.Close()
	var r raft.StatusReply
	if err := c.Call("Node.Status", raft.StatusArgs{}, &r); err != nil {
		return r, false
	}
	return r, true
}

func (t *Tester) leader() int {
	for attempts := 0; attempts < 60; attempts++ {
		for id := 1; id <= config.NumServers; id++ {
			r, ok := t.status(id)
			if ok && !r.Dead && r.State == raft.Leader {
				fmt.Printf("leader is S%d term %d\n", id, r.CurrentTerm)
				return id
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return 0
}

func (t *Tester) kill(id int) {
	c, err := t.dial(id)
	if err != nil {
		return
	}
	defer c.Close()
	var r raft.ControlReply
	_ = c.Call("Node.Kill", raft.ControlArgs{ID: id}, &r)
	fmt.Println(r.Message)
}

func (t *Tester) restart(id int) {
	c, err := t.dial(id)
	if err != nil {
		return
	}
	defer c.Close()
	var r raft.ControlReply
	_ = c.Call("Node.Restart", raft.ControlArgs{ID: id}, &r)
	fmt.Println(r.Message)
}

func (t *Tester) send(n int) {
	for i := 0; i < n; i++ {
		req := bank.MakeRequest(t.reqID)
		t.reqID++
		ok := false
		leader := t.leader()
		if leader == 0 {
			fmt.Printf("request %d failed: no leader\n", req.ID)
			continue
		}
		for attempt := 0; attempt < 10 && !ok; attempt++ {
			c, err := t.dial(leader)
			if err != nil {
				leader = leader%config.NumServers + 1
				continue
			}
			var reply raft.ClientReply
			err = c.Call("Node.HandleClientRequest", raft.ClientRequest(req), &reply)
			c.Close()
			if err == nil && reply.Success {
				fmt.Printf("request %d ok via S%d\n", req.ID, reply.LeaderID)
				ok = true
				break
			}
			if reply.LeaderID > 0 {
				leader = reply.LeaderID
			} else {
				leader = t.leader()
			}
			time.Sleep(150 * time.Millisecond)
		}
		if !ok {
			fmt.Printf("request %d not committed\n", req.ID)
		}
	}
}

func (t *Tester) catchUp() {
	l := t.leader()
	if l == 0 {
		return
	}
	c, err := t.dial(l)
	if err != nil {
		return
	}
	defer c.Close()
	var r raft.ControlReply
	_ = c.Call("Node.ForceCatchUp", raft.StatusArgs{}, &r)
	time.Sleep(2 * time.Second)
}

func (t *Tester) verify() {
	t.catchUp()
	logs := [][]raft.LogEntry{}
	for id := 1; id <= config.NumServers; id++ {
		r, ok := t.status(id)
		if ok {
			fmt.Printf("S%d dead=%v term=%d commit=%d logLen=%d\n", id, r.Dead, r.CurrentTerm, r.CommitIndex, len(r.Log))
			logs = append(logs, r.Log)
		}
	}
	if raft.LogsEqual(logs) {
		fmt.Println("PASS: all logs are identical")
	} else {
		fmt.Println("FAIL: logs differ")
	}
}

func (t *Tester) scenario1() {
	fmt.Println("SCENARIO 1")
	l := t.leader()
	if l == 0 {
		log.Fatal("no initial leader")
	}
	follower := 5
	if follower == l {
		follower = 4
	}
	t.kill(follower)
	t.send(20)
	t.kill(l)
	_ = t.leader()
	t.send(30)
	l2 := t.leader()
	if l2 != 0 {
		t.kill(l2)
	}
	t.restart(follower)
	t.restart(l)
	if l2 != 0 && l2 != l && l2 != follower {
		t.restart(l2)
	}
	_ = t.leader()
	t.send(40)
	t.verify()
}

func (t *Tester) scenario2() {
	fmt.Println("SCENARIO 2")
	l := t.leader()
	if l == 0 {
		log.Fatal("no initial leader")
	}
	deadA, deadB := 4, 5
	if l == deadA {
		deadA = 3
	}
	if l == deadB {
		deadB = 2
	}
	t.kill(deadA)
	t.kill(deadB)
	t.send(20)
	t.kill(l)
	fmt.Println("election should fail with only two live servers")
	time.Sleep(4 * time.Second)
	t.restart(deadA)
	_ = t.leader()
	t.send(30)
	l2 := t.leader()
	if l2 != 0 {
		t.kill(l2)
	}
	t.restart(l)
	if l2 != 0 && l2 != l && l2 != deadA && l2 != deadB {
		t.restart(l2)
	}
	_ = t.leader()
	t.send(40)
	t.restart(deadB)
	t.catchUp()
	t.verify()
}
