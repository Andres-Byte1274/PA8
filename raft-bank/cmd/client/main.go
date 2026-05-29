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

func main() {
	count := flag.Int("n", 1, "number of requests")
	start := flag.Int("start", 1, "first request id")
	docker := flag.Bool("docker", false, "use docker hostnames")
	flag.Parse()
	addrs := config.AllAddresses(*docker)
	leader := 1
	for i := 0; i < *count; i++ {
		req := bank.MakeRequest(*start + i)
		ok := false
		for attempt := 0; attempt < 10 && !ok; attempt++ {
			addr := addrs[leader]
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				leader = leader%config.NumServers + 1
				time.Sleep(200 * time.Millisecond)
				continue
			}
			var reply raft.ClientReply
			err = client.Call("Node.HandleClientRequest", raft.ClientRequest(req), &reply)
			client.Close()
			if err == nil && reply.Success {
				fmt.Printf("request %d committed by S%d\n", req.ID, reply.LeaderID)
				ok = true
				break
			}
			if reply.LeaderID > 0 {
				leader = reply.LeaderID
			} else {
				leader = leader%config.NumServers + 1
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !ok {
			log.Printf("request %d failed", req.ID)
		}
	}
}
