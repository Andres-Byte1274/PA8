package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"raft-bank/internal/config"
	"raft-bank/internal/raft"
	"time"
)

func main() {
	id := flag.Int("id", 1, "server id 1..5")
	logDir := flag.String("logs", "logs", "log directory")
	docker := flag.Bool("docker", false, "use docker hostnames for peers")
	flag.Parse()
	rand.Seed(time.Now().UnixNano() + int64(*id))
	peers := config.AllAddresses(*docker)
	addr := peers[*id]
	if *docker {
		addr = fmt.Sprintf("0.0.0.0:%d", 9000+*id)
	}
	n, err := raft.NewNode(*id, addr, peers, *logDir)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stdout, "S%d listening on %s\n", *id, addr)
	log.Fatal(n.Serve())
}
