package raft

import (
	"net/rpc"
	"time"
)

func call(addr, method string, args any, reply any) bool {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return false
	}
	defer client.Close()
	done := make(chan error, 1)
	go func() { done <- client.Call(method, args, reply) }()
	select {
	case err := <-done:
		return err == nil
	case <-time.After(450 * time.Millisecond):
		return false
	}
}
