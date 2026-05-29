package bank

import "fmt"

type ClientRequest struct {
	ID      int
	Command string
}

type ClientReply struct {
	Success  bool
	LeaderID int
	Message  string
}

func MakeRequest(id int) ClientRequest {
	return ClientRequest{ID: id, Command: fmt.Sprintf("deposit account%d %d", id%5, 10+id)}
}
