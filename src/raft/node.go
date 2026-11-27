package raft

type MsgType int

const (
	MsgRequestVote MsgType = iota
	MsgRequestVoteReply
	MsgAppendEntries
	MsgAppendEntriesReply
)

type Message struct {
	Type MsgType
	From int
	To int
	Term int
}

type SimpleNode struct {
	id int
	currentTick int
	currentTerm int
}

func (n *SimpleNode) Tick() []Message {
	return nil
}

func (n *SimpleNode) Step(m Message) []Message {
	return nil
}