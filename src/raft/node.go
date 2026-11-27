package raft

import (
	"fmt"
	"slices"
)

// MESSAGE
type MsgType int

const (
	MsgRequestVote MsgType = iota
	MsgRequestVoteReply
	MsgAppendEntries
	MsgAppendEntriesReply
)

type LogEntry struct {
	Term    int
	Command []byte
}

type Message struct {
	Type        MsgType
	CandidateId int

	// AppendEntries
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	Success      bool

	// RequestVote
	LastLogIndex int
	LastLogTerm  int
	VoteGranted  bool

	From int
	To   int
	Term int
}

// ROLES
type RaftRole int

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

// NODE
type RaftNode struct {
	id    int
	peers []int

	currentTick int
	currentTerm int
	votedFor    int
	role        RaftRole

	LastLogIndex int
	LastLogTerm  int

	votesReceived     int
	electionTimeout   int
	heartbeatInterval int
}

func NewRaftNode(id int, peers []int, electionTimeout, heartbeatInterval int) *RaftNode {
	if slices.Contains(peers, id) {
		panic(fmt.Sprintf("node %d is in its own peer list", id))
	}
	if heartbeatInterval <= 0 {
		panic(fmt.Sprintf("heartbeatInterval must be > 0, got %d", heartbeatInterval))
	}
	if electionTimeout <= heartbeatInterval {
		panic(fmt.Sprintf("electionTimeout (%d) must be > heartbeatInterval (%d)", electionTimeout, heartbeatInterval))
	}
	return &RaftNode{
		id:                id,
		peers:             peers,
		currentTick:       0,
		currentTerm:       0,
		votedFor:          -1,
		role:              Follower,
		LastLogIndex:      0,
		LastLogTerm:       0,
		votesReceived:     0,
		electionTimeout:   electionTimeout,
		heartbeatInterval: heartbeatInterval,
	}
}

func (n *RaftNode) ID() int       { return n.id }
func (n *RaftNode) Role() RaftRole { return n.role }
func (n *RaftNode) Term() int     { return n.currentTerm }

func (n *RaftNode) becomeFollower(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
	}
	n.role = Follower
	n.votesReceived = 0
	n.currentTick = 0
}

func (n *RaftNode) becomeLeader() []Message {
	n.role = Leader
	n.currentTick = 0
	return n.heartbeats()
}

func (n *RaftNode) logUpToDate(lastLogIndex, lastLogTerm int) bool {
	if lastLogTerm != n.LastLogTerm {
		return lastLogTerm > n.LastLogTerm
	}
	return lastLogIndex >= n.LastLogIndex
}

func (n *RaftNode) heartbeats() []Message {
	msgs := make([]Message, 0, len(n.peers))
	for _, peer := range n.peers {
		msgs = append(msgs, Message{
			Type:         MsgAppendEntries,
			From:         n.id,
			To:           peer,
			Term:         n.currentTerm,
			PrevLogIndex: n.LastLogIndex,
			PrevLogTerm:  n.LastLogTerm,
			Entries:      nil, // empty => heartbeat
			LeaderCommit: 0,
		})
	}
	return msgs
}

func (n *RaftNode) Tick() []Message {
	n.currentTick++

	if n.role == Leader {
		if n.currentTick < n.heartbeatInterval {
			return nil
		}
		n.currentTick = 0
		return n.heartbeats()
	}

	if n.currentTick < n.electionTimeout {
		return nil
	}

	// Start (or restart) an election.
	n.currentTick = 0
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.votesReceived = 1

	msgs := make([]Message, 0, len(n.peers))
	for _, peer := range n.peers {
		msgs = append(msgs, Message{
			Type:         MsgRequestVote,
			From:         n.id,
			To:           peer,
			CandidateId:  n.id,
			Term:         n.currentTerm,
			LastLogIndex: n.LastLogIndex,
			LastLogTerm:  n.LastLogTerm,
		})
	}

	// Single-node cluster: self-vote is already a majority.
	if n.votesReceived*2 > len(n.peers)+1 {
		return n.becomeLeader()
	}
	return msgs
}

func (n *RaftNode) Step(m Message) []Message {
	switch m.Type {
	case MsgRequestVote:
		if m.Term > n.currentTerm {
			n.becomeFollower(m.Term)
		}

		grant := false
		if m.Term == n.currentTerm &&
			(n.votedFor == -1 || n.votedFor == m.CandidateId) &&
			n.logUpToDate(m.LastLogIndex, m.LastLogTerm) {
			grant = true
			n.votedFor = m.CandidateId
			n.currentTick = 0
		}

		return []Message{{
			Type:        MsgRequestVoteReply,
			From:        n.id,
			To:          m.From,
			Term:        n.currentTerm,
			VoteGranted: grant,
		}}

	case MsgRequestVoteReply:
		if m.Term > n.currentTerm {
			n.becomeFollower(m.Term)
			return nil
		}
		if n.role != Candidate || m.Term < n.currentTerm || !m.VoteGranted {
			return nil
		}

		n.votesReceived++
		if n.votesReceived*2 > len(n.peers)+1 {
			return n.becomeLeader()
		}

	case MsgAppendEntries:
		if m.Term > n.currentTerm {
			n.becomeFollower(m.Term)
		}
		if m.Term < n.currentTerm {
			return []Message{{
				Type:    MsgAppendEntriesReply,
				From:    n.id,
				To:      m.From,
				Term:    n.currentTerm,
				Success: false,
			}}
		}

		// Same term: acknowledge the leader and reset the election timer.
		n.becomeFollower(m.Term)
		n.currentTick = 0

		return []Message{{
			Type:    MsgAppendEntriesReply,
			From:    n.id,
			To:      m.From,
			Term:    n.currentTerm,
			Success: true,
		}}

	case MsgAppendEntriesReply:
		if m.Term > n.currentTerm {
			n.becomeFollower(m.Term)
		}
	}
	return nil
}
