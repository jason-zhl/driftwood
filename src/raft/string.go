package raft

import (
	"fmt"
	"strings"
)

func (t MsgType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteReply:
		return "RequestVoteReply"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesReply:
		return "AppendEntriesReply"
	default:
		return fmt.Sprintf("MsgType(%d)", int(t))
	}
}

func (r RaftRole) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("RaftRole(%d)", int(r))
	}
}

func (m Message) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %d→%d term=%d", m.Type, m.From, m.To, m.Term)
	switch m.Type {
	case MsgRequestVote:
		fmt.Fprintf(&b, " cand=%d", m.CandidateId)
	case MsgRequestVoteReply:
		fmt.Fprintf(&b, " granted=%v", m.VoteGranted)
	case MsgAppendEntries:
		fmt.Fprintf(&b, " entries=%d", len(m.Entries))
	case MsgAppendEntriesReply:
		fmt.Fprintf(&b, " success=%v", m.Success)
	}
	return b.String()
}
