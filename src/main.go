package main

import (
	"fmt"
	"github.com/jason-zhl/driftwood/raft"
	"github.com/jason-zhl/driftwood/sim"
)

func main() {
	const (
		nNodes            = 3
		heartbeatInterval = 2
		until             = 40 // virtual time to run
	)
	// Stagger timeouts so one node times out first (avoids split votes).
	timeouts := []int{5, 7, 9}

	s := sim.NewSim(1)
	s.SetDelay(0, 0) // deliver immediately so the trace is easy to read

	nodes := make([]*raft.RaftNode, nNodes)
	for id := 0; id < nNodes; id++ {
		peers := make([]int, 0, nNodes-1)
		for p := 0; p < nNodes; p++ {
			if p != id {
				peers = append(peers, p)
			}
		}
		nodes[id] = raft.NewRaftNode(id, peers, timeouts[id], heartbeatInterval)
		s.AddNode(id, nodes[id])
	}

	fmt.Println("=== raft election trace ===")
	fmt.Printf("timeouts=%v heartbeat=%d runUntil=%d\n\n", timeouts, heartbeatInterval, until)

	s.Trace = electionTrace(nodes)
	s.RunUntil(until)

	fmt.Printf("\n=== final %s ===\n", formatCluster(nodes))
}
