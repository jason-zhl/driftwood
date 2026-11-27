package main

import (
	"fmt"
	"strings"

	"github.com/jason-zhl/driftwood/raft"
	"github.com/jason-zhl/driftwood/sim"
)

func electionTrace(nodes []*raft.RaftNode) sim.TraceFunc {
	return func(time int, kind sim.EventType, node int, msg raft.Message, emitted []raft.Message) {
		switch kind {
		case sim.EventTick:
			n := nodes[node]
			fmt.Printf("t=%-3d TICK  node=%d  %s term=%d\n",
				time, node, n.Role(), n.Term())
		case sim.EventDeliver:
			fmt.Printf("t=%-3d RECV  node=%d  %s\n",
				time, node, msg)
		}
		for _, m := range emitted {
			fmt.Printf("           SEND  %s\n", m)
		}
		if len(emitted) > 0 || kind == sim.EventDeliver {
			fmt.Printf("           state %s\n", formatCluster(nodes))
		}
	}
}

func formatCluster(nodes []*raft.RaftNode) string {
	parts := make([]string, len(nodes))
	for i, n := range nodes {
		parts[i] = fmt.Sprintf("%d:%s(t=%d)", n.ID(), n.Role(), n.Term())
	}
	return strings.Join(parts, " ")
}
