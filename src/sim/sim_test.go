package sim

import (
	"testing"

	"github.com/jason-zhl/driftwood/raft"
)

// pingNode is a stand-in for Raft. Every period ticks it pings each peer;
// a ping is answered with a pong. MsgRequestVote / MsgRequestVoteReply stand in
// for ping / pong so raft.Message needs no test-only types.
type pingNode struct {
	id     int
	peers  []int // must stay sorted so Tick emits messages in a fixed order
	period int
	ticks  int

	pingsSent int
	pongsSent int
	pongsRecv int
}

func (n *pingNode) Tick() []raft.Message {
	n.ticks++
	if n.period <= 0 || n.ticks%n.period != 0 {
		return nil
	}
	msgs := make([]raft.Message, 0, len(n.peers))
	for _, peer := range n.peers {
		n.pingsSent++
		msgs = append(msgs, raft.Message{
			Type: raft.MsgRequestVote,
			From: n.id,
			To:   peer,
		})
	}
	return msgs
}

func (n *pingNode) Step(m raft.Message) []raft.Message {
	switch m.Type {
	case raft.MsgRequestVote:
		n.pongsSent++
		return []raft.Message{{
			Type: raft.MsgRequestVoteReply,
			From: n.id,
			To:   m.From,
		}}
	case raft.MsgRequestVoteReply:
		n.pongsRecv++
	}
	return nil
}

func newPingCluster(t *testing.T, n int, seed int64, period int) (*Sim, []*pingNode) {
	t.Helper()
	s := NewSim(seed)
	nodes := make([]*pingNode, n)
	for i := 0; i < n; i++ {
		peers := make([]int, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		nodes[i] = &pingNode{id: i, peers: peers, period: period}
		s.AddNode(i, nodes[i])
	}
	return s, nodes
}

func totalPongs(nodes []*pingNode) int {
	sum := 0
	for _, n := range nodes {
		sum += n.pongsRecv
	}
	return sum
}

func snapshot(nodes []*pingNode) [][4]int {
	out := make([][4]int, len(nodes))
	for i, n := range nodes {
		out[i] = [4]int{n.ticks, n.pingsSent, n.pongsSent, n.pongsRecv}
	}
	return out
}

// --- event queue ---

func TestQueuePopsInTimeOrder(t *testing.T) {
	s := NewSim(1)
	for _, tm := range []int{5, 1, 9, 3, 7, 0, 2} {
		s.schedule(&Event{time: tm})
	}

	prev := -1
	for e := s.nextEvent(); e != nil; e = s.nextEvent() {
		if e.time < prev {
			t.Fatalf("popped time %d after %d", e.time, prev)
		}
		prev = e.time
	}
}

func TestQueueBreaksTiesBySchedulingOrder(t *testing.T) {
	s := NewSim(1)
	s.schedule(&Event{time: 4, node: 0})
	s.schedule(&Event{time: 2, node: 1})
	s.schedule(&Event{time: 4, node: 2})
	s.schedule(&Event{time: 2, node: 3})
	s.schedule(&Event{time: 4, node: 4})

	want := []int{1, 3, 0, 2, 4}
	for i, w := range want {
		e := s.nextEvent()
		if e == nil {
			t.Fatalf("queue empty after %d events, want %d", i, len(want))
		}
		if e.node != w {
			t.Errorf("event %d: got node %d, want %d", i, e.node, w)
		}
	}
}

func TestQueueEmpty(t *testing.T) {
	s := NewSim(1)
	if e := s.nextEvent(); e != nil {
		t.Errorf("got %+v from empty queue, want nil", e)
	}

	s.schedule(&Event{time: 1})
	s.nextEvent()
	if e := s.nextEvent(); e != nil {
		t.Errorf("got %+v after draining queue, want nil", e)
	}
}

func TestQueueInterleavedPushPop(t *testing.T) {
	s := NewSim(1)
	s.schedule(&Event{time: 10})
	s.schedule(&Event{time: 3})
	if e := s.nextEvent(); e.time != 3 {
		t.Fatalf("got time %d, want 3", e.time)
	}

	s.schedule(&Event{time: 1})
	s.schedule(&Event{time: 20})
	for _, want := range []int{1, 10, 20} {
		if e := s.nextEvent(); e.time != want {
			t.Errorf("got time %d, want %d", e.time, want)
		}
	}
}

func TestScheduleAssignsIncreasingSeq(t *testing.T) {
	s := NewSim(1)
	a := &Event{time: 1}
	b := &Event{time: 1}
	s.schedule(a)
	s.schedule(b)
	if a.seq >= b.seq {
		t.Errorf("seq not increasing: a=%d b=%d", a.seq, b.seq)
	}
}

// --- main loop + delivery ---

func TestRunUntilEmptyDoesNotPanic(t *testing.T) {
	NewSim(1).RunUntil(100)
}

func TestNodesTickOverTime(t *testing.T) {
	s, nodes := newPingCluster(t, 3, 1, 100) // period high enough that no pings fire
	s.RunUntil(9)
	for i, n := range nodes {
		if n.ticks != 10 { // ticks at t=0..9
			t.Errorf("node %d: got %d ticks, want 10", i, n.ticks)
		}
	}
	if s.now != 9 {
		t.Errorf("now=%d, want 9", s.now)
	}
}

func TestPingsAreDeliveredAndAnswered(t *testing.T) {
	s, nodes := newPingCluster(t, 3, 1, 5)
	s.RunUntil(20)

	if got := totalPongs(nodes); got == 0 {
		t.Fatalf("no pongs received")
	}
	for i, n := range nodes {
		if n.pingsSent == 0 {
			t.Errorf("node %d sent no pings", i)
		}
		if n.pongsRecv == 0 {
			t.Errorf("node %d received no pongs", i)
		}
		if n.pongsSent == 0 {
			t.Errorf("node %d sent no pongs", i)
		}
	}
}

func TestSelfMessagesAreAllowed(t *testing.T) {
	s := NewSim(1)
	n := &pingNode{id: 0, peers: []int{0}, period: 1}
	s.AddNode(0, n)
	s.RunUntil(2)
	if n.pongsRecv == 0 {
		t.Errorf("node never received a pong from itself")
	}
}

// --- fault controls ---

func TestDisconnectBlocksDelivery(t *testing.T) {
	s, nodes := newPingCluster(t, 2, 1, 1)
	s.Disconnect(1)
	s.RunUntil(20)
	if totalPongs(nodes) != 0 {
		t.Errorf("got %d pongs across a disconnect, want 0", totalPongs(nodes))
	}
}

func TestReconnectRestoresDelivery(t *testing.T) {
	s, nodes := newPingCluster(t, 2, 1, 1)
	s.Disconnect(1)
	s.RunUntil(10)
	if totalPongs(nodes) != 0 {
		t.Fatalf("pongs while disconnected: %d", totalPongs(nodes))
	}

	s.Connect(1)
	s.RunUntil(20)
	if totalPongs(nodes) == 0 {
		t.Errorf("no pongs after reconnect")
	}
}

func TestPartitionBlocksAcrossGroups(t *testing.T) {
	s, nodes := newPingCluster(t, 4, 1, 1)
	s.Partition([]int{0, 1}, []int{2, 3})
	s.RunUntil(15)

	// Within a group, pongs should flow. Across groups, they should not.
	// Each node only peers with the other three, so cross-group pings are attempted
	// and must produce no replies from the other side.
	if nodes[0].pongsRecv == 0 || nodes[1].pongsRecv == 0 {
		t.Errorf("group {0,1} got no pongs: %d %d", nodes[0].pongsRecv, nodes[1].pongsRecv)
	}
	if nodes[2].pongsRecv == 0 || nodes[3].pongsRecv == 0 {
		t.Errorf("group {2,3} got no pongs: %d %d", nodes[2].pongsRecv, nodes[3].pongsRecv)
	}

	// A one-node partition receives nothing, since its only peers are unreachable.
	s2, nodes2 := newPingCluster(t, 3, 1, 1)
	s2.Partition([]int{0}, []int{1, 2})
	s2.RunUntil(15)
	if nodes2[0].pongsRecv != 0 {
		t.Errorf("isolated node 0 got %d pongs, want 0", nodes2[0].pongsRecv)
	}
	if nodes2[1].pongsRecv == 0 || nodes2[2].pongsRecv == 0 {
		t.Errorf("group {1,2} got no pongs")
	}
}

func TestHealRestoresAllLinks(t *testing.T) {
	s, nodes := newPingCluster(t, 3, 1, 1)
	s.Partition([]int{0}, []int{1}, []int{2})
	s.RunUntil(5)
	if totalPongs(nodes) != 0 {
		t.Fatalf("pongs during full partition: %d", totalPongs(nodes))
	}

	s.Heal()
	s.RunUntil(15)
	if totalPongs(nodes) == 0 {
		t.Errorf("no pongs after Heal")
	}
}

func TestDropRateDropsMessages(t *testing.T) {
	reliable, rNodes := newPingCluster(t, 3, 42, 1)
	reliable.RunUntil(30)
	reliablePongs := totalPongs(rNodes)

	lossy, lNodes := newPingCluster(t, 3, 42, 1)
	lossy.SetDropRate(0.5)
	lossy.RunUntil(30)
	lossyPongs := totalPongs(lNodes)

	if reliablePongs == 0 {
		t.Fatalf("reliable run got no pongs")
	}
	if lossyPongs >= reliablePongs {
		t.Errorf("drop rate 0.5 got %d pongs, reliable got %d; want fewer", lossyPongs, reliablePongs)
	}
	if lossyPongs == 0 {
		t.Errorf("every message was dropped, drop rate looks stuck at 1")
	}
}

func TestSetDropRateZeroIsReliable(t *testing.T) {
	s, nodes := newPingCluster(t, 2, 7, 1)
	s.SetDropRate(0.9)
	s.SetDropRate(0)
	s.RunUntil(20)
	// With period 1 and 2 nodes, each tick sends 1 ping each way; plenty of pongs expected.
	if totalPongs(nodes) == 0 {
		t.Errorf("no pongs after SetDropRate(0)")
	}
}

func TestInvalidDropRatePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("SetDropRate(1.5) did not panic")
		}
	}()
	NewSim(1).SetDropRate(1.5)
}

func TestDelayDefersDelivery(t *testing.T) {
	s, nodes := newPingCluster(t, 2, 1, 1)
	s.SetDelay(10, 10)

	s.RunUntil(5)
	if totalPongs(nodes) != 0 {
		t.Fatalf("got %d pongs before delay elapsed", totalPongs(nodes))
	}

	s.RunUntil(25)
	if totalPongs(nodes) == 0 {
		t.Errorf("no pongs after delay elapsed")
	}
}

func TestInvalidDelayPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("SetDelay(5, 1) did not panic")
		}
	}()
	NewSim(1).SetDelay(5, 1)
}

func TestInFlightMessageLostOnDisconnect(t *testing.T) {
	s, nodes := newPingCluster(t, 2, 1, 1)
	s.SetDelay(5, 5)

	// First ping wave is scheduled at t=0 with delivery at t=5.
	s.RunUntil(0)
	if nodes[0].pingsSent == 0 && nodes[1].pingsSent == 0 {
		t.Fatalf("no pings sent at t=0")
	}

	s.Disconnect(1)
	s.RunUntil(20)
	if totalPongs(nodes) != 0 {
		t.Errorf("in-flight messages were delivered after disconnect: %d pongs", totalPongs(nodes))
	}
}

// --- determinism ---

func TestSameSeedIsDeterministic(t *testing.T) {
	run := func(seed int64) [][4]int {
		s, nodes := newPingCluster(t, 5, seed, 2)
		s.SetDropRate(0.3)
		s.SetDelay(0, 4)
		s.Partition([]int{0, 1, 2}, []int{3, 4})
		s.RunUntil(10)
		s.Heal()
		s.Disconnect(1)
		s.RunUntil(20)
		s.Connect(1)
		s.RunUntil(40)
		return snapshot(nodes)
	}

	a := run(99)
	b := run(99)
	if len(a) != len(b) {
		t.Fatalf("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("node %d: first run %v, second run %v", i, a[i], b[i])
		}
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	run := func(seed int64) int {
		s, nodes := newPingCluster(t, 4, seed, 1)
		s.SetDropRate(0.4)
		s.SetDelay(0, 3)
		s.RunUntil(50)
		return totalPongs(nodes)
	}

	if run(1) == run(2) && run(1) == run(3) {
		// Extremely unlikely with drops+delays; if it happens, widen the run.
		t.Errorf("seeds 1, 2, and 3 all produced the same pong count; expected some divergence")
	}
}
