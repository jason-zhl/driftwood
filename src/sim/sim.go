package sim

import (
	"container/heap"
	"fmt"
	"github.com/jason-zhl/driftwood/raft"
	"math/rand"
)

type Node interface {
	Tick() []raft.Message
	Step(m raft.Message) []raft.Message
}

type EventType int

const (
	EventTick EventType = iota
	EventDeliver
)

type Event struct {
	time int
	seq  int
	kind EventType
	node int          // the node to tick, for EventTick
	msg  raft.Message // the message to deliver to msg.To, for EventDeliver
}

// eventQueue implements heap.Interface. Use heap.Push and heap.Pop, never append or slice it directly.
type eventQueue []*Event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].time != q[j].time {
		return q[i].time < q[j].time
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*Event)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return e
}

type Sim struct {
	now   int        // virtual clock, in ticks
	queue eventQueue // min-heap ordered by (time, seq)
	seq   int        // breaks ties so same-time events always pop in the same order
	rng   *rand.Rand // the only random source the network uses
	nodes map[int]Node

	// fault state
	connected map[int]bool
	group     map[int]int
	dropRate  float64
	minDelay  int
	maxDelay  int
}

func NewSim(seed int64) *Sim {
	return &Sim{
		rng:       rand.New(rand.NewSource(seed)),
		nodes:     make(map[int]Node),
		connected: make(map[int]bool),
		group:     make(map[int]int),
	}
}

func (s *Sim) AddNode(id int, node Node) {
	s.nodes[id] = node
	s.connected[id] = true
	s.schedule(&Event{
		time: s.now,
		kind: EventTick,
		node: id,
	})
}

// schedule is the only way events should enter the queue, so every event gets a unique seq.
func (s *Sim) schedule(e *Event) {
	e.seq = s.seq
	s.seq++
	heap.Push(&s.queue, e)
}

func (s *Sim) nextEvent() *Event {
	if s.queue.Len() == 0 {
		return nil
	}
	return heap.Pop(&s.queue).(*Event)
}

func (s *Sim) canReach(from, to int) bool {
	return s.connected[from] && s.connected[to] && s.group[from] == s.group[to]
}

func (s *Sim) send(msg raft.Message) {
	if !s.canReach(msg.From, msg.To) {
		return
	}
	if s.rng.Float64() < s.dropRate {
		return
	}
	delay := s.minDelay + s.rng.Intn(s.maxDelay-s.minDelay+1)
	s.schedule(&Event{
		time: s.now + delay,
		kind: EventDeliver,
		node: msg.To,
		msg:  msg,
	})
}

func (s *Sim) Step() bool {
	if s.queue.Len() == 0 {
		return false
	}

	curr := s.nextEvent()
	s.now = curr.time

	var msgs []raft.Message
	switch curr.kind {
	case EventTick:
		msgs = s.nodes[curr.node].Tick()
		s.schedule(&Event{time: s.now + 1, kind: EventTick, node: curr.node})
	case EventDeliver:
		if s.canReach(curr.msg.From, curr.msg.To) {
			msgs = s.nodes[curr.msg.To].Step(curr.msg)
		}
	}
	for _, msg := range msgs {
		s.send(msg)
	}
	return true
}

func (s *Sim) RunUntil(t int) {
	for s.queue.Len() > 0 && s.queue[0].time <= t {
		ok := s.Step()
		if !ok {
			break
		}
	}
}

func (s *Sim) Connect(id int) { s.connected[id] = true }

func (s *Sim) Disconnect(id int) { s.connected[id] = false }

func (s *Sim) SetDelay(min, max int) {
	if min < 0 || max < min {
		panic(fmt.Sprintf("invalid delay range [%v, %v]", min, max))
	}
	s.minDelay = min
	s.maxDelay = max
}

func (s *Sim) SetDropRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("invalid drop rate %v", rate))
	}
	s.dropRate = rate
}

// Partition splits the cluster so nodes can only reach nodes in the same group.
// Nodes not listed in any group end up together in one extra group.
func (s *Sim) Partition(groups ...[]int) {
	s.group = make(map[int]int)
	for i, members := range groups {
		for _, id := range members {
			s.group[id] = i + 1
		}
	}
}

func (s *Sim) Heal() { s.group = make(map[int]int) }
