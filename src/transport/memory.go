package transport

import (
  "fmt"
  "math/rand"
  "sync"
  "time"
)

const (
  // Chance that an unreliable network drops a request, and separately its reply.
  dropRate = 0.1
  // A failed call waits up to this long before returning false, like a sender timing out.
  maxFailWait = 100 * time.Millisecond
)

type Handler func(args any, reply any)

type Network struct {
  mu        sync.Mutex
  handlers  map[int]map[string]Handler
  connected map[int]bool
  group     map[int]int // nodes can only talk if their group numbers match
  reliable  bool
  minDelay  time.Duration
  maxDelay  time.Duration
  rng       *rand.Rand
}

type Endpoint struct {
  network *Network
  id      int
}

// delivery is everything Call needs to decide about one message, drawn under network.mu.
type delivery struct {
  handler     Handler
  reachable   bool
  dropRequest bool
  dropReply   bool
  delay       time.Duration
  failWait    time.Duration
}

func NewNetwork(seed int64) *Network {
  return &Network{
    handlers:  make(map[int]map[string]Handler),
    connected: make(map[int]bool),
    group:     make(map[int]int),
    reliable:  true,
    rng:       rand.New(rand.NewSource(seed)),
  }
}

func (n *Network) Register(id int, rpcName string, h Handler) {
  n.mu.Lock()
  defer n.mu.Unlock()
  if _, ok := n.handlers[id]; !ok {
    n.handlers[id] = make(map[string]Handler)
  }
  n.handlers[id][rpcName] = h
}

func (n *Network) Endpoint(id int) *Endpoint {
  return &Endpoint{
    network: n,
    id:      id,
  }
}

func (n *Network) Connect(id int) {
  n.mu.Lock()
  defer n.mu.Unlock()
  n.connected[id] = true
}

func (n *Network) Disconnect(id int) {
  n.mu.Lock()
  defer n.mu.Unlock()
  n.connected[id] = false
}

func (n *Network) SetReliable(reliable bool) {
  n.mu.Lock()
  defer n.mu.Unlock()
  n.reliable = reliable
}

func (n *Network) SetDelay(min, max time.Duration) {
  if min < 0 || max < min {
    panic(fmt.Sprintf("invalid delay range [%v, %v]", min, max))
  }
  n.mu.Lock()
  defer n.mu.Unlock()
  n.minDelay = min
  n.maxDelay = max
}

// Partition splits the cluster so nodes can only reach nodes in the same group.
// Nodes not listed in any group end up together in one extra group.
func (n *Network) Partition(groups ...[]int) {
  n.mu.Lock()
  defer n.mu.Unlock()
  n.group = make(map[int]int)
  for i, members := range groups {
    for _, id := range members {
      n.group[id] = i + 1
    }
  }
}

func (n *Network) Heal() {
  n.mu.Lock()
  defer n.mu.Unlock()
  n.group = make(map[int]int)
}

// canReach assumes the caller holds n.mu.
func (n *Network) canReach(from, to int) bool {
  return n.connected[from] && n.connected[to] && n.group[from] == n.group[to]
}

// randDuration assumes the caller holds n.mu, since rand.Rand is not safe for concurrent use.
func (n *Network) randDuration(min, max time.Duration) time.Duration {
  if max <= min {
    return min
  }
  return min + time.Duration(n.rng.Int63n(int64(max-min)+1))
}

// The handler must run without network.mu held, so Call only holds the lock inside plan and stillReachable.
func (e *Endpoint) Call(peer int, rpcName string, args any, reply any) bool {
  d := e.plan(peer, rpcName)
  if !d.reachable || d.handler == nil || d.dropRequest {
    time.Sleep(d.failWait)
    return false
  }

  time.Sleep(d.delay)
  d.handler(args, reply)

  if d.dropReply || !e.stillReachable(peer) {
    return false
  }
  return true
}

func (e *Endpoint) plan(peer int, rpcName string) delivery {
  n := e.network
  n.mu.Lock()
  defer n.mu.Unlock()

  d := delivery{
    handler:   n.handlers[peer][rpcName],
    reachable: n.canReach(e.id, peer),
    delay:     n.randDuration(n.minDelay, n.maxDelay),
    failWait:  n.randDuration(0, maxFailWait),
  }
  if !n.reliable {
    d.dropRequest = n.rng.Float64() < dropRate
    d.dropReply = n.rng.Float64() < dropRate
  }
  return d
}

// stillReachable catches a disconnect or partition that happened while the handler was running.
func (e *Endpoint) stillReachable(peer int) bool {
  e.network.mu.Lock()
  defer e.network.mu.Unlock()
  return e.network.canReach(e.id, peer)
}

var _ Transport = (*Endpoint)(nil)
