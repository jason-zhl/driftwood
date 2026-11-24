package transport

import (
  "math/rand"
  "sync"
)

type Handler func(args any, reply any)

type Network struct {
  mu        sync.Mutex
  handlers  map[int]map[string]Handler
  connected map[int]bool
  rng       *rand.Rand
}

type Endpoint struct {
  network *Network
  id      int
}

func NewNetwork(seed int64) *Network {
  return &Network{
		handlers: make(map[int]map[string]Handler),
		connected: make(map[int]bool),
		rng: rand.New(rand.NewSource(seed)),
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
		id: id,
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

// The handler must run without network.mu held, so Call only holds the lock inside lookup.
func (e *Endpoint) Call(peer int, rpcName string, args any, reply any) bool {
  handler, ok := e.lookup(peer, rpcName)
  if !ok {
    return false
  }
  handler(args, reply)
  return true
}

func (e *Endpoint) lookup(peer int, rpcName string) (Handler, bool) {
  e.network.mu.Lock()
  defer e.network.mu.Unlock()

  if !e.network.connected[e.id] || !e.network.connected[peer] {
    return nil, false
  }
  handler, ok := e.network.handlers[peer][rpcName]
  return handler, ok
}

var _ Transport = (*Endpoint)(nil)