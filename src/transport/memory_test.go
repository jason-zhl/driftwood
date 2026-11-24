package transport

import (
  "sync"
  "sync/atomic"
  "testing"
  "time"
)

type echoArgs struct {
  Msg string
}

type echoReply struct {
  Msg  string
  From int
}

func echoHandler(id int) Handler {
  return func(args any, reply any) {
    a := args.(*echoArgs)
    r := reply.(*echoReply)
    r.Msg = a.Msg
    r.From = id
  }
}

// newCluster builds a network with nodes 0..n-1, each connected and serving "Echo".
func newCluster(t *testing.T, n int) (*Network, []*Endpoint) {
  t.Helper()
  net := NewNetwork(1)
  ends := make([]*Endpoint, n)
  for i := 0; i < n; i++ {
    net.Register(i, "Echo", echoHandler(i))
    net.Connect(i)
    ends[i] = net.Endpoint(i)
  }
  return net, ends
}

func callEcho(e *Endpoint, peer int, msg string) (echoReply, bool) {
  var reply echoReply
  ok := e.Call(peer, "Echo", &echoArgs{Msg: msg}, &reply)
  return reply, ok
}

func TestCallDeliversToPeer(t *testing.T) {
  _, ends := newCluster(t, 3)

  reply, ok := callEcho(ends[0], 2, "hello")
  if !ok {
    t.Fatalf("call failed, want success")
  }
  if reply.Msg != "hello" || reply.From != 2 {
    t.Errorf("got %+v, want {Msg:hello From:2}", reply)
  }
}

func TestCallToSelf(t *testing.T) {
  _, ends := newCluster(t, 1)

  reply, ok := callEcho(ends[0], 0, "me")
  if !ok || reply.From != 0 {
    t.Errorf("got %+v ok=%v, want reply from 0", reply, ok)
  }
}

func TestCallFailsWhenPeerDisconnected(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Disconnect(1)

  reply, ok := callEcho(ends[0], 1, "hello")
  if ok {
    t.Errorf("call succeeded, want failure")
  }
  if reply != (echoReply{}) {
    t.Errorf("reply was modified: %+v", reply)
  }
}

func TestCallFailsWhenCallerDisconnected(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Disconnect(0)

  if _, ok := callEcho(ends[0], 1, "hello"); ok {
    t.Errorf("call succeeded, want failure")
  }
}

func TestDisconnectedPeerHandlerNotRun(t *testing.T) {
  net := NewNetwork(1)
  var runs atomic.Int32
  net.Register(1, "Ping", func(args any, reply any) { runs.Add(1) })
  net.Connect(0)

  net.Endpoint(0).Call(1, "Ping", nil, nil)
  if got := runs.Load(); got != 0 {
    t.Errorf("handler ran %d times, want 0", got)
  }
}

func TestReconnectRestoresCalls(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Disconnect(1)
  if _, ok := callEcho(ends[0], 1, "a"); ok {
    t.Fatalf("call succeeded while disconnected")
  }

  net.Connect(1)
  if _, ok := callEcho(ends[0], 1, "b"); !ok {
    t.Errorf("call failed after reconnect")
  }
}

func TestNodesStartDisconnected(t *testing.T) {
  net := NewNetwork(1)
  net.Register(1, "Echo", echoHandler(1))

  if _, ok := callEcho(net.Endpoint(0), 1, "hello"); ok {
    t.Errorf("call succeeded before Connect, want failure")
  }
}

func TestCallUnknownRPC(t *testing.T) {
  _, ends := newCluster(t, 2)

  var reply echoReply
  if ends[0].Call(1, "Missing", &echoArgs{}, &reply) {
    t.Errorf("call to unregistered rpc succeeded")
  }
}

func TestCallUnknownPeer(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Connect(5)

  if _, ok := callEcho(ends[0], 5, "hello"); ok {
    t.Errorf("call to peer with no handlers succeeded")
  }
}

func TestRegisterMultipleRPCs(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Register(1, "Upper", func(args any, reply any) {
    reply.(*echoReply).Msg = "UPPER"
  })

  if reply, ok := callEcho(ends[0], 1, "x"); !ok || reply.Msg != "x" {
    t.Errorf("Echo: got %+v ok=%v", reply, ok)
  }
  var reply echoReply
  if ok := ends[0].Call(1, "Upper", &echoArgs{}, &reply); !ok || reply.Msg != "UPPER" {
    t.Errorf("Upper: got %+v ok=%v", reply, ok)
  }
}

func TestRegisterReplacesHandler(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Register(1, "Echo", func(args any, reply any) {
    reply.(*echoReply).Msg = "replaced"
  })

  if reply, _ := callEcho(ends[0], 1, "x"); reply.Msg != "replaced" {
    t.Errorf("got %q, want %q", reply.Msg, "replaced")
  }
}

// If Call held the network lock while running the handler, this would deadlock.
func TestHandlerCanMakeNestedCall(t *testing.T) {
  net, ends := newCluster(t, 3)
  net.Register(1, "Forward", func(args any, reply any) {
    forwarded, _ := callEcho(ends[1], 2, args.(*echoArgs).Msg)
    *reply.(*echoReply) = forwarded
  })

  var reply echoReply
  ok := ends[0].Call(1, "Forward", &echoArgs{Msg: "hop"}, &reply)
  if !ok || reply.Msg != "hop" || reply.From != 2 {
    t.Errorf("got %+v ok=%v, want {Msg:hop From:2}", reply, ok)
  }
}

func TestConcurrentCallsAndConnectionChanges(t *testing.T) {
  const nodes = 5
  net, ends := newCluster(t, nodes)

  var wg sync.WaitGroup
  stop := make(chan struct{})

  wg.Add(1)
  go func() {
    defer wg.Done()
    for i := 0; ; i++ {
      select {
      case <-stop:
        return
      default:
      }
      id := i % nodes
      net.Disconnect(id)
      net.Connect(id)
      net.SetReliable(i%2 == 0)
      net.SetDelay(0, time.Duration(i%3)*time.Millisecond)
      if i%4 == 0 {
        net.Partition([]int{id}, []int{(id + 1) % nodes})
      } else {
        net.Heal()
      }
    }
  }()

  var callers sync.WaitGroup
  for from := 0; from < nodes; from++ {
    callers.Add(1)
    go func(from int) {
      defer callers.Done()
      for i := 0; i < 100; i++ {
        to := (from + i) % nodes
        reply, ok := callEcho(ends[from], to, "m")
        if ok && reply.From != to {
          t.Errorf("reply from %d, want %d", reply.From, to)
        }
      }
    }(from)
  }

  callers.Wait()
  close(stop)
  wg.Wait()
}

func TestReliableNetworkNeverDrops(t *testing.T) {
  _, ends := newCluster(t, 2)

  for i := 0; i < 500; i++ {
    if _, ok := callEcho(ends[0], 1, "m"); !ok {
      t.Fatalf("call %d failed on a reliable network", i)
    }
  }
}

func TestUnreliableNetworkDropsRequestsAndReplies(t *testing.T) {
  const calls = 300
  net := NewNetwork(42)
  var handled atomic.Int32
  net.Register(1, "Ping", func(args any, reply any) { handled.Add(1) })
  net.Connect(0)
  net.Connect(1)
  net.SetReliable(false)
  e := net.Endpoint(0)

  var succeeded atomic.Int32
  var wg sync.WaitGroup
  for i := 0; i < calls; i++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      if e.Call(1, "Ping", nil, nil) {
        succeeded.Add(1)
      }
    }()
  }
  wg.Wait()

  s, h := int(succeeded.Load()), int(handled.Load())
  if s == calls {
    t.Fatalf("no calls failed on an unreliable network")
  }
  if s < calls/2 {
    t.Errorf("only %d/%d calls succeeded, drop rate looks too high", s, calls)
  }
  if h == calls {
    t.Errorf("every request reached the handler, want some requests dropped")
  }
  if h == s {
    t.Errorf("handler ran exactly as often as calls succeeded, want some replies dropped")
  }
}

func TestSetReliableRestoresDelivery(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.SetReliable(false)
  net.SetReliable(true)

  for i := 0; i < 200; i++ {
    if _, ok := callEcho(ends[0], 1, "m"); !ok {
      t.Fatalf("call %d failed after SetReliable(true)", i)
    }
  }
}

func TestDelayIsApplied(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.SetDelay(20*time.Millisecond, 30*time.Millisecond)

  start := time.Now()
  if _, ok := callEcho(ends[0], 1, "m"); !ok {
    t.Fatalf("call failed")
  }
  if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
    t.Errorf("call took %v, want at least 20ms", elapsed)
  }
}

func TestDelayReordersMessages(t *testing.T) {
  net := NewNetwork(7)
  var mu sync.Mutex
  var order []int
  net.Register(1, "Record", func(args any, reply any) {
    mu.Lock()
    order = append(order, args.(int))
    mu.Unlock()
  })
  net.Connect(0)
  net.Connect(1)
  net.SetDelay(0, 20*time.Millisecond)
  e := net.Endpoint(0)

  const msgs = 50
  var wg sync.WaitGroup
  for i := 0; i < msgs; i++ {
    wg.Add(1)
    go func(i int) {
      defer wg.Done()
      e.Call(1, "Record", i, nil)
    }(i)
    time.Sleep(100 * time.Microsecond)
  }
  wg.Wait()

  for i := 1; i < len(order); i++ {
    if order[i] < order[i-1] {
      return
    }
  }
  t.Errorf("all %d messages arrived in send order, want some reordering", msgs)
}

func TestInvalidDelayPanics(t *testing.T) {
  defer func() {
    if recover() == nil {
      t.Errorf("SetDelay(30ms, 10ms) did not panic")
    }
  }()
  NewNetwork(1).SetDelay(30*time.Millisecond, 10*time.Millisecond)
}

func TestPartitionBlocksAcrossGroups(t *testing.T) {
  net, ends := newCluster(t, 5)
  net.Partition([]int{0, 1}, []int{2, 3, 4})

  cases := []struct {
    from, to int
    want     bool
  }{
    {0, 1, true},
    {1, 0, true},
    {2, 4, true},
    {0, 2, false},
    {4, 1, false},
  }
  for _, c := range cases {
    if _, ok := callEcho(ends[c.from], c.to, "m"); ok != c.want {
      t.Errorf("call %d -> %d: got ok=%v, want %v", c.from, c.to, ok, c.want)
    }
  }
}

func TestPartitionUnlistedNodesShareAGroup(t *testing.T) {
  net, ends := newCluster(t, 4)
  net.Partition([]int{0, 1})

  if _, ok := callEcho(ends[2], 3, "m"); !ok {
    t.Errorf("unlisted nodes 2 and 3 could not reach each other")
  }
  if _, ok := callEcho(ends[2], 0, "m"); ok {
    t.Errorf("unlisted node 2 reached partitioned node 0")
  }
}

func TestPartitionReplacesPrevious(t *testing.T) {
  net, ends := newCluster(t, 3)
  net.Partition([]int{0}, []int{1, 2})
  net.Partition([]int{0, 1}, []int{2})

  if _, ok := callEcho(ends[0], 1, "m"); !ok {
    t.Errorf("0 -> 1 failed, want the new partition to allow it")
  }
  if _, ok := callEcho(ends[1], 2, "m"); ok {
    t.Errorf("1 -> 2 succeeded, want the new partition to block it")
  }
}

func TestHealRestoresAllLinks(t *testing.T) {
  net, ends := newCluster(t, 3)
  net.Partition([]int{0}, []int{1}, []int{2})
  net.Heal()

  for from := 0; from < 3; from++ {
    for to := 0; to < 3; to++ {
      if _, ok := callEcho(ends[from], to, "m"); !ok {
        t.Errorf("call %d -> %d failed after Heal", from, to)
      }
    }
  }
}

func TestPartitionAndDisconnectCombine(t *testing.T) {
  net, ends := newCluster(t, 3)
  net.Partition([]int{0, 1}, []int{2})
  net.Disconnect(1)

  if _, ok := callEcho(ends[0], 1, "m"); ok {
    t.Errorf("reached disconnected node 1 inside its own partition")
  }
  net.Heal()
  if _, ok := callEcho(ends[0], 1, "m"); ok {
    t.Errorf("Heal reconnected node 1, want it to stay disconnected")
  }
}

func TestDisconnectDuringHandlerLosesReply(t *testing.T) {
  net := NewNetwork(1)
  var ran atomic.Bool
  net.Register(1, "Crash", func(args any, reply any) {
    ran.Store(true)
    net.Disconnect(1)
  })
  net.Connect(0)
  net.Connect(1)

  if net.Endpoint(0).Call(1, "Crash", nil, nil) {
    t.Errorf("call succeeded, want the reply lost after the peer disconnected")
  }
  if !ran.Load() {
    t.Errorf("handler did not run")
  }
}

func TestFailedCallWaitsBeforeReturning(t *testing.T) {
  net, ends := newCluster(t, 2)
  net.Disconnect(1)

  start := time.Now()
  for i := 0; i < 10; i++ {
    callEcho(ends[0], 1, "m")
  }
  if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
    t.Errorf("10 failed calls took %v, want failures to wait like a timeout", elapsed)
  }
}
