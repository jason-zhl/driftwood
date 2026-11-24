package transport

import (
  "sync"
  "sync/atomic"
  "testing"
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
    }
  }()

  var callers sync.WaitGroup
  for from := 0; from < nodes; from++ {
    callers.Add(1)
    go func(from int) {
      defer callers.Done()
      for i := 0; i < 200; i++ {
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
