package transport

type Transport interface {
  Call(peer int, rpcName string, args any, reply any) bool
}
