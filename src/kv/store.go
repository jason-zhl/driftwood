package kv

import (
	"fmt"
	"sync"
)

type Entry struct {
	Value string
}

type Result struct {
	Value string
	Found bool
}

type OpKind int

const (
	OpGet OpKind = iota
	OpPut
	OpAppend
)

type Op struct {
	Kind  OpKind
	Key   string
	Value string
}

type Store struct {
	mu   sync.Mutex
	data map[string]Entry
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]Entry),
	}
}

// Lowercase methods assume the caller holds s.mu and must never lock it.
// Uppercase methods take s.mu and must only call lowercase methods.

func (s *Store) get(key string) Result {
	entry, found := s.data[key]
	return Result{
		Value: entry.Value,
		Found: found,
	}
}

func (s *Store) put(key string, value string) {
	s.data[key] = Entry{
		Value: value,
	}
}

func (s *Store) append(key string, value string) Result {
	entry := s.data[key]
	entry.Value += value
	s.data[key] = entry
	return Result{
		Value: entry.Value,
		Found: true,
	}
}

func (s *Store) Get(key string) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.get(key)
}

func (s *Store) Put(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.put(key, value)
}

func (s *Store) Append(key string, value string) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.append(key, value)
}

func (s *Store) Apply(op Op) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch op.Kind {
	case OpGet:
		return s.get(op.Key)
	case OpPut:
		s.put(op.Key, op.Value)
		return Result{
			Value: op.Value,
			Found: true,
		}
	case OpAppend:
		return s.append(op.Key, op.Value)
	default:
		panic(fmt.Sprintf("unknown op kind %d", op.Kind))
	}
}
