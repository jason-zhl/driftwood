package kv

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func mustGet(t *testing.T, s *Store, key string) string {
	t.Helper()
	result := s.Get(key)
	if !result.Found {
		t.Fatalf("key %q not found", key)
	}
	return result.Value
}

func TestGetOnEmpty(t *testing.T) {
	s := NewStore()

	result := s.Get("K")
	if result.Found {
		t.Errorf("got %v, want not found", result)
	}
}

func TestPutThenGet(t *testing.T) {
	s := NewStore()
	s.Put("k", "v")

	if got := mustGet(t, s, "k"); got != "v" {
		t.Errorf("got %q, want %q", got, "v")
	}
}

func TestPutOverwrites(t *testing.T) {
	s := NewStore()
	s.Put("k", "first")
	s.Put("k", "second")

	if got := mustGet(t, s, "k"); got != "second" {
		t.Errorf("got %q, want %q", got, "second")
	}
}

func TestPutEmptyValueIsFound(t *testing.T) {
	s := NewStore()
	s.Put("k", "")

	result := s.Get("k")
	if !result.Found {
		t.Fatalf("empty value should still be found")
	}
	if result.Value != "" {
		t.Errorf("got %q, want empty string", result.Value)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	s := NewStore()
	s.Put("a", "1")
	s.Put("b", "2")
	s.Append("a", "x")

	if got := mustGet(t, s, "a"); got != "1x" {
		t.Errorf("a: got %q, want %q", got, "1x")
	}
	if got := mustGet(t, s, "b"); got != "2" {
		t.Errorf("b: got %q, want %q", got, "2")
	}
	if result := s.Get("c"); result.Found {
		t.Errorf("c: got %v, want not found", result)
	}
}

func TestAppend(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		hasKey  bool
		add     string
		want    string
	}{
		{"missing key", "", false, "x", "x"},
		{"existing key", "ab", true, "c", "abc"},
		{"append empty to existing", "ab", true, "", "ab"},
		{"append empty to missing", "", false, "", ""},
		{"existing empty value", "", true, "x", "x"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStore()
			if tc.hasKey {
				s.Put("k", tc.initial)
			}

			returned := s.Append("k", tc.add)
			if !returned.Found || returned.Value != tc.want {
				t.Errorf("Append returned %v, want {%q true}", returned, tc.want)
			}
			if got := mustGet(t, s, "k"); got != tc.want {
				t.Errorf("stored %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAppendRepeated(t *testing.T) {
	s := NewStore()
	for _, part := range []string{"a", "b", "c"} {
		s.Append("k", part)
	}

	if got := mustGet(t, s, "k"); got != "abc" {
		t.Errorf("got %q, want %q", got, "abc")
	}
}

func TestPutAfterAppend(t *testing.T) {
	s := NewStore()
	s.Append("k", "abc")
	s.Put("k", "new")

	if got := mustGet(t, s, "k"); got != "new" {
		t.Errorf("got %q, want %q", got, "new")
	}
}

func TestConcurrentAppend(t *testing.T) {
	const n = 100
	s := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Append("k", "x")
		}()
	}
	wg.Wait()

	if got := mustGet(t, s, "k"); got != strings.Repeat("x", n) {
		t.Errorf("got %d appends, want %d", len(got), n)
	}
}

func TestConcurrentMixedOps(t *testing.T) {
	const workers = 10
	const opsPerWorker = 100
	s := NewStore()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			own := fmt.Sprintf("worker-%d", w)
			for i := 0; i < opsPerWorker; i++ {
				s.Put(own, fmt.Sprint(i))
				s.Append("shared", "x")
				s.Get("shared")
				s.Get(own)
			}
		}(w)
	}
	wg.Wait()

	for w := 0; w < workers; w++ {
		key := fmt.Sprintf("worker-%d", w)
		want := fmt.Sprint(opsPerWorker - 1)
		if got := mustGet(t, s, key); got != want {
			t.Errorf("%s: got %q, want %q", key, got, want)
		}
	}
	if got := mustGet(t, s, "shared"); len(got) != workers*opsPerWorker {
		t.Errorf("shared: got %d appends, want %d", len(got), workers*opsPerWorker)
	}
}

func TestApply(t *testing.T) {
	tests := []struct {
		name string
		op   Op
		want Result
	}{
		{"get missing", Op{Kind: OpGet, Key: "k"}, Result{}},
		{"put", Op{Kind: OpPut, Key: "k", Value: "a"}, Result{Value: "a", Found: true}},
		{"get after put", Op{Kind: OpGet, Key: "k"}, Result{Value: "a", Found: true}},
		{"append", Op{Kind: OpAppend, Key: "k", Value: "b"}, Result{Value: "ab", Found: true}},
		{"append missing", Op{Kind: OpAppend, Key: "j", Value: "x"}, Result{Value: "x", Found: true}},
		{"put overwrites", Op{Kind: OpPut, Key: "k", Value: "z"}, Result{Value: "z", Found: true}},
		{"get after overwrite", Op{Kind: OpGet, Key: "k"}, Result{Value: "z", Found: true}},
	}

	s := NewStore()
	for _, tc := range tests {
		if got := s.Apply(tc.op); got != tc.want {
			t.Errorf("%s: Apply(%+v) = %+v, want %+v", tc.name, tc.op, got, tc.want)
		}
	}
}

func TestApplyIsDeterministic(t *testing.T) {
	ops := []Op{
		{Kind: OpPut, Key: "a", Value: "1"},
		{Kind: OpAppend, Key: "a", Value: "2"},
		{Kind: OpAppend, Key: "b", Value: "x"},
		{Kind: OpGet, Key: "a"},
		{Kind: OpPut, Key: "b", Value: "y"},
		{Kind: OpAppend, Key: "c", Value: "z"},
	}

	s1 := NewStore()
	s2 := NewStore()
	for i, op := range ops {
		r1 := s1.Apply(op)
		r2 := s2.Apply(op)
		if r1 != r2 {
			t.Fatalf("op %d (%+v): results differ: %+v vs %+v", i, op, r1, r2)
		}
	}

	for _, key := range []string{"a", "b", "c", "missing"} {
		if r1, r2 := s1.Get(key), s2.Get(key); r1 != r2 {
			t.Errorf("key %q: stores differ: %+v vs %+v", key, r1, r2)
		}
	}
}

func TestApplyUnknownKindPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic for unknown op kind")
		}
	}()

	NewStore().Apply(Op{Kind: OpKind(99), Key: "k"})
}

func TestConcurrentApply(t *testing.T) {
	const n = 100
	s := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Apply(Op{Kind: OpAppend, Key: "k", Value: "x"})
			s.Apply(Op{Kind: OpGet, Key: "k"})
		}()
	}
	wg.Wait()

	if got := mustGet(t, s, "k"); got != strings.Repeat("x", n) {
		t.Errorf("got %d appends, want %d", len(got), n)
	}
}
