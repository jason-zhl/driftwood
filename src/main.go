package main

import (
	"fmt"

	"github.com/jason-zhl/driftwood/kv"
)

func main() {

	store := kv.NewStore()
	store.Put("key", "Hello, I'm a value")
	result := store.Get("key")
	fmt.Println(result.Value)
}
