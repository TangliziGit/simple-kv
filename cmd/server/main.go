package main

import "simple-kv/pkg/protos"

func main() {
	// TODO: go-flags
	server := protos.NewServer("localhost", "8080")
	defer server.Close()

	err := server.Run()
	if err != nil {
		panic(err)
	}
}
