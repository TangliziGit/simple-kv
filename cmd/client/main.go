package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"simple-kv/pkg/parsers"
	"simple-kv/pkg/protos"
)

func main() {
	// TODO: go-flags
	if err := Interact("localhost", "8080"); err != nil {
		panic(err)
	}
}

func Interact(hostname string, port string) error {
	addr := fmt.Sprintf("%s:%s", hostname, port)
	dial, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(os.Stdin)
	parser := parsers.NewParser()
	for {
		fmt.Printf("[%s] > ", addr)
		line, err := reader.ReadString('\n')
		if err != nil {
			// TODO: log
			continue
		}

		req, err := parser.Parse(line)
		if err != nil {
			fmt.Println(err)
			continue
		}

		err = req.Send(dial)
		if err != nil {
			fmt.Printf("fail to send command: req=%v, err=%v\n", req, err)
			continue
		}

		resp, err := protos.ParseCommand(dial)
		if err != nil {
			fmt.Printf("fail to parse command: req=%v, resp=%v, err=%v\n", req, resp, err)
			continue
		}

		fmt.Println(resp.Payload)
	}
}
