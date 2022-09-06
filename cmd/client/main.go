package main

import (
	"bufio"
	"fmt"
	"github.com/jessevdk/go-flags"
	"net"
	"os"
	"simple-kv/pkg/parsers"
	"simple-kv/pkg/protos"
)

var opts struct {
	Host string `value-name:"host" short:"h" long:"host" default:"localhost" description:"simple-kv server host"`
	Port string `value-name:"port" short:"p" long:"port" default:"8081" description:"simple-kv server port"`
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		if flags.WroteHelp(err) {
			return
		} else {
			panic(err)
		}
	}

	err = Interact(opts.Host, opts.Port)
	if err != nil {
		fmt.Println(err.Error())
	}
}

const ErrorSymbol = "<ERROR>"

func Interact(hostname string, port string) error {
	addr := fmt.Sprintf("%s:%s", hostname, port)
	dial, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(os.Stdin)
	parser := parsers.NewParser()
	for {
		fmt.Printf("[%s]> ", addr)
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("%s %s\n", ErrorSymbol, err.Error())
			continue
		}

		req, err := parser.Parse(line)
		if err != nil {
			fmt.Printf("%s %s\n", ErrorSymbol, err.Error())
			continue
		}

		err = req.Send(dial)
		if err != nil {
			fmt.Printf("%s fail to send command: req=%v, err=%v\n", ErrorSymbol, req, err)
			continue
		}

		resp, err := protos.ParseCommand(dial)
		if err != nil {
			fmt.Printf("%s fail to parse command: req=%v, resp=%v, err=%v\n", ErrorSymbol, req, resp, err)
			continue
		}

		showResponse(resp)
	}
}

func showResponse(resp *protos.Command) {
	switch resp.Type {
	case protos.None:
		break
	case protos.Error:
		fmt.Printf("%s %s\n", ErrorSymbol, resp.Payload[0])
	case protos.String:
		fmt.Printf("%s\n", resp.Payload[0])
	case protos.Strings:
		fmt.Println(resp.Payload)
	default:
		fmt.Printf("%s invalid response type: resp=%v\n", ErrorSymbol, resp)
	}
}
