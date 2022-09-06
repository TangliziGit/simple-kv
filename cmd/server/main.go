package main

import (
	"github.com/jessevdk/go-flags"
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

	server := protos.NewServer(opts.Host, opts.Port)
	defer server.Close()

	err = server.Run()
	if err != nil {
		panic(err)
	}
}
