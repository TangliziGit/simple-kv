package protos

import (
	"fmt"
	"net"
	"simple-kv/pkg/engines"
)

type Server struct {
	Hostname string
	Port     string
	Listener net.Listener
	Engine   *engines.StringEngine
}

func NewServer(hostname string, port string) *Server {
	return &Server{
		Hostname: hostname,
		Port:     port,
		Listener: nil,
		Engine:   engines.NewStringEngine(),
	}
}

func (s *Server) Run() (err error) {
	addr := fmt.Sprintf("%s:%s", s.Hostname, s.Port)
	s.Listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return err
		}

		handler := NewHandler(s.Engine)
		go handler.Handle(conn)
	}
}

func (s *Server) Close() error {
	return s.Listener.Close()
}
