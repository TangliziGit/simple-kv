package protos

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

type CommandType byte

const (
	Get CommandType = iota
	Put
	Del
	Scan
	Begin
	Commit
	Abort

	None
	String
	Strings

	Invalid
)
const CommandHeaderLength = 9

func ToCommandType(t string) CommandType {
	switch strings.ToUpper(t) {
	case "GET":
		return Get
	case "PUT":
		return Put
	case "DEL":
		return Del
	case "SCAN":
		return Scan
	case "BEGIN":
		return Begin
	case "COMMIT":
		return Commit
	case "ABORT":
		return Abort
	case "NONE":
		return None
	case "STRING":
		return String
	case "STRINGS":
		return Strings
	default:
		return Invalid
	}
}

type Command struct {
	PayloadLength uint64
	Type          CommandType
	Payload       []string
}

func NewCommand(t CommandType, payload []string) *Command {
	return &Command{
		PayloadLength: calcPayloadLength(payload),
		Type:          t,
		Payload:       payload,
	}
}

func ParseCommand(conn net.Conn) (*Command, error) {
	header := make([]byte, CommandHeaderLength)
	n, err := conn.Read(header)
	if err != nil {
		return nil, err
	}
	if n <= 8 {
		return nil, fmt.Errorf("invalid message received: n=%d", n)
	}

	command := &Command{
		PayloadLength: binary.BigEndian.Uint64(header),
		Type:          CommandType(header[8]),
	}
	if command.Type >= Invalid {
		return nil, fmt.Errorf("invalid command type: type=%v", command.Type)
	}

	payload := make([]byte, command.PayloadLength)
	n, err = conn.Read(payload)
	if err != nil {
		return nil, err
	}
	if uint64(n) != command.PayloadLength {
		return nil, fmt.Errorf("received message content size: expect=%d, got=%d", command.PayloadLength, n)
	}

	command.Payload = parsePayload(payload, command.PayloadLength)
	return command, nil
}

func calcPayloadLength(payload []string) uint64 {
	length := 0
	for _, p := range payload {
		length += 8 + len(p)
	}
	return uint64(length)
}

func parsePayload(buffer []byte, length uint64) []string {
	var res []string
	for i := uint64(0); i < length; {
		l := binary.BigEndian.Uint64(buffer[i:])
		res = append(res, string(buffer[i+8:i+8+l]))
		i += 8 + l
	}
	return res
}

func (c *Command) Serialize() []byte {
	c.PayloadLength = calcPayloadLength(c.Payload)
	buffer := make([]byte, 8+c.PayloadLength)
	binary.BigEndian.PutUint64(buffer, c.PayloadLength)
	buffer[8] = byte(c.Type)

	i := 9
	for _, payload := range c.Payload {
		binary.BigEndian.PutUint64(buffer[i:], uint64(len(payload)))
		copy(buffer[i+8:], payload)
		i += 8 + len(payload)
	}
	return buffer
}

func (c *Command) Send(conn net.Conn) error {
	// TODO: retry
	_, err := conn.Write(c.Serialize())
	return err
}
