package protos

import (
	"fmt"
	"net"
	"simple-kv/pkg/engines"
	"strconv"
)

type Handler struct {
	engine  *engines.StringEngine
	session *Session
}

func NewHandler(engine *engines.StringEngine) *Handler {
	return &Handler{
		engine:  engine,
		session: NewSession(),
	}
}

func (h *Handler) Handle(conn net.Conn) {
	var (
		req  *Command
		resp *Command
		err  error
	)

	for {
		req, err = ParseCommand(conn)
		if err != nil {
			// TODO: log
			// TODO: resp = command.FromError()
		} else {
			resp, err = h.Execute(req)
			if err != nil {
				// TODO: log
				// TODO: resp = command.FromError()
			}
		}

		err = resp.Send(conn)
		if err != nil {
			// TODO: log
		}
	}
}

func (h *Handler) Execute(req *Command) (resp *Command, err error) {
	if h.session.GetTxn() == nil && req.Type != Begin {
		h.session.SetTxn(h.engine.NewTxn())
		defer h.session.GetTxn().Commit()
	}

	txn := h.session.GetTxn()
	switch req.Type {
	case Get:
		resp.Type = String
		resp.Payload = make([]string, 1)
		resp.Payload[0], err = h.engine.Get(txn, req.Payload[0])

	case Put:
		resp.Type = None
		err = h.engine.Put(txn, req.Payload[0], req.Payload[1])

	case Del:
		resp.Type = None
		err = h.engine.Del(txn, req.Payload[0])

	case Scan:
		count, err := strconv.Atoi(req.Payload[1])
		if err != nil {
			break
		}

		resp.Type = Strings
		resp.Payload, err = h.engine.Scan(txn, req.Payload[0], count)

	case Begin:
		h.session.SetTxn(h.engine.NewTxn())
	case Commit:
		// TODO: error
		h.session.GetTxn().Commit()
	case Abort:
		h.session.GetTxn().Abort()
	default:
		return nil, fmt.Errorf("invalid command type: type=%v", req.Type)
	}
	return
}
