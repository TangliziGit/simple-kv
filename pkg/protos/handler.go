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
			resp = NewErrorCommand(err)
		} else {
			resp, err = h.Execute(req)
			if err != nil {
				// TODO: log
				resp = NewErrorCommand(err)
			}
		}

		err = resp.Send(conn)
		if err != nil {
			// TODO: log
		}
	}
}

func (h *Handler) Execute(req *Command) (resp *Command, err error) {
	isLocalTxn := h.session.GetTxn() == nil && req.Type != Begin
	if isLocalTxn {
		h.session.SetTxn(h.engine.NewTxn())
	}

	resp = &Command{}
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
		resp.Type = None
	case Commit:
		// TODO: error
		h.session.GetTxn().Commit()
		h.session.SetTxn(nil)
		resp.Type = None
	case Abort:
		h.session.GetTxn().Abort()
		h.session.SetTxn(nil)
		resp.Type = None
	default:
		err = fmt.Errorf("invalid command type: type=%v", req.Type)
	}

	if isLocalTxn {
		h.session.GetTxn().Commit()
		h.session.SetTxn(nil)
	}
	return
}
