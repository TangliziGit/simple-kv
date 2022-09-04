package protos

import "simple-kv/pkg/txns"

type Session struct {
	txn *txns.Txn
}

func NewSession() *Session {
	return &Session{}
}

func (s *Session) GetTxn() *txns.Txn {
	return s.txn
}

func (s *Session) SetTxn(txn *txns.Txn) {
	s.txn = txn
}
