package logger

import "go.uber.org/zap"

var Inst *zap.SugaredLogger

func init() {
	var err error
	l, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	Inst = l.Sugar()
}
