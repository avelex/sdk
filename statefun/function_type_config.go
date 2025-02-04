// Copyright 2023 NJWS Inc.

package statefun

import "github.com/foliagecp/easyjson"

const (
	MsgAckWaitTimeoutMs = 10000
	MsgChannelSize      = 64
	MsgAckChannelSize   = 64
	BalanceNeeded       = true
	MutexLifetimeSec    = 120
)

type FunctionTypeConfig struct {
	msgAckWaitMs      int
	msgChannelSize    int
	msgAckChannelSize int
	balanceNeeded     bool
	balanced          bool
	mutexLifeTimeSec  int
	options           *easyjson.JSON
}

func NewFunctionTypeConfig() *FunctionTypeConfig {
	return &FunctionTypeConfig{
		msgAckWaitMs:      MsgAckWaitTimeoutMs,
		msgChannelSize:    MsgChannelSize,
		msgAckChannelSize: MsgAckChannelSize,
		balanceNeeded:     BalanceNeeded,
		mutexLifeTimeSec:  MutexLifetimeSec,
		options:           easyjson.NewJSONObject().GetPtr(),
	}
}

func (ftc *FunctionTypeConfig) SetMsgAckWaitMs(msgAckWaitMs int) *FunctionTypeConfig {
	ftc.msgAckWaitMs = msgAckWaitMs
	return ftc
}

func (ftc *FunctionTypeConfig) SeMsgChannelSize(msgChannelSize int) *FunctionTypeConfig {
	ftc.msgChannelSize = msgChannelSize
	return ftc
}

func (ftc *FunctionTypeConfig) SetMsgAckChannelSize(msgAckChannelSize int) *FunctionTypeConfig {
	ftc.msgAckChannelSize = msgAckChannelSize
	return ftc
}

func (ftc *FunctionTypeConfig) SetBalanceNeeded(balanceNeeded bool) *FunctionTypeConfig {
	ftc.balanceNeeded = balanceNeeded
	return ftc
}

func (ftc *FunctionTypeConfig) SetMutexLifeTimeSec(mutexLifeTimeSec int) *FunctionTypeConfig {
	ftc.mutexLifeTimeSec = mutexLifeTimeSec
	return ftc
}

func (ftc *FunctionTypeConfig) SetOptions(options *easyjson.JSON) *FunctionTypeConfig {
	ftc.options = options
	return ftc
}
