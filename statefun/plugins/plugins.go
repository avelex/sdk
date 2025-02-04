// Copyright 2023 NJWS Inc.

// Foliage statefun plugins package.
// Provides unified interfaces for stateful functions plugins
package plugins

import (
	"fmt"
	"sync"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/statefun/cache"
)

type StatefunAddress struct {
	Typename string
	ID       string
}

type StatefunContextProcessor struct {
	GlobalCache        *cache.Store
	GetFunctionContext func() *easyjson.JSON
	SetFunctionContext func(*easyjson.JSON)
	GetObjectContext   func() *easyjson.JSON
	SetObjectContext   func(*easyjson.JSON)
	Call               func(string, string, *easyjson.JSON, *easyjson.JSON)
	// TODO: DownstreamCall(<function type>, <links filters>, <payload>, <options>)
	GolangCallSync func(string, string, *easyjson.JSON, *easyjson.JSON) (*easyjson.JSON, error)
	Egress         func(string, *easyjson.JSON)
	Self           StatefunAddress
	Caller         StatefunAddress
	Payload        *easyjson.JSON
	Options        *easyjson.JSON
}

type StatefunExecutor interface {
	Run(contextProcessor *StatefunContextProcessor) error
	BuildError() error
}

type StatefunExecutorConstructor func(alias string, source string) StatefunExecutor

type TypenameExecutorPlugin struct {
	alias                      string
	source                     string
	idExecutors                sync.Map
	executorContructorFunction StatefunExecutorConstructor
}

func NewTypenameExecutor(alias string, source string, executorContructorFunction StatefunExecutorConstructor) *TypenameExecutorPlugin {
	tnex := TypenameExecutorPlugin{alias: alias, source: source, executorContructorFunction: executorContructorFunction}
	return &tnex
}

func (tnex *TypenameExecutorPlugin) AddForID(id string) {
	if tnex.executorContructorFunction == nil {
		fmt.Printf("Cannot create new StatefunExecutor for id=%s: missing newExecutor function\n", id)
		tnex.idExecutors.Store(id, nil)
	} else {
		fmt.Printf("______________ Created StatefunExecutor for id=%s\n", id)
		executor := tnex.executorContructorFunction(tnex.alias, tnex.source)
		tnex.idExecutors.Store(id, executor)
	}
}

func (tnex *TypenameExecutorPlugin) RemoveForID(id string) {
	tnex.idExecutors.Delete(id)
}

func (tnex *TypenameExecutorPlugin) GetForID(id string) StatefunExecutor {
	value, _ := tnex.idExecutors.Load(id)
	return value.(StatefunExecutor)
}
