package statefun

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/statefun"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const _MAIN_DISPATCHER = "main_dispatcher"

const (
	dispatcherInit   = "functions.dispatcher.init"
	dispatcherBegin  = "functions.dispatcher.begin"
	dispatcherAdd    = "functions.dispatcher.add"
	dispatcherCommit = "functions.dispatcher.commit"
	dispatcherPush   = "functions.dispatcher.push"
)

type handler struct {
	runtime *statefun.Runtime
}

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	h := &handler{
		runtime: runtime,
	}

	statefun.NewFunctionType(runtime, dispatcherBegin, h.begin, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, dispatcherAdd, h.add, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, dispatcherCommit, h.commit, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, dispatcherPush, h.push, *statefun.NewFunctionTypeConfig())
}

/*
	adapter --> 		begin 		 	  --> main_dipatcher
 	adapter <--   slave_dispatcher_id     <-- main_dipatcher
	adapter --> 	     add 			  --> work_dispatcher
	adapter --> 	     add 			  --> work_dispatcher
	adapter --> 	     add 			  --> work_dispatcher
	adapter --> 	    commit 			  --> work_dispatcher
	work_dispatcher --> compile --> push  --> main_dipatcher
*/

// only call on "main_dispatcher"
func (h *handler) begin(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self
	if self.ID != _MAIN_DISPATCHER {
		return
	}

	obj := contextProcessor.GetObjectContext()

	source := obj.GetByPath("source").AsStringDefault("")
	nonce := int(obj.GetByPath("nonce").AsNumericDefault(0))
	nonce++
	obj.SetByPath("nonce", easyjson.NewJSON(nonce))

	contextProcessor.SetObjectContext(obj)

	hash := sha256.Sum256([]byte(self.ID + strconv.Itoa(nonce)))
	slaveDispatcherID := hex.EncodeToString(hash[:])
	slaveDispatcherSource := filepath.Join(source, slaveDispatcherID)

	body := easyjson.NewJSONObject()
	body.SetByPath("body.id", easyjson.NewJSON(slaveDispatcherID))
	body.SetByPath("body.createdAt", easyjson.NewJSON(system.GetCurrentTimeNs()))
	body.SetByPath("body.links", easyjson.NewJSONObject())
	body.SetByPath("body.types", easyjson.NewJSONObject())
	body.SetByPath("body.objects", easyjson.NewJSONObject())
	body.SetByPath("body.source", easyjson.NewJSON(slaveDispatcherSource))

	_, err := contextProcessor.GolangCallSync("functions.graph.ll.api.object.create", slaveDispatcherID, &body, nil)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	// TODO: add link main_dispatcher -> work_dispatcher

	if err := os.MkdirAll(slaveDispatcherSource, os.ModeDir|0700); err != nil {
		log.Println(err)
	}

	qid := common.GetQueryID(contextProcessor)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("payload.id", easyjson.NewJSON(slaveDispatcherID))

	common.ReplyQueryID(qid, &payload, contextProcessor)
}

/*
	payload: {
		links:[
			{
				"from",
				"to",
				"linkType"
			}
		],
		types:[
			{
				"id",
				"body"
			}
		],
		objects:[
			{
				"id",
				"originType",
				"body"
			}
		],
	}

add save payload to temp file or context object
*/
func (h *handler) add(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self
	payload := contextProcessor.Payload

	revID, err := statefun.KeyMutexLock(h.runtime, self.ID, false)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	defer statefun.KeyMutexUnlock(h.runtime, self.ID, revID)

	obj := contextProcessor.GetObjectContext()

	source := obj.GetByPath("source").AsStringDefault("")
	currentLinks, _ := obj.GetByPath("links").AsObject()
	currentObjects, _ := obj.GetByPath("objects").AsObject()
	currentTypes, _ := obj.GetByPath("types").AsObject()

	cmp, err := newCompiler(source, currentLinks, currentTypes, currentObjects)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	if payload.PathExists("links") {
		links, _ := payload.GetByPath("links").AsArray()
		for _, link := range links {
			var l _link
			if err := json.Unmarshal(easyjson.NewJSON(link).ToBytes(), &l); err != nil {
				slog.Error(err.Error())
				continue
			}

			if l.Type == "" {
				cmp.createLinkObjects(l.From, l.To)
			} else {
				cmp.createLinkTypes(l.From, l.To, l.Type)
			}
		}
	}

	if payload.PathExists("types") {
		types, _ := payload.GetByPath("types").AsArray()
		for _, ttype := range types {
			jsonType := easyjson.NewJSON(ttype)
			id := jsonType.GetByPath("id").AsStringDefault("")
			body := jsonType.GetByPath("body")

			if err := cmp.createType(id, &body); err != nil {
				slog.Error(err.Error())
				continue
			}
		}
	}

	if payload.PathExists("objects") {
		objects, _ := payload.GetByPath("objects").AsArray()
		for _, object := range objects {
			jsonObj := easyjson.NewJSON(object)

			id := jsonObj.GetByPath("id").AsStringDefault("")
			ot := jsonObj.GetByPath("originType").AsStringDefault("")
			body := jsonObj.GetByPath("body")

			if err := cmp.createObject(id, ot, &body); err != nil {
				slog.Error(err.Error())
				continue
			}
		}
	}

	obj.SetByPath("links", easyjson.NewJSON(cmp.links))
	obj.SetByPath("types", easyjson.NewJSON(cmp.types))
	obj.SetByPath("objects", easyjson.NewJSON(cmp.objects))
	contextProcessor.SetObjectContext(obj)

	qid := common.GetQueryID(contextProcessor)
	reply := easyjson.NewJSONObject()
	reply.SetByPath("payload.status", easyjson.NewJSON("ok"))

	common.ReplyQueryID(qid, &reply, contextProcessor)
}

/*
1. compile

2. push to "main_dispatcher"
*/
func (h *handler) commit(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self
	revID, err := statefun.KeyMutexLock(h.runtime, self.ID, false)
	if err != nil {
		return
	}

	defer statefun.KeyMutexUnlock(h.runtime, self.ID, revID)

	obj := contextProcessor.GetObjectContext()
	source := obj.GetByPath("source").AsStringDefault("")
	currentLinks, _ := obj.GetByPath("links").AsObject()
	currentObjects, _ := obj.GetByPath("objects").AsObject()
	currentTypes, _ := obj.GetByPath("types").AsObject()

	cmp, err := newCompiler(source, currentLinks, currentTypes, currentObjects)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	qid := common.GetQueryID(contextProcessor)

	if err1 := cmp.compile(); err1 != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("payload.status", easyjson.NewJSON("failed"))
		reply.SetByPath("payload.result", easyjson.NewJSON(fmt.Errorf("compile: %w", err1)))
		common.ReplyQueryID(qid, &reply, contextProcessor)
		return
	}

	pushPayload := easyjson.NewJSONObject()
	pushPayload.SetByPath("links", easyjson.NewJSON(cmp.links))
	pushPayload.SetByPath("types", easyjson.NewJSON(cmp.types))
	pushPayload.SetByPath("objects", easyjson.NewJSON(cmp.objects))

	contextProcessor.Call(dispatcherPush, _MAIN_DISPATCHER, &pushPayload, nil)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("payload.status", easyjson.NewJSON("ok"))
	reply.SetByPath("payload.result", easyjson.NewJSON(""))
	common.ReplyQueryID(qid, &reply, contextProcessor)
}

func (h *handler) push(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self

	if self.ID != _MAIN_DISPATCHER {
		return
	}

	payload := contextProcessor.Payload

	currentLinks := make(map[string]*_link)
	currentTypes := make(map[string]*_type)
	currentObjects := make(map[string]*_object)

	if types, ok := payload.GetByPath("types").AsObject(); ok {
		for k, v := range types {
			currentTypes[k] = typeFromJSON(v)
		}
	}

	if objects, ok := payload.GetByPath("objects").AsObject(); ok {
		for k, v := range objects {
			currentObjects[k] = objectFromJSON(v)
		}
	}

	if links, ok := payload.GetByPath("links").AsObject(); ok {
		for k, v := range links {
			currentLinks[k] = linkFromJSON(v)
		}
	}

	for id, t := range currentTypes {
		body := easyjson.NewJSONObject()

		if bytes, err := os.ReadFile(t.Path); err == nil {
			body, _ = easyjson.JSONFromBytes(bytes)
		}

		if err := h.createFoliageObject(id, &body); err != nil {
			slog.Warn("Cannot create type", "id", id)
		}
	}

	for id, obj := range currentObjects {
		body := easyjson.NewJSONObject()

		if bytes, err := os.ReadFile(obj.Path); err == nil {
			body, _ = easyjson.JSONFromBytes(bytes)
		}

		if err := h.createFoliageObject(id, &body); err != nil {
			slog.Warn("Cannot create object", "id", id)
		}
	}

	for _, link := range currentLinks {
		body := easyjson.NewJSONObject()
		if link.ObjectType != "" {
			body.SetByPath("link_type", easyjson.NewJSON(link.ObjectType))
		}

		if err := h.createFoliageLink(link.From, link.To, link.Type, &body); err != nil {
			slog.Warn("Cannot create link", "id", link.From+"/"+link.To, "error", err)
		}
	}

	slog.Info("Done!")
}
