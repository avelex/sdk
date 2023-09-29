package dispatcher

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
)

/*

	Adapter -> [Compiler -> Dispatcher] -> Foliage

*/

/*
				linktype = type_b
			type_a -------------> type_b
			^					  |
	__type	|                     |  __object
			|                     !
			object ------------> object
			    	  obj
		 tags[type_t1, type_t2..., name_X]
*/

type Dispatcher interface {
	CreateLinkTypes(from, to, linkType string)
	CreateLinkObjects(from, to string)
	CreateType(id string, body *easyjson.JSON) error
	CreateObject(id, originType string, body *easyjson.JSON) error
	Compile() error
}

type dispatcher struct {
	objectsWorker *worker[*_object]
	typesWorker   *worker[*_type]
	linksWorker   *worker[*_link]

	source string

	m       *sync.Mutex
	types   map[string]struct{} // type_id -> path?
	objects map[string]string   // object_id -> type_id
	links   map[string]*_link

	runtime *statefun.Runtime
}

func New(r *statefun.Runtime) (*dispatcher, error) {
	sourcePath := filepath.Join(os.TempDir(), "fdc_"+strconv.Itoa(int(time.Now().Unix())))
	if err := os.MkdirAll(sourcePath, os.ModeDir|0700); err != nil {
		return nil, err
	}

	d := &dispatcher{
		source:        sourcePath,
		m:             &sync.Mutex{},
		objects:       make(map[string]string),
		types:         make(map[string]struct{}),
		links:         make(map[string]*_link),
		typesWorker:   newWorker[*_type](1),   // remove magic number
		objectsWorker: newWorker[*_object](1), // remove magic number
		linksWorker:   newWorker[*_link](1),   // remove magic number
		runtime:       r,
	}

	if err := d.initBuilInObjects(); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *dispatcher) CreateLinkTypes(from, to, linkType string) {
	d.createLinkTypes(from, to, linkType)
}

func (d *dispatcher) CreateLinkObjects(from, to string) {
	d.createLinkObjects(from, to)
}

func (d *dispatcher) CreateType(id string, body *easyjson.JSON) error {
	return d.createType(id, body)
}

func (d *dispatcher) CreateObject(id, originType string, body *easyjson.JSON) error {
	return d.createObject(id, originType, body)
}

/*
 0. maybe validate all names
 1. check that all object's types exists
 2. check links:
    2.1. if it TYPES:
    2.1.1. check "From" and "To" in `types`
    2.1.2. check that link type is not empty
    2.2. if it OBJECTS:
    2.2.1. check "From" and "To" in `objects`
    2.2.2. find link type -> find "From" type and "To" type -> find link
 3. send all types
 4. send all objects
 5. send all links
*/
func (d *dispatcher) Compile() error {
	d.m.Lock()
	defer d.m.Unlock()

	slog.Info("[X] Compiling...")
	if err := d.compile(); err != nil {
		return err
	}
	slog.Info("[X] Compiling done!")

	slog.Info("[X] Ready for processing types")
	for i := 0; i < d.typesWorker.Limit(); i++ {
		go d.processTypes()
	}

	slog.Info("[X] Start sending types...")
	d.sendTypes()
	d.typesWorker.Close()
	slog.Info("[X] Send types done!")

	slog.Info("[X] Ready for processing objects")
	for i := 0; i < d.objectsWorker.Limit(); i++ {
		go d.processObjects()
	}

	slog.Info("[X] Start sending objects...")
	d.sendObjects()
	d.objectsWorker.Close()
	slog.Info("[X] Send objects done!")

	slog.Info("[X] Ready for processing links")
	for i := 0; i < d.linksWorker.Limit(); i++ {
		go d.processLinks()
	}

	slog.Info("[X] Start sending links...")
	d.sendLinks()
	d.linksWorker.Close()
	slog.Info("[X] Send links done!")

	slog.Info("[X] Dispatcher work done!")

	return nil
}

func (d *dispatcher) compile() error {
	if err := d.validateAllIDs(); err != nil {
		return err
	}

	if err := d.checkObjectsTypeMatching(); err != nil {
		return fmt.Errorf("object type matching: %w", err)
	}

	if err := d.buildLinks(); err != nil {
		return fmt.Errorf("build links: %w", err)
	}

	return nil
}

func (d *dispatcher) createObject(id, originType string, body *easyjson.JSON) error {
	d.m.Lock()
	d.objects[id] = originType
	d.m.Unlock()

	if originType == "builtin" {
		return nil
	}

	path := filepath.Join(d.source, "obj_"+id+".json")
	if err := os.WriteFile(path, body.ToBytes(), os.ModePerm); err != nil {
		return err
	}

	// create objects -> object link
	d.createLink(_DEFINED, BUILT_IN_OBJECTS, id, OBJECT_TYPELINK, "")

	// create type -> object link
	d.createLink(_DEFINED, originType, id, OBJECT_TYPELINK, "")

	// create object -> type link
	d.createLink(_DEFINED, id, originType, TYPE_TYPELINK, "")

	return nil
}

func (d *dispatcher) createType(id string, body *easyjson.JSON) error {
	d.m.Lock()
	d.types[id] = struct{}{}
	d.m.Unlock()

	path := filepath.Join(d.source, "type_"+id+".json")
	if err := os.WriteFile(path, body.ToBytes(), os.ModePerm); err != nil {
		return err
	}

	// create types -> type link
	d.createLink(_DEFINED, BUILT_IN_TYPES, id, TYPE_TYPELINK, "")

	return nil
}

func (d *dispatcher) createLinkTypes(from, to, objectLinkType string) {
	d.createLink(_DEFINED, from, to, to, objectLinkType)
}

func (d *dispatcher) createLinkObjects(from, to string) {
	d.createLink(_UNDEFINED, from, to, OBJECT_2_OBJECT_TYPELINK, "")
}

func (d *dispatcher) createLink(mode linkMode, from, to, linkType, objectLinkType string) {
	linkID := from + "/" + to // maybe hash

	d.m.Lock()
	d.links[linkID] = &_link{
		mode:       mode,
		From:       from,
		To:         to,
		Type:       linkType,
		ObjectType: objectLinkType,
	}
	d.m.Unlock()
}

func (d *dispatcher) initBuilInObjects() error {
	d.types["builtin"] = struct{}{}

	// create root
	if err := d.createObject(BUILT_IN_ROOT, "builtin", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}

	// create objects and types
	if err := d.createObject(BUILT_IN_OBJECTS, "builtin", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}

	if err := d.createObject(BUILT_IN_TYPES, "builtin", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}

	// create root -> objects link
	d.createLink(_DEFINED, BUILT_IN_ROOT, BUILT_IN_OBJECTS, OBJECTS_TYPELINK, "")

	// create root -> types link
	d.createLink(_DEFINED, BUILT_IN_ROOT, BUILT_IN_TYPES, TYPES_TYPELINK, "")

	// create group type ----------------------------------------
	d.createType("group", easyjson.NewJSONObject().GetPtr())

	// link from group -> group, need for define "group" link type
	d.createLinkTypes("group", "group", GROUP_TYPELINK)
	//-----------------------------------------------------------

	// create NAV ------------------------------------------------
	if err := d.createObject("nav", "group", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}
	// -----------------------------------------------------------

	slog.Info("[X] Initialized Built-In objects")

	return nil
}

func (d *dispatcher) processTypes() {
	d.typesWorker.Start()

	for ttype := range d.typesWorker.Queue() {
		if ttype == nil {
			continue
		}

		if err := d.createFoliageObject(ttype.id, ttype.body); err != nil {
			slog.Error("Cannot create type", "id", ttype.id)
			continue
		}
	}

	d.typesWorker.Done()
}

func (d *dispatcher) processObjects() {
	d.objectsWorker.Start()

	for object := range d.objectsWorker.Queue() {
		if object == nil {
			continue
		}

		if err := d.createFoliageObject(object.id, object.body); err != nil {
			slog.Error("Cannot create Foliage object", "id", object.id)
			continue
		}
	}

	d.objectsWorker.Done()
}

func (d *dispatcher) processLinks() {
	d.linksWorker.Start()

	for link := range d.linksWorker.Queue() {
		if link == nil {
			continue
		}

		body := easyjson.NewJSONObject()
		if link.ObjectType != "" {
			body.SetByPath("link_type", easyjson.NewJSON(link.ObjectType))
		}

		if err := d.createFoliageLink(link.From, link.To, link.Type, &body); err != nil {
			slog.Warn("Cannot create link", "id", link.From+"/"+link.To, "error", err)
		}
	}

	d.linksWorker.Done()
}

func (d *dispatcher) sendTypes() {
	for k := range d.types {
		path := filepath.Join(d.source, "type_"+k+".json")

		body := easyjson.NewJSONObject()

		if bytes, err := os.ReadFile(path); err == nil {
			body, _ = easyjson.JSONFromBytes(bytes)
		}

		d.typesWorker.Send(&_type{
			id:   k,
			body: &body,
		})
	}
}

func (d *dispatcher) sendObjects() {
	for k := range d.objects {
		path := filepath.Join(d.source, "obj_"+k+".json")

		body := easyjson.NewJSONObject()

		if bytes, err := os.ReadFile(path); err == nil {
			body, _ = easyjson.JSONFromBytes(bytes)
		}

		d.objectsWorker.Send(&_object{
			id:   k,
			body: &body,
		})
	}
}

func (d *dispatcher) sendLinks() {
	for _, link := range d.links {
		d.linksWorker.Send(link)
	}
}

func (d *dispatcher) validateAllIDs() error {
	return nil
}

func (d *dispatcher) checkObjectsTypeMatching() error {
	for objectID, typeID := range d.objects {
		if _, ok := d.types[typeID]; !ok {
			return fmt.Errorf("object '%s' missmatching with '%s' type", objectID, typeID)
		}
	}
	return nil
}

func (d *dispatcher) buildLinks() error {
	for id, link := range d.links {
		_, fromTypeExists := d.types[link.From]
		_, fromObjectExists := d.objects[link.From]
		if !fromObjectExists && !fromTypeExists {
			return fmt.Errorf("%s exists neither in types nor in objects", link.From)
		}

		_, toTypeExists := d.types[link.To]
		_, toObjectExists := d.objects[link.To]
		if !toTypeExists && !toObjectExists {
			return fmt.Errorf("%s exists neither in types nor in objects", link.To)
		}

		if link.Type == "" {
			return fmt.Errorf("%s missed type", id)
		}

		switch link.mode {
		case _DEFINED:
		case _UNDEFINED:
			objectTypeFrom := d.objects[link.From]
			objectTypeTo := d.objects[link.To]
			linkID := objectTypeFrom + "/" + objectTypeTo

			findLink, ok := d.links[linkID]
			if !ok {
				return fmt.Errorf("can't find link type between %s", linkID)
			}

			if findLink.ObjectType == "" {
				return fmt.Errorf("%s object link type is empty", linkID)
			}

			d.links[id] = &_link{
				mode: _DEFINED,
				From: link.From,
				To:   link.To,
				Type: findLink.ObjectType,
			}
		}
	}
	return nil
}
