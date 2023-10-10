package dispatcher

import (
	"errors"
	"fmt"

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
	Begin() error
	CreateLinkTypes(from, to, linkType string)
	CreateLinkObjects(from, to string)
	CreateType(id string, body *easyjson.JSON) error
	CreateObject(id, originType string, body *easyjson.JSON) error
	Commit() error
}

type dispatcher struct {
	id string
	r  *statefun.Runtime
}

func New(r *statefun.Runtime) *dispatcher {
	return &dispatcher{
		r: r,
	}
}

func (d *dispatcher) Begin() error {
	const op = "functions.dispatcher.begin"

	result, err := d.r.IngressGolangSync(op, "main_dispatcher", easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	workDispatcherID := result.GetByPath("payload.id").AsStringDefault("")
	if workDispatcherID == "" {
		return errors.New("empty work dispatcher id")
	}

	d.id = workDispatcherID

	return nil
}

func (d *dispatcher) CreateLinkTypes(from, to, linkType string) {
	d.createLink(from, to, linkType)
}

func (d *dispatcher) CreateLinkObjects(from, to string) {
	d.createLink(from, to, "")
}

func (d *dispatcher) CreateType(id string, body *easyjson.JSON) error {
	types := easyjson.NewJSONArray()
	t := easyjson.NewJSONObject()
	t.SetByPath("id", easyjson.NewJSON(id))
	t.SetByPath("body", *body)
	types.AddToArray(t)

	return d.add(easyjson.NewJSONObjectWithKeyValue("types", types).GetPtr())
}

func (d *dispatcher) CreateObject(id, originType string, body *easyjson.JSON) error {
	objects := easyjson.NewJSONArray()
	obj := easyjson.NewJSONObject()
	obj.SetByPath("id", easyjson.NewJSON(id))
	obj.SetByPath("body", *body)
	obj.SetByPath("originType", easyjson.NewJSON(originType))
	objects.AddToArray(obj)

	return d.add(easyjson.NewJSONObjectWithKeyValue("objects", objects).GetPtr())
}

func (d *dispatcher) Commit() error {
	const op = "functions.dispatcher.commit"

	result, err := d.r.IngressGolangSync(op, d.id, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if result.GetByPath("payload.status").AsStringDefault("failed") == "failed" {
		fmt.Printf("result.ToString(): %v\n", result.ToString())
		return fmt.Errorf("%s: %s", op, result.GetByPath("payload.result").AsStringDefault(""))
	}

	return nil
}

func (d *dispatcher) createLink(from, to, linkType string) error {
	links := easyjson.NewJSONArray()

	link := easyjson.NewJSONObject()
	link.SetByPath("from", easyjson.NewJSON(from))
	link.SetByPath("to", easyjson.NewJSON(to))
	link.SetByPath("linkType", easyjson.NewJSON(linkType))

	links.AddToArray(link)

	return d.add(easyjson.NewJSONObjectWithKeyValue("links", links).GetPtr())
}

func (d *dispatcher) add(payload *easyjson.JSON) error {
	const op = "functions.dispatcher.add"

	if _, err := d.r.IngressGolangSync(op, d.id, payload, nil); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
