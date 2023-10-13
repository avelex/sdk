package dispatcher

import (
	"errors"
	"fmt"
	"sync"

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

var _ Dispatcher = (*dispatcher)(nil)

type Dispatcher interface {
	CreateType(id string, body *easyjson.JSON) error
	CreateObject(id, originType string, body *easyjson.JSON) error
	CreateTypesLink(from, to, linkType string) error
	CreateObjectsLink(from, to string) error
	Commit() error
}

type dispatcher struct {
	once                  *sync.Once
	id                    string
	limit                 int64
	filled                int64
	mu                    *sync.Mutex
	types, links, objects []*easyjson.JSON
	runtime               *statefun.Runtime
}

func New(r *statefun.Runtime) *dispatcher {
	return &dispatcher{
		once:    &sync.Once{},
		limit:   1 << 14,
		filled:  0,
		mu:      &sync.Mutex{},
		types:   make([]*easyjson.JSON, 0),
		links:   make([]*easyjson.JSON, 0),
		objects: make([]*easyjson.JSON, 0),
		runtime: r,
	}
}

func (d *dispatcher) CreateTypesLink(from, to, linkType string) error {
	return d.createLink(from, to, linkType)
}

func (d *dispatcher) CreateObjectsLink(from, to string) error {
	return d.createLink(from, to, "")
}

func (d *dispatcher) CreateType(id string, body *easyjson.JSON) error {
	t := easyjson.NewJSONObject()
	t.SetByPath("id", easyjson.NewJSON(id))

	if body.PathExists("body") {
		t.SetByPath("body", body.GetByPath("body"))
	} else {
		t.SetByPath("body", *body)
	}

	return d.addType(&t)
}

func (d *dispatcher) CreateObject(id, originType string, body *easyjson.JSON) error {
	obj := easyjson.NewJSONObject()
	obj.SetByPath("id", easyjson.NewJSON(id))
	obj.SetByPath("originType", easyjson.NewJSON(originType))

	if body.PathExists("body") {
		obj.SetByPath("body", body.GetByPath("body"))
	} else {
		obj.SetByPath("body", *body)
	}

	return d.addObject(&obj)
}

func (d *dispatcher) Commit() error {
	const op = "functions.dispatcher.commit"

	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.empty() {
		if err := d.send(); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}

		d.reset()
	}

	id, err := d.workDispatcherID()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	result, err := d.runtime.IngressGolangSync(op, id, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if result.GetByPath("payload.status").AsStringDefault("failed") == "failed" {
		return fmt.Errorf("%s: %s", op, result.GetByPath("payload.result").AsStringDefault(""))
	}

	return nil
}

func (d *dispatcher) createLink(from, to, linkType string) error {
	link := easyjson.NewJSONObject()
	link.SetByPath("from", easyjson.NewJSON(from))
	link.SetByPath("to", easyjson.NewJSON(to))
	link.SetByPath("linkType", easyjson.NewJSON(linkType))
	return d.addLink(&link)
}

func (d *dispatcher) send() error {
	const op = "functions.dispatcher.add"

	id, err := d.workDispatcherID()
	if err != nil {
		return err
	}

	batch := easyjson.NewJSONObject()
	batch.SetByPath("links", *sliceToJSON(d.links))
	batch.SetByPath("objects", *sliceToJSON(d.objects))
	batch.SetByPath("types", *sliceToJSON(d.types))

	if _, err := d.runtime.IngressGolangSync(op, id, &batch, nil); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (d *dispatcher) workDispatcherID() (string, error) {
	d.once.Do(func() {
		const op = "functions.dispatcher.begin"

		result, err := d.runtime.IngressGolangSync(op, "main_dispatcher", easyjson.NewJSONObject().GetPtr(), nil)
		if err != nil {
			return
		}

		workDispatcherID := result.GetByPath("payload.id").AsStringDefault("")
		if workDispatcherID == "" {
			return
		}

		d.id = workDispatcherID
	})

	if d.id == "" {
		return "", errors.New("empty work dispatcher id")
	}

	return d.id, nil
}

func (d *dispatcher) reset() {
	d.links = d.links[:0]
	d.types = d.types[:0]
	d.objects = d.objects[:0]
	d.filled = 0
}

func (d *dispatcher) addLink(l *easyjson.JSON) error {
	size := int64(len(l.ToBytes()))

	if err := d.checkOverflow(size); err != nil {
		return err
	}

	d.links = append(d.links, l)

	return nil
}

func (d *dispatcher) addType(t *easyjson.JSON) error {
	size := int64(len(t.ToBytes()))

	if err := d.checkOverflow(size); err != nil {
		return err
	}

	d.types = append(d.types, t)

	return nil
}

func (d *dispatcher) addObject(o *easyjson.JSON) error {
	size := int64(len(o.ToBytes()))

	if err := d.checkOverflow(size); err != nil {
		return err
	}

	d.objects = append(d.objects, o)

	return nil
}

func (d *dispatcher) checkOverflow(size int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.filled+size < d.limit {
		d.filled += size
		return nil
	}

	if err := d.send(); err != nil {
		return err
	}

	d.reset()

	return nil
}

func (d *dispatcher) empty() bool {
	return len(d.links) == 0 && len(d.objects) == 0 && len(d.types) == 0
}

func sliceToJSON(v []*easyjson.JSON) *easyjson.JSON {
	out := easyjson.NewJSONArray()
	for _, j := range v {
		out.AddToArray(*j)
	}
	return &out
}
