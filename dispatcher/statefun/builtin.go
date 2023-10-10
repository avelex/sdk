package statefun

import (
	"encoding/json"

	"github.com/foliagecp/easyjson"
)

const (
	OBJECTS_TYPELINK         = "__objects"
	TYPES_TYPELINK           = "__types"
	TYPE_TYPELINK            = "__type"
	OBJECT_TYPELINK          = "__object"
	OBJECT_2_OBJECT_TYPELINK = "obj"
	BUILT_IN_TYPES           = "types"
	BUILT_IN_OBJECTS         = "objects"
	BUILT_IN_ROOT            = "root"
	GROUP_TYPELINK           = "group"
)

type linkMode int

const (
	_UNDEFINED linkMode = iota
	_DEFINED
)

type _link struct {
	Mode       linkMode `json:"mode,omitempty"`
	From       string   `json:"from"`
	To         string   `json:"to"`
	Type       string   `json:"linkType,omitempty"`
	ObjectType string   `json:"objectType,omitempty"`
}

func linkFromJSON(v any) *_link {
	var l _link
	if err := unmarshalEasyJSON(&v, &l); err != nil {
		return &_link{}
	}
	return &l
}

type _type struct {
	ID   string         `json:"id"`
	Path string         `json:"path"`
	Body *easyjson.JSON `json:"body,omitempty"`
}

func typeFromJSON(v any) *_type {
	var t _type
	if err := unmarshalEasyJSON(&v, &t); err != nil {
		return &_type{}
	}
	return &t
}

type _object struct {
	ID         string         `json:"id"`
	Path       string         `json:"path,omitempty"`
	OriginType string         `json:"originType"`
	Body       *easyjson.JSON `json:"body,omitempty"`
}

func objectFromJSON(v any) *_object {
	var o _object
	if err := unmarshalEasyJSON(&v, &o); err != nil {
		return &_object{}
	}
	return &o
}

func unmarshalEasyJSON(v any, out any) error {
	bytes, err := json.Marshal(v)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(bytes, out); err != nil {
		return err
	}

	return nil
}
