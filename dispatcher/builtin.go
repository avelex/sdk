package dispatcher

import "github.com/foliagecp/easyjson"

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
	mode       linkMode
	From       string
	To         string
	Type       string
	ObjectType string
}

type _type struct {
	id   string
	body *easyjson.JSON
}

type _object struct {
	id   string
	body *easyjson.JSON
}
