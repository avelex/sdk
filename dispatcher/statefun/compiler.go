package statefun

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/foliagecp/easyjson"
)

type compiler struct {
	source  string
	types   map[string]*_type
	objects map[string]*_object
	links   map[string]*_link
}

func compile(source string, links, types, objects map[string]any) error {
	c, err := newCompiler(source, links, types, objects)
	if err != nil {
		return err
	}

	return c.compile()
}

func newCompiler(source string, links, types, objects map[string]any) (*compiler, error) {
	typedLinks := make(map[string]*_link, len(links))
	for k, v := range links {
		typedLinks[k] = linkFromJSON(v)
	}

	typedTypes := make(map[string]*_type, len(types))
	for k, v := range types {
		typedTypes[k] = typeFromJSON(v)
	}

	typedObjects := make(map[string]*_object, len(objects))
	for k, v := range objects {
		typedObjects[k] = objectFromJSON(v)
	}

	return &compiler{
		source:  source,
		types:   typedTypes,
		objects: typedObjects,
		links:   typedLinks,
	}, nil
}

func (c *compiler) compile() error {
	if err := c.initBuilInObjects(); err != nil {
		return err
	}

	if err := c.validateAllIDs(); err != nil {
		return err
	}

	if err := c.checkObjectsTypeMatching(); err != nil {
		return fmt.Errorf("object type matching: %w", err)
	}

	if err := c.buildLinks(); err != nil {
		return fmt.Errorf("build links: %w", err)
	}

	return nil
}

func (c *compiler) validateAllIDs() error {
	return nil
}

func (c *compiler) checkObjectsTypeMatching() error {
	for _, object := range c.objects {
		if _, ok := c.types[object.OriginType]; !ok {
			return fmt.Errorf("object '%s' missmatching with '%s' type", object.ID, object.OriginType)
		}
	}
	return nil
}

func (c *compiler) buildLinks() error {
	for id, link := range c.links {
		_, fromTypeExists := c.types[link.From]
		_, fromObjectExists := c.objects[link.From]
		if !fromObjectExists && !fromTypeExists {
			return fmt.Errorf("%s exists neither in types nor in objects", link.From)
		}

		_, toTypeExists := c.types[link.To]
		_, toObjectExists := c.objects[link.To]
		if !toTypeExists && !toObjectExists {
			return fmt.Errorf("%s exists neither in types nor in objects", link.To)
		}

		if link.Type == "" {
			return fmt.Errorf("%s missed type", id)
		}

		switch link.Mode {
		case _DEFINED:
		case _UNDEFINED:
			objectTypeFrom := c.objects[link.From]
			objectTypeTo := c.objects[link.To]
			linkID := objectTypeFrom.OriginType + "/" + objectTypeTo.OriginType

			findLink, ok := c.links[linkID]
			if !ok {
				return fmt.Errorf("can't find link type between %s", linkID)
			}

			if findLink.ObjectType == "" {
				return fmt.Errorf("%s object link type is empty", linkID)
			}

			c.links[id] = &_link{
				Mode: _DEFINED,
				From: link.From,
				To:   link.To,
				Type: findLink.ObjectType,
			}
		}
	}
	return nil
}

func (c *compiler) createLinkTypes(from, to, objectLinkType string) {
	c.createLink(_DEFINED, from, to, to, objectLinkType)
}

func (c *compiler) createLinkObjects(from, to string) {
	c.createLink(_UNDEFINED, from, to, OBJECT_2_OBJECT_TYPELINK, "")
}

func (c *compiler) createLink(mode linkMode, from, to, linkType, objectLinkType string) {
	linkID := from + "/" + to // maybe hash

	// TODO: lock mutex if create link will be concurrent
	c.links[linkID] = &_link{
		Mode:       mode,
		From:       from,
		To:         to,
		Type:       linkType,
		ObjectType: objectLinkType,
	}
}

func (c *compiler) createObject(id, originType string, body *easyjson.JSON) error {
	c.objects[id] = &_object{
		ID:         id,
		OriginType: originType,
	}

	if originType == "builtin" {
		return nil
	}

	path := filepath.Join(c.source, "obj_"+id+".json")

	if len(body.ToBytes()) != 0 {
		if err := os.WriteFile(path, body.ToBytes(), os.ModePerm); err != nil {
			return err
		}
	}

	c.objects[id].Path = path

	// create objects -> object link
	c.createLink(_DEFINED, BUILT_IN_OBJECTS, id, OBJECT_TYPELINK, "")

	// create type -> object link
	c.createLink(_DEFINED, originType, id, OBJECT_TYPELINK, "")

	// create object -> type link
	c.createLink(_DEFINED, id, originType, TYPE_TYPELINK, "")

	return nil
}

func (c *compiler) createType(id string, body *easyjson.JSON) error {
	path := filepath.Join(c.source, "type_"+id+".json")

	if len(body.ToBytes()) != 0 {
		if err := os.WriteFile(path, body.ToBytes(), os.ModePerm); err != nil {
			return err
		}
	}

	c.types[id] = &_type{
		ID:   id,
		Path: path,
	}

	// create types -> type link
	c.createLink(_DEFINED, BUILT_IN_TYPES, id, TYPE_TYPELINK, "")

	return nil
}

func sendToGraph() error {
	return nil
}

func (c *compiler) initBuilInObjects() error {
	c.types["builtin"] = &_type{}

	// create root
	if err := c.createObject(BUILT_IN_ROOT, "builtin", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}

	// create objects and types
	if err := c.createObject(BUILT_IN_OBJECTS, "builtin", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}

	if err := c.createObject(BUILT_IN_TYPES, "builtin", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}

	// create root -> objects link
	c.createLink(_DEFINED, BUILT_IN_ROOT, BUILT_IN_OBJECTS, OBJECTS_TYPELINK, "")

	// create root -> types link
	c.createLink(_DEFINED, BUILT_IN_ROOT, BUILT_IN_TYPES, TYPES_TYPELINK, "")

	// create group type ----------------------------------------
	c.createType("group", easyjson.NewJSONObject().GetPtr())

	// link from group -> group, need for define "group" link type
	c.createLinkTypes("group", "group", GROUP_TYPELINK)
	//-----------------------------------------------------------

	// create NAV ------------------------------------------------
	if err := c.createObject("nav", "group", easyjson.NewJSONObject().GetPtr()); err != nil {
		return err
	}
	// -----------------------------------------------------------

	slog.Info("[X] Initialized Built-In objects")

	return nil
}
