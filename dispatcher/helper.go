package dispatcher

import (
	"fmt"

	"github.com/foliagecp/easyjson"
)

func (d *dispatcher) createFoliageObject(id string, payload *easyjson.JSON) error {
	const typename = "functions.graph.ll.api.object.create"

	if _, err := d.runtime.IngressGolangSync(typename, id, payload, nil); err != nil {
		return fmt.Errorf("dispatcher create object: %w", err)
	}

	return nil
}

func (d *dispatcher) createFoliageLink(from, to, linkType string, linkBody *easyjson.JSON, tags ...string) error {
	const typename = "functions.graph.ll.api.link.create"

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(linkType))

	if linkBody == nil {
		link.SetByPath("link_body", easyjson.NewJSONObject())
	} else {
		link.SetByPath("link_body", *linkBody)
	}

	if len(tags) > 0 {
		link.SetByPath("link_body.tags", easyjson.JSONFromArray(tags))
	}

	if _, err := d.runtime.IngressGolangSync(typename, from, &link, nil); err != nil {
		return fmt.Errorf("dispatcher create link: %w", err)
	}

	return nil
}
