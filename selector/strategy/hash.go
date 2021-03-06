package strategy

import (
	"fmt"
	"github.com/eaglc/lamer/registry"
	"github.com/eaglc/lamer/selector"
)

type hash struct {
}

func (h *hash) Do(nds []registry.Node, opts ...selector.SelectOption) selector.Next {
	return func() (node registry.Node, e error) {
		return nil, fmt.Errorf("not support")
	}
}

func (h *hash) DoA(nds []registry.Node, opts ...selector.SelectOption) registry.Node {
	return nil
}

func (h *hash) Mark(name string, node registry.Node, err error) {
	return
}

func (h *hash) String() string {
	return "hash"
}

func NewHash() selector.Strategy {
	return &hash{}
}
