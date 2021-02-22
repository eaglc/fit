package strategy

import (
	"fmt"
	"github.com/eaglc/lamer/registry"
	"github.com/eaglc/lamer/selector"
	"stathat.com/c/consistent"
	"sync"
)

type hashRing struct {
	sync.RWMutex
	rings   map[string]*consistent.Consistent
	nodes   map[string]registry.Node
	replica int
}

func (h *hashRing) Do(nds []registry.Node, opts ...selector.SelectOption) selector.Next {
	return func() (node registry.Node, e error) {
		return nil, fmt.Errorf("not support")
	}
}

func (h *hashRing) DoA(nds []registry.Node, opts ...selector.SelectOption) registry.Node {
	var op selector.SelectOptions
	for _, o := range opts {
		o(&op)
	}

	name := op.Name
	key := op.Key

	if len(nds) > 0 {
		for i := range nds {
			h.Mark(name, nds[i], nil)
		}
	}

	h.RLock()
	c := h.rings[name]
	h.RUnlock()

	if c == nil {
		return nil
	}

	if addr, err := c.Get(key); err != nil {
		return nil
	} else {
		h.RLock()
		n, ok := h.nodes[addr]
		h.RUnlock()

		if ok {
			return n
		} else {
			return nil
		}
	}
}

func (h *hashRing) Mark(name string, node registry.Node, err error) {
	h.RLock()
	c, _ := h.rings[name]
	h.RUnlock()

	if c == nil {
		c = consistent.New()
		if h.replica > 0 {
			c.NumberOfReplicas = h.replica
		}
	}

	h.Lock()
	if err == nil {
		c.Add(node.Addr())
		delete(h.nodes, node.Addr())
	} else {
		c.Remove(node.Addr())
		h.nodes[node.Addr()] = node
	}
	h.rings[name] = c

	h.Unlock()
}

func (h *hashRing) String() string {
	return "hashring"
}

func NewHashRing(replica int) selector.Strategy {
	return &hashRing{
		rings:   make(map[string]*consistent.Consistent),
		nodes:   make(map[string]registry.Node),
		replica: replica,
	}
}
