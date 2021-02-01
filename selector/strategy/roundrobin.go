package strategy

import (
	"errors"
	"github.com/eaglc/lamer/registry"
	"github.com/eaglc/lamer/selector"
	"sync"
	"sync/atomic"
)

type roundBin struct {
	sync.RWMutex
	nodes map[string][]registry.Node
	off   map[string]*uint32
}

func (r *roundBin) Do(opts ...selector.SelectOption) selector.Next {
	var op selector.SelectOptions
	for _, o := range opts {
		o(&op)
	}

	name := op.Name
	i := 0
	r.RLock()
	nodes, _ := r.nodes[name]
	r.RUnlock()

	mtx := sync.Mutex{}
	return func() (n registry.Node, e error) {
		if len(nodes) == 0 {
			return nil, errors.New("no available")
		}

		mtx.Lock()
		n = nodes[i%len(nodes)]
		mtx.Unlock()
		i++

		return n, nil
	}
}

func (r *roundBin) DoA(opts ...selector.SelectOption) registry.Node {
	var op selector.SelectOptions
	for _, o := range opts {
		o(&op)
	}

	name := op.Name

	r.RLock()
	nodes := r.nodes[name]
	r.RUnlock()

	if len(nodes) == 0 {
		return nil
	}

	r.RLock()
	_, ok := r.off[name]
	r.RUnlock()

	if !ok {
		var j uint32 = 0
		r.Lock()
		r.off[name] = &j
		r.Unlock()
	}

	k := atomic.AddUint32(r.off[name], 1)

	return nodes[k%uint32(len(nodes))]
}

func (r *roundBin) Mark(name string, node registry.Node, err error) {
	nds, _ := r.nodes[name]
	skey := node.Key()

	r.Lock()
	if err != nil {
		if nds != nil {
			for i := range nds {
				sk := nds[i].Key()
				if sk == skey {
					nds = append(nds[:i], nds[i+1:]...)
					break
				}
			}
		}
		r.nodes[name] = nds
	} else {
		for i := range nds {
			sk := nds[i].Key()
			if sk == skey {
				nds = append(nds[:i], nds[i+1:]...)
				break
			}
		}
		nds = append(nds, node)
		r.nodes[name] = nds
	}
	r.Unlock()
}

func (r *roundBin) String() string {
	return "roundbin"
}

func NewRoundRobin() selector.Strategy {
	return &roundBin{
		off:   make(map[string]*uint32),
		nodes: make(map[string][]registry.Node),
	}
}
