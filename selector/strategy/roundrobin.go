package strategy

import (
    "errors"
    "github.com/eaglc/lamer/registry"
    "github.com/eaglc/lamer/selector"
    "sync"
    "sync/atomic"
)

type RoundBin struct {
    sync.RWMutex
    off map[string]*uint32
}

func (r *RoundBin) Do(name string, nodes []registry.Node) selector.Next {
    i := 0
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

func (r *RoundBin) DoA(name string, nodes []registry.Node) registry.Node {
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

func NewRoundRobin() selector.Strategy {
    return &RoundBin{
        off:make(map[string]*uint32),
    }
}