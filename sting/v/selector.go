package v

import (
    "errors"
    "github.com/eaglc/lamer/log"
    "github.com/eaglc/lamer/registry"
    "github.com/eaglc/lamer/selector"
    "sync"
)

type Selector struct {
    sync.RWMutex
    opts selector.Options

    // nodes available
    //nodes map[string]registry.Node
    // key: name
    // val: []Node
    nodes map[string][]registry.Node
}

func (s *Selector) Init(opts ...selector.Option) {
    for _, o := range opts {
        o(&s.opts)
    }

    // TODO default options
}

func (s *Selector) Options() selector.Options {
    return s.opts
}

func (s *Selector) Select(n string) (selector.Next, error) {
    var nodes []registry.Node

    s.RLock()
    nodes, ok := s.nodes[n]
    if !ok {
        s.RUnlock()
        return nil, errors.New("not found")
    }
    s.RUnlock()

    for _, filter := range s.opts.Filters {
        nodes = filter.Do(nodes)
    }

    for i := range s.opts.Strategies {
        if next := s.opts.Strategies[i].Do(n, nodes); next != nil {
            return next, nil
        }
    }

    return nil, errors.New("not found")
}

func (s *Selector) SelectA(n string) (registry.Node, error){
    var nodes []registry.Node

    s.RLock()
    nodes, ok := s.nodes[n]
    if !ok {
        s.RUnlock()
        return nil, errors.New("not found")
    }
    s.RUnlock()

    for _, filter := range s.opts.Filters {
        nodes = filter.Do(nodes)
    }

    for i := range s.opts.Strategies {
        if rn := s.opts.Strategies[i].DoA(n, nodes); rn != nil {
            return rn, nil
        }
    }

    return nil, errors.New("not found")
}

func (s *Selector) String() string {
    return "v.selector"
}

func (s *Selector) Mark(node registry.Node, err error) {
    key, _ := node.MarshalKey()
    skey := string(key)

    n, _ := node.(*Node)

    nds, _ := s.nodes[n.Name]

    log.Debug("mark ", skey, " err:", err)
    s.Lock()
    if err != nil {
        if nds != nil {
            for i := range nds {
                k, _ := nds[i].MarshalKey()
                sk := string(k)
                if sk == skey {
                    nds = append(nds[:i], nds[i+1:]...)
                    break
                }
            }
        }
        s.nodes[n.Name] = nds
    } else {
        for i := range nds {
            k, _ := nds[i].MarshalKey()
            sk := string(k)
            if sk == skey {
                nds = append(nds[:i], nds[i+1:]...)
                break
            }
        }
        nds = append(nds, node)
        s.nodes[n.Name] = nds
    }
    s.Unlock()
}

func NewSelector(opts ...selector.Option) selector.Selector {
    var options = selector.Options{
        Strategies: []selector.Strategy{NewStrategy()},
    }

    for _, o := range opts {
        o(&options)
    }

    return &Selector{
        opts:options,
        nodes:make(map[string][]registry.Node),
    }
}
