package v

import (
    "errors"
    "github.com/eaglc/lamer/codec"
    "github.com/eaglc/lamer/registry"
    "github.com/eaglc/lamer/session"
    "sync"
)

// outer-user connection
type userSession struct {
}

const (
    StateInit session.State = iota
    StateConnectNow
    StateReadyToReconnect
    StateTimeout
    StateDestroy
    StateSendAuth
    StateAvailable
)

// inter-server connection
type nodeSession struct {
    sync.RWMutex
    node registry.Node
    codec codec.Codec
    state session.State
    err error
}

func (ns *nodeSession) Codec() codec.Codec {
    ns.RLock()
    defer ns.RUnlock()
    return ns.codec
}

func (ns *nodeSession) SetCodec(code codec.Codec) {
    ns.Lock()
    defer ns.Unlock()
    ns.codec = code
}

func (ns *nodeSession) Metadata() interface{} {
    ns.RLock()
    defer ns.RUnlock()
    return ns.node
}

func (ns *nodeSession) SetMetadata(v interface{}) {
    ns.Lock()
    defer ns.Unlock()
    ns.node = v.(registry.Node)
}

func (ns *nodeSession) State() session.State {
    ns.RLock()
    defer ns.RUnlock()
    return ns.state
}

func (ns *nodeSession) Error() error {
    ns.RLock()
    defer ns.RUnlock()
    return ns.err
}

func (ns *nodeSession) SetState(s session.State) {
    ns.Lock()
    defer ns.Unlock()
    ns.state = s
}

func (ns *nodeSession) SetError(e error) {
    ns.Lock()
    defer ns.Unlock()
    ns.err = e
}

func (ns *nodeSession) InState(s session.State) bool {
    ns.RLock()
    defer ns.RUnlock()
    return ns.state == s
}

func (ns *nodeSession) Close() error {
    ns.Lock()
    defer ns.Unlock()

    ns.state = StateDestroy

    if ns.codec != nil {
        return ns.codec.Close()
    }

    return nil
}

func NewSession(n registry.Node, codec codec.Codec) session.Session {
    return &nodeSession{
        node:n,
        codec:codec,
    }
}

type nodeSessionManager struct {
    sync.RWMutex

    // TODO
    // To be optimized, Reduce time complexity
    category map[string][]session.Session

    index map[string]session.Session
    addrs map[string]session.Session
}

func (mgr *nodeSessionManager) OnNewSession(s session.Session) error {
    node, err := GetMetadata(s)
    if err != nil {
        return err
    }

    category := node.Name
    id := node.Id
    addr := node.Addr()

    mgr.Lock()
    mgr.category[category] = append(mgr.category[category], s)
    mgr.index[id] = s
    mgr.addrs[addr] = s
    mgr.Unlock()

    return nil
}

func (mgr *nodeSessionManager) OnDelSession(s session.Session) error {
    node, err := GetMetadata(s)
    if err != nil {
        return err
    }

    category := node.Name
    id := node.Id
    addr := node.Addr()

    mgr.Lock()
    delete(mgr.addrs, addr)
    delete(mgr.index, id)
    if x, ok := mgr.category[category]; ok {
        for i := range x {
            n, err := GetMetadata(x[i])
            if err != nil {
                return err
            }
            if n.Id == node.Id && n.Name == node.Name {
                mgr.category[category] = append(mgr.category[category][:i], mgr.category[category][i+1:]...)
                break
            }
        }
    }

    mgr.Unlock()
    return nil
}

func (mgr *nodeSessionManager) OnUpdateSession(s session.Session) error {
    node, err := GetMetadata(s)
    if err != nil {
        return err
    }

    category := node.Name
    id := node.Id
    addr := node.Addr()

    mgr.Lock()
    if x, ok := mgr.category[category]; ok {
        for i := range x {
            n, err := GetMetadata(x[i])
            if err != nil {
                return err
            }
            if n.Id == node.Id && n.Name == node.Name {
                mgr.category[category] = append(mgr.category[category][:i], mgr.category[category][i+1:]...)
                break
            }
        }
    }

    mgr.category[category] = append(mgr.category[category], s)
    mgr.index[id] = s
    mgr.addrs[addr] = s
    mgr.Unlock()

    return nil
}

func (mgr *nodeSessionManager) All() ([]session.Session, error) {
    var sessions []session.Session
    mgr.RLock()
    for _, v := range mgr.index {
        sessions = append(sessions, v)
    }
    mgr.RUnlock()

    if len(sessions) > 0 {
        return sessions, nil
    }

    return nil, errors.New("have no session")
}

func (mgr *nodeSessionManager) Category(category interface{}) ([]session.Session, error) {
    mgr.RLock()
    c := category.(string)
    if ss, ok := mgr.category[c]; ok {
        mgr.RUnlock()
        return ss, nil
    }

    mgr.RUnlock()

    return nil, errors.New("no matching category")
}

func (mgr *nodeSessionManager) Indexes(indexes ...interface{}) ([]session.Session, error) {
    var sessions []session.Session
    mgr.RLock()
    for _, idx := range indexes {
        index := idx.(string)
        if ss, ok := mgr.index[index]; ok {
            sessions = append(sessions, ss)
        }
    }
    mgr.RUnlock()

    if len(sessions) > 0 {
        return sessions, nil
    }

    return nil, errors.New("no matching indexes")
}

func (mgr *nodeSessionManager) Index(index interface{}) (session.Session, error) {
    mgr.RLock()
    idx := index.(string)
    if ss, ok := mgr.index[idx]; ok {
        mgr.RUnlock()
        return ss, nil
    }

    mgr.RUnlock()

    return nil, errors.New("not matching index")
}

func (mgr *nodeSessionManager) Addrs(addrs ...string) ([]session.Session, error) {
    var sessions []session.Session

    mgr.RLock()
    for _, addr := range addrs {
        if ss, ok := mgr.addrs[addr]; ok {
            sessions = append(sessions, ss)
        }
    }
    mgr.RUnlock()

    if len(sessions) > 0 {
        return sessions, nil
    }

    return nil, errors.New("no matching addrs")
}

func (mgr *nodeSessionManager) Addr(addr string) (session.Session, error) {
    mgr.RLock()
    if ss, ok := mgr.addrs[addr]; ok {
        mgr.RUnlock()
        return ss, nil
    }

    mgr.RUnlock()

    return nil, errors.New("no matching addr")
}

func NewSessionManager() session.Manager {
    return &nodeSessionManager{
        category:make(map[string][]session.Session),
        index:make(map[string]session.Session),
        addrs:make(map[string]session.Session),
    }
}

func GetMetadata(ss session.Session) (*Node, error) {
    if node, ok := ss.Metadata().(*Node); ok {
        return node, nil
    }

    return nil, errors.New("the type of metadata is not *Node")
}