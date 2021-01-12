package v

import (
    "context"
    "github.com/eaglc/lamer/log"
    "github.com/eaglc/lamer/registry"
)

type Watcher struct {
    Context context.Context
    Client *Client
}

func (w *Watcher) Create(n registry.Node) {
    d,_ := n.MarshalNode()
    log.Debug("on create node:", string(d))
    //addr := n.Addr()
    _ = w.Client.newNode(n)
    //_ = w.Client.newConn(addr)
}

func (w *Watcher) Update(n registry.Node) {
    d,_ := n.MarshalNode()
    log.Debug("on update node:", string(d))

    _ = w.Client.newNode(n)
}

func (w *Watcher) Delete(n registry.Node) {
    d,_ := n.MarshalNode()
    log.Debug("on delete node:", string(d))
}

func NewWatcher(ctx context.Context) registry.Watcher {
    v := ctx.Value(CtxKeyClient)

    if cli, ok := v.(*Client); ok {
        return &Watcher{
            Context:ctx,
            Client:cli,
        }
    }

    return &Watcher{Context:ctx}
}

func (w *Watcher) GetNodes(s string) ([]registry.Node, error) {
    return nil, nil
}
