package etcd

import (
    "context"
    "encoding/json"
    "github.com/eaglc/lamer/log"
    "github.com/eaglc/lamer/registry"
    "go.etcd.io/etcd/clientv3"
    "path"
    "strings"
    "sync"
)

const (
    prefix = "/rtjazz/registry/"
)

type Registry struct {
    sync.RWMutex
    opts registry.Options
    client *clientv3.Client

    // TODO
    // Optimize search performance of []registry.Node
    nodes map[string][]registry.Node
}

func NewRegistry(opts ...registry.Option) registry.Registry {
    var options registry.Options
    for _, o := range opts {
        o(&options)
    }

    return &Registry{
        opts:options,
        nodes:make(map[string][]registry.Node),
    }
}

func (r *Registry) Init(opts ...registry.Option) {
    // TODO handle some default options
    for _, o := range opts {
        o(&r.opts)
    }

    if r.opts.Context == nil {
        r.opts.Context = context.TODO()
    }

    config := clientv3.Config{Endpoints:r.opts.Addrs}
    cli, err := clientv3.New(config)
    if err != nil {
        panic(err)
    }

    r.client = cli
}

func (r *Registry) Watch(ctx context.Context, key string, watcher registry.NewWatcher) error {
    key = fullPath(key)
    wch := r.client.Watch(r.opts.Context, key, clientv3.WithPrefix(), clientv3.WithPrevKV())

    go func() {
        rwatcher := watcher(ctx)

        for w := range wch {
            if w.Err() != nil {
                break
            }

            for _, e := range w.Events {

                switch e.Type {
                case clientv3.EventTypePut:
                    log.Debug("event type put")
                    if e.IsCreate() {
                        rwatcher.Create(decode(e.Kv.Value))
                        r.Lock()
                        r.nodes[key] = append(r.nodes[key], decode(e.Kv.Value))
                        r.Unlock()
                    } else if e.IsModify() {
                        log.Debug("event type modify")
                        rwatcher.Update(decode(e.Kv.Value))
                        r.Lock()
                        if nodes, ok := r.nodes[key]; ok {
                            for i := range nodes {
                                k, _ := nodes[i].MarshalKey()
                                if string(k) == string(e.Kv.Key) {
                                    nodes = append(nodes[:i], nodes[i+1:]...)
                                    nodes = append(nodes, decode(e.Kv.Value))
                                    break
                                }
                            }

                            r.nodes[key] = nodes
                        } else {
                            r.nodes[key] = append(r.nodes[key], decode(e.Kv.Value))
                        }
                        r.Unlock()
                    }
                case clientv3.EventTypeDelete:
                    rwatcher.Delete(decode(e.PrevKv.Value))
                    r.Lock()
                    if nodes, ok := r.nodes[key]; ok {
                        for i := range nodes {
                            k, _ := nodes[i].MarshalKey()
                            if fullPath(string(k)) == string(e.PrevKv.Key) {
                                nodes = append(nodes[:i], nodes[i+1:]...)
                                break
                            }
                        }

                        r.nodes[key] = nodes
                    }
                    r.Unlock()
                }
            }
        }
    }()

    return nil
}

func (r *Registry) Options() registry.Options {
    return r.opts
}

// get from registry
// if !exist
//     grant and register
// else
//     if changed
//         grant and register
//
func (r *Registry) Register(n registry.Node, opts ...registry.RegisterOption) error {
    var (
        //reg = false
        //h uint64 = 0
        leaseId clientv3.LeaseID = 0
    )

    key := nodeKey(n)
    ctx, cancel := context.WithTimeout(r.opts.Context, r.opts.Timeout)
    defer cancel()

    log.Debug("register node:", fullPath(key))

    //getRsp, err := r.client.Get(ctx, fullPath(key))
    //if err != nil {
    //    return err
    //}
    //
    //if getRsp.Count != 0 {
    //    for _, kv := range getRsp.Kvs {
    //        nd := decode(kv.Value)
    //        if nd == nil {
    //            return errors.New("decode error")
    //        }
    //
    //        h1, err := hash.Hash(nd, nil)
    //        if err != nil {
    //            return err
    //        }
    //
    //        h, err = hash.Hash(n, nil)
    //        if err != nil {
    //            return err
    //        }
    //
    //        if h != h1 {
    //            reg = true
    //        }
    //        break
    //    }
    //} else {
    //    reg = true
    //}
    //
    //if !reg {
    //    leaseRsp, _ := r.client.Leases(ctx)
    //    if leaseRsp != nil {
    //        for _, l := range leaseRsp.Leases {
    //            if _, err := r.client.KeepAlive(r.opts.Context, l.ID); err != nil {
    //                return err
    //            }
    //        }
    //    }
    //    return nil
    //}

    var options registry.RegisterOptions
    for _, o := range opts {
        o(&options)
    }
    if options.TTL > 0 {
        lease, err := r.client.Grant(ctx, int64(options.TTL.Seconds()))
        if err != nil {
            return err
        }
        leaseId = lease.ID
    }

    if leaseId > 0 {
        // Temporary node
        if _, err := r.client.Put(ctx, fullPath(key), encode(n), clientv3.WithLease(leaseId)); err != nil {
            return err
        }

        if _, err := r.client.KeepAlive(r.opts.Context, leaseId); err != nil {
            return err
        }
    } else {
        // Permanent node
        if _, err := r.client.Put(ctx, fullPath(key), encode(n)); err != nil {
            return err
        }
    }

    return nil
}

func (r *Registry) Deregister(n registry.Node) error {
    ctx, cancel := context.WithTimeout(r.opts.Context, r.opts.Timeout)
    defer cancel()

    key := nodeKey(n)
    _, err := r.client.Delete(ctx, fullPath(key))

    log.Debug("deregister node:", fullPath(key))

    return err
}

func (r *Registry) GetNodes(key string) ([]registry.Node, error) {
    key = fullPath(key)

    r.RLock()
    nodes, ok := r.nodes[key]
    r.RUnlock()

    if ok {
        return nodes, nil
    }

    // get from registry
    ctx, cancel := context.WithTimeout(r.opts.Context, r.opts.Timeout)
    defer cancel()

    rsp, err := r.client.Get(ctx, key, clientv3.WithPrefix())
    if err != nil {
        return nil, err
    }

    var nds []registry.Node
    for _, kv := range rsp.Kvs {
        nds = append(nds, decode(kv.Value))
    }

    r.Lock()
    r.nodes[key] = nds
    r.Unlock()

    return nds, nil
}

func (r *Registry) String() string {
    return "rtjazz.registry"
}

func fullPath(s string) string {
    return path.Join(prefix, s)
}

func parentPath(s string) string {
    full := fullPath(s)
    index := strings.LastIndex(string(full[0:len(full)-2]), "/")
    if index != -1 {
        return string(full[0:index])
    }
    return full
}

func decode(b []byte) *Node {
    var n Node
    _ = json.Unmarshal(b, &n)
    return &n
}

func encode(n registry.Node) string {
    d, _ := n.MarshalNode()
    return string(d)
}

func nodeKey(n registry.Node) string {
    k, _ := n.MarshalKey()
    return string(k)
}