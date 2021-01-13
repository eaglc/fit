package etcd

import (
    "context"
    "github.com/eaglc/lamer/log"
    "github.com/eaglc/lamer/registry"
    "go.etcd.io/etcd/clientv3"
    "path"
    "strings"
    "sync"
)

const (
    prefix = "/lamer/registry/"
)

type etcdRegistry struct {
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

    return &etcdRegistry {
        opts:options,
        nodes:make(map[string][]registry.Node),
    }
}

func (r *etcdRegistry) Init(opts ...registry.Option) {
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

func (r *etcdRegistry) Watch(ctx context.Context, key string) (registry.Watcher, error) {
    key = fullPath(key)
    return newEtcdWatcher(r, ctx, key)
}

func (r *etcdRegistry) Options() registry.Options {
    return r.opts
}

// get from registry
// if !exist
//     grant and register
// else
//     if changed
//         grant and register
//
func (r *etcdRegistry) Register(n registry.Node, opts ...registry.RegisterOption) error {
    var (
        //reg = false
        //h uint64 = 0
        leaseId clientv3.LeaseID = 0
    )

    key := n.Key()
    ctx, cancel := context.WithTimeout(r.opts.Context, r.opts.Timeout)
    defer cancel()

    log.Debug("register node:", fullPath(key))

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
        if _, err := r.client.Put(ctx, fullPath(key), string(n.Data()), clientv3.WithLease(leaseId)); err != nil {
            return err
        }

        if _, err := r.client.KeepAlive(r.opts.Context, leaseId); err != nil {
            return err
        }
    } else {
        // Permanent node
        if _, err := r.client.Put(ctx, fullPath(key), string(n.Data())); err != nil {
            return err
        }
    }

    return nil
}

func (r *etcdRegistry) Deregister(n registry.Node) error {
    ctx, cancel := context.WithTimeout(r.opts.Context, r.opts.Timeout)
    defer cancel()

    key := n.Key()
    _, err := r.client.Delete(ctx, fullPath(key))

    log.Debug("deregister node:", fullPath(key))

    return err
}

func (r *etcdRegistry) GetNodes(key string) ([]registry.Node, error) {
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
        if n, err := r.opts.Decode(kv.Value); err == nil {
            nds = append(nds, n)
        }
    }

    r.Lock()
    r.nodes[key] = nds
    r.Unlock()

    return nds, nil
}

func (r *etcdRegistry) String() string {
    return "lamer.registry"
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