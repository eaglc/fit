package v

import (
    "context"
    "errors"
    "github.com/eaglc/lamer/client"
    "github.com/eaglc/lamer/codec"
    "github.com/eaglc/lamer/log"
    "github.com/eaglc/lamer/registry"
    "github.com/eaglc/lamer/router"
    "github.com/eaglc/lamer/session"
    "github.com/eaglc/lamer/transport"
    "sync"
    "time"
)

// TODO with connection pool
// Do not force use pool, if need to implement it yourself
type Client struct {
    sync.RWMutex
    opts client.Options

    // connections
    ccs map[string] codec.Codec

    done chan struct{}

    // sessions to connect now
    now chan session.Session

    // sessions to reconnect
    retry []session.Session
}

func (cli *Client) Init(opts ...client.Option) {
    for _, o := range opts {
        o(&cli.opts)
    }

    // TODO default options
}

func (cli *Client) String() string {
    return "v.client"
}

func (cli *Client) Name() string {
    return cli.opts.Name
}

func (cli *Client) Options() client.Options {
    return cli.opts
}

func (cli *Client) Router() router.Router {
    return cli.opts.Router
}

func (cli *Client) Start() error {
    // TODO direct mode
    ctx,cancel := context.WithCancel(cli.opts.Context)
    defer cancel()
    r := cli.opts.Registry
    for i := range cli.opts.Endpoints {
        nodes, err := cli.opts.Registry.GetNodes(cli.opts.Endpoints[i])
        if err != nil {
            log.Error("get nodes from registry error: ", err, " ed:", cli.opts.Endpoints[i])
           return err
        }


        for _, n := range nodes {
            if err := cli.newNode(n); err != nil {
               return err
           }
        }
        // TODO

        if err := r.Watch(context.WithValue(ctx, CtxKeyClient, cli), cli.opts.Endpoints[i], NewWatcher); err != nil {
            return err
        }
    }

    go cli.connectNow()
    go cli.retryConnect()

    go func() {
        select {
        case <- cli.done:
        }
    }()
    return nil
}

func (cli *Client) Stop() error {
    close(cli.done)
    sessions, _ := cli.opts.Cache.All()

    for _, ss := range sessions {
        _ = ss.Codec().Close()
    }

    return nil
}

func (cli *Client) Send(m interface{}) error {
    // TODO
    // fetch endpoint name
    ed := "endpoint name"

    if message, ok := m.(*Message); !ok {
        return errors.New("unknown message")
    } else {
        ed = message.ServiceName
    }

    //next, err := cli.opts.Selector.Select(ed)
    //if err != nil {
    //    return err
    //}

    node, err := cli.opts.Selector.SelectA(ed)
    if err != nil {
        return err
    }

    if ss, err := cli.opts.Cache.Addr(node.Addr()); err == nil {
        if err := ss.Codec().Write(m); err != nil {
            log.Error("write failed: ", node.Addr(), " err: ", err)
            cli.opts.Selector.Mark(node, err)

            if !ss.InState(StateDestroy) {
                ss.SetState(StateReadyToReconnect)
                cli.retry = append(cli.retry, ss)
            }

            return err
        }
    } else {
        cli.opts.Selector.Mark(node, err)
        // TODO
        return err
    }

    return nil
}

func (cli *Client) serveSession(sess session.Session) {
    // TODO
    // handle duplicate and connection retry.
    nd, _ := GetMetadata(sess)
    cli.opts.Selector.Mark(nd, nil)

    retry := true
    breakErr := errors.New("")
    code := sess.Codec()
    errChan := make(chan error)
    go func() {
        for {
            m := NewMessage("")
            err := code.Read(m)
            if err != nil {
                log.Error("read failed: ", nd, " err: ", err)
                errChan <- err

                // TODO
                break
            }

            req := &Request{
                body:m,
            }

            rw := &ResponseWriter{
                w:code,
            }

            _ = cli.Router().Serve(cli.opts.Context, req, rw)
        }
    }()

    select {
    case <- cli.done:
        retry = false
    case breakErr = <- errChan:
    }

    if sess.InState(StateDestroy) {
        retry = false
    }

    cli.opts.Selector.Mark(nd, breakErr)
    if retry {
        sess.SetState(StateReadyToReconnect)
        cli.retry = append(cli.retry, sess)
    } else {
        for _, cb := range cli.opts.Callbacks {
            _ = cb.OnDelSession(sess)
        }
    }
}

func (cli *Client) serveConn(c transport.Conn) {
    // TODO
    // handle duplicate and connection retry.
    addr := c.RemoteAddr()

    code := cli.opts.Codec(c)

    cli.Lock()
    cli.ccs[addr] = code
    cli.Unlock()

    wg := sync.WaitGroup{}
    go func() {
        wg.Add(1)
        defer wg.Done()
        for {
            m := NewMessage("")
            err := code.Read(m)
            if err != nil {
                // TODO
                break
            }

            req := &Request{
                body:m,
            }

            rw := &ResponseWriter{
                w:code,
            }

            _ = cli.Router().Serve(cli.opts.Context, req, rw)
        }
    }()

    select {
    case <- cli.done:
        _ = code.Close()
        cli.Lock()
        delete(cli.ccs, addr)
        cli.Unlock()
    }

    wg.Wait()
}

func (cli *Client) connectNow() {
    // TODO lock
    for ss := range cli.now {
        if !ss.InState(StateConnectNow) {
            continue
        }

        select {
        case <- cli.done:
            break
        default:
        }

        node, _ := GetMetadata(ss)
        log.Debug("connect node: ", node)
        c, err := cli.opts.Transport.Dial(node.Addr())
        if err != nil {
            log.Debug("connect node failed: ", node, " err: ", err)
            ss.SetState(StateReadyToReconnect)
            cli.retry = append(cli.retry, ss)
            continue
        }

        code := cli.opts.Codec(c)
        ss.SetCodec(code)
        ss.SetState(StateAvailable)
        log.Debug("connect node success: ", node)
        for _, cb := range cli.opts.Callbacks {
            _ = cb.OnNewSession(ss)
        }

        go cli.serveSession(ss)
    }
}

func (cli *Client) retryConnect() {
    t := time.NewTicker(time.Second * 3)

loop:
    for {
        select {
        case <- cli.done:
            break loop
        case <-t.C:
            cli.Lock()
            retrySess := cli.retry
            cli.retry = []session.Session{}
            cli.Unlock()
            for _, ss := range retrySess {
                if !ss.InState(StateReadyToReconnect) {
                    continue
                }
                node, _ := GetMetadata(ss)
                log.Debug("reconnect node: ", node)
                c, err := cli.opts.Transport.Dial(node.Addr())
                if err != nil {
                    log.Debug("reconnect node failed: ", node, " err: ", err)
                    ss.SetState(StateReadyToReconnect)
                    cli.Lock()
                    cli.retry = append(cli.retry, ss)
                    cli.Unlock()
                    continue
                }
                log.Debug("reconnect node success: ", node)
                code := cli.opts.Codec(c)
                ss.SetCodec(code)
                ss.SetState(StateAvailable)
                go cli.serveSession(ss)
            }
        }
    }
}

func (cli *Client) ddd(node registry.Node) error {

    n, ok := node.(*Node)
    if !ok {
        log.Error("registry.Node is not type of *v.Node")
        return errors.New("type mismatch")
    }

    // find session by addr.
    oldss, err := cli.opts.Cache.Addr(node.Addr())
    if err == nil {
        oldss.SetMetadata(node)
        for _, cb := range cli.opts.Callbacks {
            _ = cb.OnUpdateSession(oldss)
        }
        return nil
    }

    // find session by id.
    oldss, err = cli.opts.Cache.Index(n.Id)
    if err == nil {
        oldn, err := GetMetadata(oldss)
        if err == nil {
            if oldn.Addr() != n.Addr() {
                cli.opts.Selector.Mark(oldn, errors.New(""))
                _ = oldss.Close()
                for _, cb := range cli.opts.Callbacks {
                    _ = cb.OnDelSession(oldss)
                }
            } else {
                return nil
            }
        }
    }

    ss := NewSession(node, nil)
    ss.SetState(StateConnectNow)
    cli.now <- ss

    return nil
}

func (cli *Client) newNode(node registry.Node) error {
    return cli.ddd(node)
    //log.Debug("on new node:", node)
    //addr := node.Addr()
    //
    //if ss, err := cli.opts.Cache.Addr(addr); err == nil && ss.Error() == nil {
    //    return nil
    //}
    //
    //cli.Lock()
    //c, err := cli.opts.Transport.Dial(addr)
    //if err != nil {
    //    cli.Unlock()
    //    return err
    //}
    //cli.Unlock()
    //
    //code := cli.opts.Codec(c)
    //
    //sess := NewSession(node, code)
    //
    //for _, cb := range cli.opts.Callbacks {
    //    _ = cb.OnNewSession(sess)
    //}
    //
    ////go cli.serveConn(c)
    //go cli.serveSession(sess)
    //
    //return nil
}

func (cli *Client) newConn(addr string) error {
    cli.RLock()
    _, ok := cli.ccs[addr]
    cli.RUnlock()
    if ok {
        return nil
    }

    cli.Lock()
    c, err := cli.opts.Transport.Dial(addr)
    if err != nil {
        cli.Unlock()
        return err
    }
    cli.Unlock()

    go cli.serveConn(c)

    return nil
}

func NewClient(opts ...client.Option) client.Client {
    var options client.Options
    for _, o := range opts {
        o(&options)
    }

    return &Client{
        opts:options,
        done:make(chan struct{}),
        ccs:make(map[string]codec.Codec),
        now:make(chan session.Session, 128),
    }
}