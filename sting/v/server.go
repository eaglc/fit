package v

import (
    "fmt"
    "github.com/eaglc/lamer/registry"
    "github.com/eaglc/lamer/router"
    "github.com/eaglc/lamer/server"
    "github.com/eaglc/lamer/transport"
    "net"
    "sync"
    "time"
)

type Server struct {
    sync.RWMutex

    opts server.Options

    done chan struct{}
    running bool
    wait sync.WaitGroup

    node *Node

    listeners []transport.Listener
}

func (s *Server) Init(opts ...server.Option)  {
    s.Lock()
    for _, o := range opts {
        o(&s.opts)
    }

    s.node.Id = s.opts.Id
    s.node.Name = s.opts.Name
    s.node.Address = s.opts.Addr

    // TODO some default options

    s.done = make(chan struct{})
    s.Unlock()
}

func (s *Server) Options() server.Options {
    return s.opts
}

// Must not block
func (s *Server) Start() error {
    s.RLock()
    if s.running {
        s.Unlock()
        return nil
    }
    s.RUnlock()

    l, err := s.opts.Transport.Listen(s.opts.Addr)
    if err != nil {
        return err
    }

    s.listeners = append(s.listeners, l)

    go s.serve(l)

    s.Lock()
    s.running = true
    s.Unlock()

    err = s.opts.Registry.Register(s.node, registry.RegisterTTL(30 * time.Second))

    return err
}

func (s *Server) Stop() error {
    fmt.Println("server.stop")

    _ = s.opts.Registry.Deregister(s.node)
    fmt.Println("server.stop deregistered")

    s.Lock()
    s.running = false
    close(s.done)
    s.Unlock()

    for _, l := range s.listeners {
        l.Close()
    }

    fmt.Println("server.stop to wait")

    s.wait.Wait()
    fmt.Println("server.stop wait end")

    // TODO
    return nil
}

func (s *Server) Router() router.Router {
    return s.opts.Router
}

func (s *Server) String() string {
    return "rtjazz.server"
}

func (s *Server) serveConn(c transport.Conn) {
    nc := s.opts.Codec
    r := s.opts.Router
    codecReal := nc(c)
    ctx := s.opts.Context

loop:
    for {
        select {
        case <-s.done:
            break loop
        default:
        }
        msg := NewMessage("")
        err := codecReal.Read(msg)
        if err != nil {
            break loop
        }

        req := &Request{
            body: msg,
        }

        rw := &ResponseWriter{
            w:codecReal,
        }

        _ = r.Serve(ctx, req, rw)
    }

    _ = codecReal.Close()
}

func (s *Server) serve(l transport.Listener) {
    s.wait.Add(1)
    defer s.wait.Done()
    var tempDelay time.Duration
    for {
        c, err := l.Accept()
        if err != nil {
            select {
            case <-s.done:
                return
            default:
            }
            if ne, ok := err.(net.Error); ok && ne.Temporary() {
                if tempDelay == 0 {
                    tempDelay = 5 * time.Millisecond
                } else {
                    tempDelay *= 2
                }
                if max := 1 * time.Second; tempDelay > max {
                    tempDelay = max
                }
                time.Sleep(tempDelay)
                continue
            }
        }
        tempDelay = 0
        go s.serveConn(c)
    }
}

func NewServer(opts ...server.Option) server.Server {
    var options server.Options
    for _, o := range opts {
        o(&options)
    }

    return &Server{
        opts:options,
        node:&Node{},
    }
}
