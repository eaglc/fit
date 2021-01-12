package tcp

import (
    "context"
    "github.com/eaglc/lamer/transport"
    "net"
    "time"
)

type tcpTransport struct {
    opts transport.Options
}

type tcpConn struct {
    opts transport.ConnOptions
    c net.Conn
}

type tcpListener struct {
    l net.Listener
}

func (t *tcpTransport) Init(opts ...transport.Option) error {
    for _,f := range opts {
        f(&t.opts)
    }
    return nil
}

func (t *tcpTransport) Options() transport.Options {
    return t.opts
}

func (t *tcpTransport) Dial(addr string) (transport.Conn, error){
    d := net.Dialer{Timeout: t.opts.DialTimeout}

    ctx := context.Background()
    if t.opts.Context != nil {
        ctx = t.opts.Context
    }

    c, err := d.DialContext(ctx,"tcp", addr)
    if err != nil {
        return nil, err
    }

    return &tcpConn{
        c:c,
    }, nil
}

func (t *tcpTransport) Listen(addr string) (transport.Listener, error) {
    ctx := context.Background()
    if t.opts.Context != nil {
        ctx = t.opts.Context
    }

    var lc net.ListenConfig
    l, err := lc.Listen(ctx, "tcp", addr)
    if err != nil {
        return nil, err
    }

    return &tcpListener{
        l:l,
    }, nil
}

func (t *tcpTransport) String() string {
    return "transport.tcp"
}
////////////////////////////////////////////////////////////////////////////////
func (c *tcpConn) Init(opts ...transport.ConnOption) {
    for _, f := range opts {
        f(&c.opts)
    }

    if c.opts.ReadDeadline.After(time.Now()) {
        _ = c.c.SetReadDeadline(c.opts.ReadDeadline)
    }

    if c.opts.WriteDeadline.After(time.Now()) {
        _ = c.c.SetWriteDeadline(c.opts.WriteDeadline)
    }
}

func (c *tcpConn) Close() error {
    return c.c.Close()
}

func (c *tcpConn) Read(b []byte) (int, error) {
    return c.c.Read(b)
}

func (c *tcpConn) Write(b []byte) (int ,error) {
    return c.c.Write(b)
}

func (c *tcpConn) LocalAddr() string {
    return c.c.LocalAddr().String()
}

func (c *tcpConn) RemoteAddr() string {
    return c.c.RemoteAddr().String()
}

////////////////////////////////////////////////////////////////////////////////
func (l *tcpListener) Accept() (transport.Conn, error) {
    c, err := l.l.Accept()
    if err != nil {
        return nil, err
    }

    return &tcpConn{
        c:c,
    }, nil
}

func (l *tcpListener) Close() error {
    return l.l.Close()
}

func (l *tcpListener) Addr() string {
    return l.l.Addr().String()
}

func NewTransport(opts ...transport.Option) transport.Transport {
    var options transport.Options
    for _, o := range opts {
        o(&options)
    }

    return &tcpTransport{
        opts:options,
    }
}
