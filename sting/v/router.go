package v

import (
    "context"
    "github.com/eaglc/lamer/router"
)
import "github.com/eaglc/lamer/handler"

type HandlerWrapper func(ctx context.Context, request *Request, writer *ResponseWriter) error

type Router struct {
    handlers map[uint32] handler.Handler
}

func (r *Router) Handle(pattern interface{}, handler handler.Func) {
    if cmd, ok := pattern.(uint32); ok {
        r.handlers[cmd] = handler
    }
}

func (r *Router) String() string {
    return "v.router"
}

func (r *Router) Serve(ctx context.Context, request interface{}, replay interface{}) error {
    req := request.(*Request)
    m := req.Body().(*Message)
    h := r.handlers[m.Cmd()]
    if h != nil {
        go func() {
            _ = h.Serve(ctx, request, replay)
        }()
    }
    return nil
}

func NewRouter() router.Router {
    return &Router{
        handlers:make(map[uint32] handler.Handler),
    }
}

func Wrapper(h HandlerWrapper) handler.Func {
    return func(ctx context.Context, request interface{}, replay interface{}) error {
        req := request.(*Request)
        rsp := replay.(*ResponseWriter)
        return h(ctx, req, rsp)
    }
}
