package v

type Request struct {
    body *Message
}

func (r *Request) Header() map[string]string {
    return nil
}

func (r *Request) Body() interface{} {
    return r.body
}