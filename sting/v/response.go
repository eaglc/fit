package v

import "github.com/eaglc/lamer/codec"

type ResponseWriter struct {
    w codec.Codec
}

func (rw *ResponseWriter) Write(v interface{}) error {
    return rw.w.Write(v)
}

func (rw *ResponseWriter) Writer() codec.Codec {
    return rw.w
}