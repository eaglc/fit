package v

import (
    "bufio"
    "encoding/binary"
    "errors"
    "fmt"
    "github.com/eaglc/lamer/codec"
    "github.com/eaglc/rtjazz"

    "io"
    "sync"
)

const (
    defaultMessageChanSize = 1000
)

type Codec struct {
    mtx sync.Mutex
    enc codec.Encoder
    dec codec.Decoder
    c io.ReadWriteCloser
}

type Encoder struct {
    w *bufio.Writer
}

type Decoder struct {
    r io.Reader
}

func NewEncoder(w io.Writer) codec.Encoder {
    return &Encoder{
        w:bufio.NewWriter(w),
    }
}

func NewDecoder(r io.Reader) codec.Decoder {
    return &Decoder{
        r:r,
    }
}

func (dec *Decoder) Decode(i interface{}) error {
    defer func() {
        if err := recover(); err != nil {
            // TODO
        }
    }()

    m, ok := i.(*Message)
    if !ok {
        return fmt.Errorf("message expected, but accpted: %v", i)
    }

    if _, err := io.ReadFull(dec.r, m.Header[0:1]); err != nil {
        return err
    }

    if !CheckMagicNumber() {
        return fmt.Errorf("wrong magic number: %v", m.Header[0])
    }

    if _, err := io.ReadFull(dec.r, m.Header[1:]); err != nil {
        return err
    }

    var serviceNameB [2]byte
    if _, err := io.ReadFull(dec.r, serviceNameB[:]); err != nil {
        return err
    }

    serviceNameL := binary.LittleEndian.Uint16(serviceNameB[:])
    if serviceNameL > 0 {
        serviceName := make([]byte, serviceNameL)
        if _, err := io.ReadFull(dec.r, serviceName[:]); err != nil {
            return err
        }
        m.ServiceName = string(serviceName)
    }

    var payloadB [4]byte
    if _, err := io.ReadFull(dec.r, payloadB[:]); err != nil {
        return err
    }

    payloadL := binary.LittleEndian.Uint32(payloadB[:])

    if payloadL > 0 {
        m.Payload = make([]byte, payloadL)
        if _, err := io.ReadFull(dec.r, m.Payload[:]); err != nil {
            return err
        }
    }

    return nil
}

func (enc *Encoder) Encode(v interface{}) error {
    defer func() {
        if err := recover(); err != nil {
            // TODO
        }
    }()

    m, ok := v.(*Message)
    if !ok {
        return errors.New("encode unrecognized message")
    }

    if _, err := enc.w.Write(m.Header[:]); err != nil {
        // TODO
    }

    serviceNameB := make([]byte, 2)
    binary.LittleEndian.PutUint16(serviceNameB, uint16(len(m.ServiceName)))
    if _, err := enc.w.Write(serviceNameB[:]); err != nil {
        return err
    }

    if len(m.ServiceName) > 0 {
        if _, err := enc.w.Write([]byte(m.ServiceName)); err != nil {
            return err
        }
    }

    payloadB := make([]byte, 4)
    binary.LittleEndian.PutUint32(payloadB, uint32(len(m.Payload)))
    if _, err := enc.w.Write(payloadB[:]); err != nil {
        return err
    }

    if len(m.Payload) > 0 {
        if _, err := enc.w.Write(m.Payload); err != nil {
            // TODO
            return err
        }
    }

    return enc.w.Flush()
}

func (code *Codec) Close() error {
    defer func() {
        if err := recover(); err != nil {
            // TODO
        }
    }()

    _ = code.c.Close()

    return nil
}

func (code *Codec) Write(i interface{}) error {
    defer func() {
        if err := recover(); err != nil {
            // TODO
        }
    }()

    code.mtx.Lock()
    defer code.mtx.Unlock()

    return code.enc.Encode(i)
}

func (code *Codec) Read(v interface{}) error {
    if err := code.dec.Decode(v); err != nil {
        return err
    }
    return nil
}

func (code *Codec) String() string {
    return "v.codec"
}

func NewCodec(io io.ReadWriteCloser) codec.Codec {
    return &Codec{
        enc:NewEncoder(io),
        dec:NewDecoder(io),
        c:io,
    }
}
