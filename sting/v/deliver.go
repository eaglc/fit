package v

import (
    "github.com/eaglc/lamer/deliver"
    "sync"
)

var (
    once sync.Once
    inst deliver.Deliver
)

type rtDeliver struct {
    opts deliver.Options
}

func (d *rtDeliver) Init(opts ...deliver.Option) {
    for _, o := range opts {
        o(&d.opts)
    }
}

func (d *rtDeliver) Options() deliver.Options {
    return d.opts
}

// broadcast with a category
func (d *rtDeliver) Broadcast(m interface{}, category interface{}) error{
    nodes, err := d.opts.Cache.Category(category)

    if err != nil {
        return err
    }

    for i := range nodes {
        _ = nodes[i].Codec().Write(m)
    }

    return nil
}

// multicast with multiple indexes
func (d *rtDeliver) Multicast(m interface{}, indexes ...interface{}) error {
    nodes, err := d.opts.Cache.Indexes(indexes...)
    if err != nil {
        return err
    }

    for i := range nodes {
        _ = nodes[i].Codec().Write(m)
    }

    return nil
}

// unicast with one index
func (d *rtDeliver) Unicast(m interface{}, index interface{}) error {
    node, err := d.opts.Cache.Index(index)
    if err != nil {
        return err
    }

    return node.Codec().Write(m)
}

func NewDeliver(opts ...deliver.Option) deliver.Deliver {
    var options deliver.Options
    for _, o := range opts {
        o(&options)
    }

    return &rtDeliver{
        opts:options,
    }
}

func Deliver() deliver.Deliver {
    if inst == nil {
        once.Do(func() {
            inst = NewDeliver()
        })
    }
    return inst
}
