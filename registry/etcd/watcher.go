package etcd

import (
	"context"
	"errors"
	"github.com/eaglc/lamer/registry"
	"go.etcd.io/etcd/clientv3"
)

type etcdWatcher struct {
	ctx context.Context
	key string

	client *clientv3.Client
	w      clientv3.WatchChan
	stop   chan struct{}

	// result chan
	events chan *registry.Event
}

func newEtcdWatcher(r *etcdRegistry, ctx context.Context, key string) (registry.Watcher, error) {

	ew := &etcdWatcher{
		ctx:    ctx,
		key:    key,
		client: r.client,
		stop:   make(chan struct{}),
		events: make(chan *registry.Event),
	}

	go ew.watch()

	return ew, nil
}

func (w *etcdWatcher) Next() (*registry.Event, error) {
	select {
	case <-w.stop:
		return nil, errors.New("watcher stopped")
	case ev := <-w.events:
		return ev, nil
	}
}

func (w *etcdWatcher) Stop() {
	select {
	case <-w.stop:
		return
	default:
		close(w.stop)
	}
}

func (w *etcdWatcher) watch() {
	w.w = w.client.Watch(context.Background(), w.key, clientv3.WithPrefix(), clientv3.WithPrevKV())

loop:
	for {
		select {
		case <-w.stop:
			break loop
		case wc, ok := <-w.w:
			if !ok {
				break loop
			}
			if wc.Err() != nil {
				break loop
			}
			if wc.Canceled {
				break loop
			}
			for _, e := range wc.Events {
				switch e.Type {
				case clientv3.EventTypePut:
					if e.IsCreate() {
						w.events <- &registry.Event{
							Evt:  registry.Create,
							Data: e.Kv.Value,
						}
					} else if e.IsModify() {
						w.events <- &registry.Event{
							Evt:  registry.Update,
							Data: e.Kv.Value,
						}
					}
				case clientv3.EventTypeDelete:
					w.events <- &registry.Event{
						Evt:  registry.Delete,
						Data: e.PrevKv.Value,
					}
				}
			}
		}
	}
}
