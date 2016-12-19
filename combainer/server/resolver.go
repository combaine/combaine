package server

import (
	"fmt"
	"net"
	"sync"

	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc/naming"
)

type serfResolver struct {
	Serf        *serf.Serf
	serfEventCh chan serf.Event

	mu       sync.Mutex
	watchers []watcher
}

func (r *serfResolver) handleSerfEvents() {
	for e := range r.serfEventCh {
		switch e.EventType() {
		case serf.EventMemberJoin:
			r.addNode(e.(serf.MemberEvent))
		case serf.EventMemberLeave, serf.EventMemberFailed:
			r.deleteNode(e.(serf.MemberEvent))
		case serf.EventMemberUpdate, serf.EventMemberReap,
			serf.EventUser, serf.EventQuery: // Ignore
		}
	}
}

func (r *serfResolver) addNode(event serf.MemberEvent) {
	r.sendUpdates(naming.Add, event.Members)
}

func (r *serfResolver) deleteNode(event serf.MemberEvent) {
	r.sendUpdates(naming.Delete, event.Members)
}

func (r *serfResolver) sendUpdates(op naming.Operation, members []serf.Member) {
	updates := make([]*naming.Update, 0, len(members))
	for _, me := range members {
		updates = append(updates, &naming.Update{
			Op: op,
			// NOTE: next time
			Addr: net.JoinHostPort(me.Addr.String(), "10052"),
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < len(r.watchers); {
		w := r.watchers[i]
		select {
		case w.updateCh <- updates:
			i++
		case <-w.closeCh:
			r.watchers = append(r.watchers[:i], r.watchers[i+1:]...)
		}
	}
}

func (r *serfResolver) Resolve(target string) (naming.Watcher, error) {
	w := watcher{
		updateCh: make(chan []*naming.Update, 1),
		closeCh:  make(chan struct{}),
	}

	var initialUpdates []*naming.Update
	for _, member := range r.Serf.Members() {
		if member.Status == serf.StatusAlive {
			initialUpdates = append(initialUpdates, &naming.Update{
				Op:   naming.Add,
				Addr: net.JoinHostPort(member.Addr.String(), "10052"),
			})
		}
	}
	w.updateCh <- initialUpdates

	r.mu.Lock()
	r.watchers = append(r.watchers, w)
	r.mu.Unlock()

	return w, nil
}

type watcher struct {
	updateCh chan []*naming.Update
	closeCh  chan struct{}
}

func (w watcher) Next() ([]*naming.Update, error) {
	select {
	case updates := <-w.updateCh:
		return updates, nil
	case <-w.closeCh:
		return nil, fmt.Errorf("closed watcher")
	}
}

func (w watcher) Close() {
	close(w.closeCh)
}
