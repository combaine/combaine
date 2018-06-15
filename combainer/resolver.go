package combainer

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc/resolver"
)

const defaultFreq = time.Minute * 5

// NewSerfResolverBuilder creates a new serf resolver builder
func NewSerfResolverBuilder(lookup func() []serf.Member) *Resolver {
	ctx, cancel := context.WithCancel(context.Background())
	return &Resolver{
		freq:   defaultFreq,
		ctx:    ctx,
		cancel: cancel,
		rn:     make(chan struct{}, 1),
		t:      time.NewTimer(0),
		lookup: lookup,
	}
}

// Scheme returns the serf scheme.
func (r *Resolver) Scheme() string {
	return "serf"
}

// Resolver is also a resolver builder.
// It's build() function always returns itself.
type Resolver struct {
	freq time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	// Fields actually belong to the resolver.
	cc resolver.ClientConn
	// rn channel is used by ResolveNow() to force an immediate resolution of the target.
	rn     chan struct{}
	t      *time.Timer
	wg     sync.WaitGroup
	lookup func() []serf.Member
}

// Build returns itself for Resolver, because it's both a builder and a resolver.
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	r.cc = cc
	r.wg.Add(1)
	go r.watcher()
	return r, nil
}

// ResolveNow invoke an immediate resolution of the target that this dnsResolver watches.
func (r *Resolver) ResolveNow(opt resolver.ResolveNowOption) {
	select {
	case r.rn <- struct{}{}:
	default:
	}
}

// Close is a noop for Resolver.
func (r *Resolver) Close() {
	r.cancel()
	r.wg.Wait()
	r.t.Stop()
}

func (r *Resolver) watcher() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.t.C:
		case <-r.rn:
		}
		result := r.resolve()
		// Next resolve should happen after an interval defined by r.freq.
		r.t.Reset(r.freq)
		//r.cc.NewServiceConfig(sc)
		r.cc.NewAddress(result)
	}
}

func (r *Resolver) resolve() []resolver.Address {
	var newAddrs []resolver.Address
	for _, m := range r.lookup() {
		newAddrs = append(newAddrs, resolver.Address{Addr: m.Addr.String()})
	}
	return newAddrs
}

// GenerateAndRegisterSerfResolver generates and registers Serf Resolver.
func GenerateAndRegisterSerfResolver(lookup func() []serf.Member) {
	r := NewSerfResolverBuilder(lookup)
	resolver.Register(r)
}
