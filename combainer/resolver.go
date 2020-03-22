package combainer

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc/resolver"
)

const (
	defaultFreq = time.Minute * 5
	defaultPort = "10052"
)

// NewSerfResolverBuilder creates a new serf resolver builder
func NewSerfResolverBuilder(lookup func() []serf.Member) resolver.Builder {
	return &serfBuilder{freq: defaultFreq, lookup: lookup}
}

type serfBuilder struct {
	freq   time.Duration
	lookup func() []serf.Member
}

// Scheme returns the serf scheme.
func (b *serfBuilder) Scheme() string {
	return "serf"
}

// Build creates and starts a Serf resolver that watches cluster members
func (b *serfBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Resolver{
		freq:   b.freq,
		cc:     cc,
		ctx:    ctx,
		cancel: cancel,
		rn:     make(chan struct{}, 1),
		t:      time.NewTimer(0),
		lookup: b.lookup,
	}
	r.wg.Add(1)
	go r.watcher()
	return r, nil
}

// Resolver is Serf members resolver
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

// ResolveNow invoke an immediate resolution of the target that this serfResolver watches.
func (r *Resolver) ResolveNow(opt resolver.ResolveNowOptions) {
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
		addr := net.JoinHostPort(m.Addr.String(), defaultPort)
		newAddrs = append(newAddrs, resolver.Address{Addr: addr, Metadata: m.Name})
	}
	rand.Shuffle(len(newAddrs), func(i, j int) {
		newAddrs[i], newAddrs[j] = newAddrs[j], newAddrs[i]
	})
	return newAddrs
}

// GenerateAndRegisterSerfResolver generates and registers Serf Resolver.
func GenerateAndRegisterSerfResolver(lookup func() []serf.Member) {
	r := NewSerfResolverBuilder(lookup)
	resolver.Register(r)
}
