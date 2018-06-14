package combainer

import (
	"google.golang.org/grpc/resolver"
)

// NewSerfResolverBuilder creates a new serf resolver builder
func NewSerfResolverBuilder() *Resolver {
	return &Resolver{
		scheme: "serf",
	}
}

// Resolver is also a resolver builder.
// It's build() function always returns itself.
type Resolver struct {
	scheme string

	// Fields actually belong to the resolver.
	cc resolver.ClientConn
}

// Build returns itself for Resolver, because it's both a builder and a resolver.
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	r.cc = cc
	return r, nil
}

// Scheme returns the test scheme.
func (r *Resolver) Scheme() string {
	return r.scheme
}

// ResolveNow is a noop for Resolver.
func (*Resolver) ResolveNow(o resolver.ResolveNowOption) {}

// Close is a noop for Resolver.
func (*Resolver) Close() {}

// NewAddress calls cc.NewAddress.
func (r *Resolver) NewAddress(addrs []resolver.Address) {
	r.cc.NewAddress(addrs)
}

// NewServiceConfig calls cc.NewServiceConfig.
func (r *Resolver) NewServiceConfig(sc string) {
	r.cc.NewServiceConfig(sc)
}

// GenerateAndRegisterSerfResolver generates and registers Serf Resolver.
func GenerateAndRegisterSerfResolver() *Resolver {
	r := NewSerfResolverBuilder()
	resolver.Register(r)
	return r
}
