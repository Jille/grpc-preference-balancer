// Package preferencebalancer registers a gRPC balancer that strictly prefers some servers over others.
// If multiple preferred servers are available, they are used in round robin.
// If no preferred server is available, the rest are used in round robin.
//
// Make sure to import this package, for example with:
//
//	import _ "github.com/Jille/grpc-preference-balancer"
//
// This balancer can be used with a service config like:
//
//	{"loadBalancingPolicy": "preference_balancer", "loadBalancingConfig": [{"preference_balancer": {"preferredEndpoints": ["127.0.0.1:1234"]}}]}
//
// A service config can for example be given with [google.golang.org/grpc.WithDefaultServiceConfig] (and enforced with [google.golang.org/grpc.WithDisableServiceConfig] to ignore the resolvers ServiceConfig).
package preferencebalancer

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Jille/errchain"
	"github.com/Jille/genericz"
	"github.com/Jille/genericz/slicez"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const Name = "preference_balancer"

// SimpleServiceConfig is an optional helper function to write a service config for you.
func SimpleServiceConfig(preferredEndpoints []string) (string, error) {
	j, err := json.Marshal(preferredEndpoints)
	if err != nil {
		return "", err
	}
	return `{"loadBalancingPolicy": "preference_balancer", "loadBalancingConfig": [{"preference_balancer": {"preferredEndpoints": ` + string(j) + `}}]}`, nil
}

func init() {
	balancer.Register(builder{})
}

type builder struct{}

var _ balancer.Builder = builder{}
var _ balancer.ConfigParser = builder{}

func (builder) Name() string {
	return Name
}

func (builder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	return &preferenceBalancer{
		cc:       cc,
		subconns: resolver.NewEndpointMap[*subconn](),
	}
}

// LBConfig is the balancer config for the preference balancer.
type LBConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`

	PreferredEndpoints []string `json:"preferredEndpoints,omitempty"`
}

func (builder) ParseConfig(j json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var lbConfig LBConfig
	if err := json.Unmarshal(j, &lbConfig); err != nil {
		return nil, fmt.Errorf("preference_balancer: unable to unmarshal LBConfig: %v", err)
	}
	return &lbConfig, nil
}

type preferenceBalancer struct {
	cc balancer.ClientConn

	// subconns itself is not thread-safe, but only written to from the balancer.Balancer interface methods which is guaranteed to be called from a single goroutine.
	subconns *resolver.EndpointMap[*subconn]

	// mtx guards preferredEndpoints, subconns.state and subconns.isPreferred
	mtx                sync.Mutex
	preferredEndpoints []string
}

var _ balancer.Balancer = &preferenceBalancer{}

func (pb *preferenceBalancer) UpdateClientConnState(ccs balancer.ClientConnState) error {
	cfg, ok := ccs.BalancerConfig.(*LBConfig)
	if !ok {
		return balancer.ErrBadResolverState
	}
	pb.mtx.Lock()
	defer pb.mtx.Unlock()
	pb.preferredEndpoints = cfg.PreferredEndpoints
	if err := pb.syncEndpoints(ccs); err != nil {
		return err
	}
	pb.exitIdleLocked()
	return nil
}

func (pb *preferenceBalancer) syncEndpoints(ccs balancer.ClientConnState) error {
	var failedAdds []error
	endpoints := ccs.ResolverState.Endpoints
	if len(endpoints) == 0 && len(ccs.ResolverState.Addresses) > 0 {
		endpoints = slicez.Map(ccs.ResolverState.Addresses, func(a resolver.Address) resolver.Endpoint {
			return resolver.Endpoint{
				Addresses: []resolver.Address{a},
			}
		})
	}
	for _, e := range endpoints {
		if _, ok := pb.subconns.Get(e); !ok {
			if err := pb.addEndpoint(e); err != nil {
				failedAdds = append(failedAdds, err)
			}
		}
	}
	if pb.subconns.Len()+len(failedAdds) > len(endpoints) {
		wanted := resolver.NewEndpointMap[struct{}]()
		for _, e := range endpoints {
			wanted.Set(e, struct{}{})
		}
		for _, e := range pb.subconns.Keys() {
			if _, ok := wanted.Get(e); !ok {
				pb.forgetEndpoint(e)
			}
		}
	}
	for _, e := range pb.subconns.Keys() {
		s, _ := pb.subconns.Get(e)
		s.isPreferred = false
		for _, a := range e.Addresses {
			if slices.Contains(pb.preferredEndpoints, a.Addr) {
				s.isPreferred = true
				break
			}
		}
	}
	return errchain.Chain(failedAdds...)
}

func (pb *preferenceBalancer) addEndpoint(e resolver.Endpoint) error {
	s := &subconn{pb: pb}
	sc, err := pb.cc.NewSubConn(e.Addresses, balancer.NewSubConnOptions{
		StateListener: s.stateListener,
	})
	if err != nil {
		return err
	}
	s.sc = sc
	pb.subconns.Set(e, s)
	return nil
}

func (pb *preferenceBalancer) forgetEndpoint(e resolver.Endpoint) {
	sc, _ := pb.subconns.Get(e)
	sc.sc.Shutdown()
	pb.subconns.Delete(e)
}

func (preferenceBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	// This should never be called because we provide a StateListener.
}

func (pb *preferenceBalancer) ResolverError(err error) {
	if pb.subconns.Len() == 0 {
		pb.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			Picker:            base.NewErrPicker(err),
		})
	}
}

func (pb *preferenceBalancer) ExitIdle() {
	pb.mtx.Lock()
	defer pb.mtx.Unlock()
	pb.exitIdleLocked()
}

func (pb *preferenceBalancer) exitIdleLocked() {
	fallback := true
	for _, sc := range pb.subconns.Values() {
		if sc.isPreferred {
			switch sc.state {
			case connectivity.Idle:
				sc.sc.Connect()
				fallback = false
			case connectivity.Connecting, connectivity.Ready:
				fallback = false
			}
		}
	}
	if fallback {
		for _, sc := range pb.subconns.Values() {
			if !sc.isPreferred && sc.state == connectivity.Idle {
				sc.sc.Connect()
			}
		}
	}
}

func (pb *preferenceBalancer) Close() {
	for _, sc := range pb.subconns.Values() {
		sc.sc.Shutdown()
	}
	// Break any future calls
	pb.subconns = nil
	pb.cc = nil
}

type subconn struct {
	pb *preferenceBalancer
	sc balancer.SubConn

	// Guarded by pb.mtx
	state       connectivity.State
	isPreferred bool
}

func (s *subconn) stateListener(scs balancer.SubConnState) {
	s.pb.mtx.Lock()
	defer s.pb.mtx.Unlock()
	s.state = scs.ConnectivityState
	s.pb.updateStateLocked()
}

func (pb *preferenceBalancer) updateStateLocked() {
	var preferredReady, othersReady int
	var anyConnecting, anyIdle bool
	for _, s := range pb.subconns.Values() {
		switch s.state {
		case connectivity.Ready:
			if s.isPreferred {
				preferredReady++
			} else {
				othersReady++
			}
		case connectivity.Connecting:
			anyConnecting = true
		case connectivity.Idle:
			anyIdle = true
		}
	}
	if preferredReady > 0 || othersReady > 0 {
		chosen := make([]balancer.SubConn, 0, genericz.Ternary(preferredReady > 0, preferredReady, othersReady))
		for _, s := range pb.subconns.Values() {
			if s.state == connectivity.Ready && (s.isPreferred || preferredReady == 0) {
				chosen = append(chosen, s.sc)
			}
		}
		p := &picker{subconns: chosen}
		p.next.Store(rand.Uint32N(uint32(len(chosen))))
		pb.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Ready,
			Picker:            p,
		})
		return
	}
	st := balancer.State{
		Picker: base.NewErrPicker(balancer.ErrNoSubConnAvailable),
	}
	if anyConnecting {
		st.ConnectivityState = connectivity.Connecting
	} else if anyIdle {
		st.ConnectivityState = connectivity.Idle
	} else {
		st.ConnectivityState = connectivity.TransientFailure
	}
	pb.cc.UpdateState(st)
}

type picker struct {
	subconns []balancer.SubConn
	next     atomic.Uint32
}

var _ balancer.Picker = &picker{}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	// We have a little skew when the uint32 overflows.
	idx := p.next.Add(1) % uint32(len(p.subconns))
	return balancer.PickResult{
		SubConn: p.subconns[idx],
	}, nil
}
