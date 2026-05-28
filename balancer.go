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
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

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

	// mtx guards everything below and all properties of subconn.
	mtx                    sync.Mutex
	preferredEndpoints     []string
	resolverError          error
	lastPreferredConnError error
	lastConnError          error
	connectingDeadline     time.Time
	moreConnectionsTimer   *time.Timer
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
	pb.updateStateLocked()
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
		for e, sc := range pb.subconns.All() {
			if _, ok := wanted.Get(e); !ok {
				sc.sc.Shutdown()
				pb.subconns.Delete(e)
			}
		}
	}

	for e, s := range pb.subconns.All() {
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

func (preferenceBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	// This should never be called because we provide a StateListener.
}

func (pb *preferenceBalancer) ResolverError(err error) {
	pb.mtx.Lock()
	defer pb.mtx.Unlock()
	pb.resolverError = err
	pb.updateStateLocked()
}

// ExitIdle is a noop because we always want to be connected.
func (pb *preferenceBalancer) ExitIdle() {
}

func (pb *preferenceBalancer) Close() {
	pb.mtx.Lock()
	defer pb.mtx.Unlock()
	pb.clearConnectionDeadline()
	for _, sc := range pb.subconns.Values() {
		sc.sc.Shutdown()
	}
	pb.cc = nil
}

type subconn struct {
	pb *preferenceBalancer
	sc balancer.SubConn

	// Guarded by pb.mtx
	state                       connectivity.State
	isPreferred                 bool
	lastConnectionAttemptFailed bool
}

func (s *subconn) stateListener(scs balancer.SubConnState) {
	s.pb.mtx.Lock()
	defer s.pb.mtx.Unlock()
	s.state = scs.ConnectivityState
	switch scs.ConnectivityState {
	case connectivity.Ready:
		s.lastConnectionAttemptFailed = false
	case connectivity.TransientFailure:
		s.lastConnectionAttemptFailed = true
		if s.isPreferred {
			s.pb.lastPreferredConnError = scs.ConnectionError
		}
		s.pb.lastConnError = scs.ConnectionError
	}
	s.pb.updateStateLocked()
}

func (pb *preferenceBalancer) updateStateLocked() {
	if pb.cc == nil {
		// pb.Close() was called.
		return
	}
	if pb.subconns.Len() == 0 {
		if pb.resolverError != nil {
			pb.cc.UpdateState(balancer.State{
				ConnectivityState: connectivity.TransientFailure,
				Picker:            base.NewErrPicker(pb.resolverError),
			})
		}
		pb.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Connecting,
			Picker:            base.NewErrPicker(balancer.ErrNoSubConnAvailable),
		})
		return
	}
	var hopefulConnectingAttempt bool
	var anyPreferredConnecting bool
	var preferredReady, othersReady int
	for _, s := range pb.subconns.All() {
		switch s.state {
		case connectivity.Ready:
			if s.isPreferred {
				preferredReady++
			} else {
				othersReady++
			}
		case connectivity.Connecting:
			if s.isPreferred {
				anyPreferredConnecting = true
			}
			if !s.lastConnectionAttemptFailed {
				hopefulConnectingAttempt = true
			}
		case connectivity.TransientFailure:
		case connectivity.Idle:
			if s.isPreferred {
				s.sc.Connect()
				anyPreferredConnecting = true
				if !s.lastConnectionAttemptFailed {
					hopefulConnectingAttempt = true
				}
				if pb.connectingDeadline.IsZero() {
					pb.connectingDeadline = time.Now().Add(250 * time.Millisecond)
					pb.moreConnectionsTimer = time.AfterFunc(250*time.Millisecond, pb.connectionTimerTrigger)
				}
			}
		}
	}
	var connectToNonPreferred bool
	if preferredReady == 0 && !hopefulConnectingAttempt {
		connectToNonPreferred = true
	}
	if !anyPreferredConnecting {
		pb.clearConnectionDeadline()
	} else if preferredReady == 0 && !pb.connectingDeadline.IsZero() && time.Since(pb.connectingDeadline) >= 0 {
		connectToNonPreferred = true
		pb.clearConnectionDeadline()
	}
	if connectToNonPreferred {
		for _, s := range pb.subconns.All() {
			if !s.isPreferred && s.state == connectivity.Idle {
				s.sc.Connect()
				if !s.lastConnectionAttemptFailed {
					hopefulConnectingAttempt = true
				}
			}
		}
	}
	if preferredReady > 0 || othersReady > 0 {
		chosen := make([]balancer.SubConn, 0, genericz.Ternary(preferredReady > 0, preferredReady, othersReady))
		for _, s := range pb.subconns.All() {
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
	if hopefulConnectingAttempt {
		pb.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Connecting,
			Picker:            base.NewErrPicker(balancer.ErrNoSubConnAvailable),
		})
	} else {
		err := pb.lastPreferredConnError
		if err == nil {
			err = pb.lastConnError
		}
		if err == nil {
			err = errors.New("balancer is not ready")
		}
		pb.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			Picker:            base.NewErrPicker(err),
		})
	}
}

func (pb *preferenceBalancer) connectionTimerTrigger() {
	pb.mtx.Lock()
	defer pb.mtx.Unlock()
	pb.moreConnectionsTimer = nil
	pb.updateStateLocked()
}

func (pb *preferenceBalancer) clearConnectionDeadline() {
	pb.connectingDeadline = time.Time{}
	if pb.moreConnectionsTimer != nil {
		pb.moreConnectionsTimer.Stop()
		pb.moreConnectionsTimer = nil
	}
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
