package preferencebalancer

import (
	"context"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	_ "github.com/Jille/grpc-multi-resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

type servers struct {
	impls       []testService
	grpcServers []*grpc.Server
	listeners   []net.Listener
	addrs       []string
}

func newServers(n int) *servers {
	return &servers{
		impls:       make([]testService, n),
		grpcServers: make([]*grpc.Server, n),
		listeners:   make([]net.Listener, n),
		addrs:       slices.Repeat([]string{"127.0.0.1:0"}, n),
	}
}

func (s *servers) start(t *testing.T, n int) {
	l, err := net.Listen("tcp", s.addrs[n])
	if err != nil {
		t.Fatal(err)
	}
	s.listeners[n] = l
	s.addrs[n] = l.Addr().String()
	g := grpc.NewServer()
	pb.RegisterTestServiceServer(g, &s.impls[n])
	go g.Serve(l)
	s.grpcServers[n] = g
	t.Cleanup(func() {
		g.Stop()
		l.Close()
	})
}

func (s *servers) stop(t *testing.T, n int) {
	s.grpcServers[n].Stop()
	s.listeners[n].Close()
}

func (s *servers) numHits(n int) int32 {
	return s.impls[n].hits.Load()
}

func (s *servers) resetHits() {
	for i := range s.impls {
		s.impls[i].hits.Store(0)
	}
}

func TestPreferenceBalancer(t *testing.T) {
	servers := newServers(3)
	for i := range 3 {
		servers.start(t, i)
	}

	sc, err := SimpleServiceConfig(servers.addrs[1:2])
	if err != nil {
		t.Fatal(err)
	}

	conn, err := grpc.NewClient("multi:///"+strings.Join(servers.addrs, ","), grpc.WithDisableServiceConfig(), grpc.WithDefaultServiceConfig(sc), grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := pb.NewTestServiceClient(conn)

	// Wait to be connected to servers[1]
	for servers.numHits(1) == 0 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{})
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	servers.resetHits()
	// servers[1] is READY, no future calls should go to the other servers.

	for range 100 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{})
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	if got := servers.numHits(0); got != 0 {
		t.Errorf("Server[0] got %d hits, wanted 0", got)
	}
	if got := servers.numHits(1); got != 100 {
		t.Errorf("Server[1] got %d hits, wanted 100", got)
	}
	if got := servers.numHits(2); got != 0 {
		t.Errorf("Server[2] got %d hits, wanted 0", got)
	}
	if t.Failed() {
		return
	}

	t.Log("Part 2: Stop the preferred server")

	servers.stop(t, 1)

	c.EmptyCall(t.Context(), &pb.Empty{}) // Allow one call to fail for gRPC to realize the server is gone
	servers.resetHits()
	for range 100 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{})
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	if got := servers.numHits(1); got != 0 {
		t.Errorf("Server[1] got %d hits, wanted 0", got)
	}
	if got := servers.numHits(0) + servers.numHits(2); got != 100 {
		t.Errorf("Servers[0+2] got %d hits, wanted 100", got)
	}

	t.Log("Part 3: Restart the preferred server and stop the others")
	servers.start(t, 1)
	servers.stop(t, 0)
	servers.stop(t, 2)

	c.EmptyCall(t.Context(), &pb.Empty{}) // Allow one call to fail for gRPC to realize the server is gone
	servers.resetHits()
	for range 100 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{}, grpc.WaitForReady(true))
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	if got := servers.numHits(0); got != 0 {
		t.Errorf("Server[0] got %d hits, wanted 0", got)
	}
	if got := servers.numHits(1); got != 100 {
		t.Errorf("Server[1] got %d hits, wanted 100", got)
	}
	if got := servers.numHits(2); got != 0 {
		t.Errorf("Server[2] got %d hits, wanted 0", got)
	}

	t.Log("Part 4: Resume a non-preferred server, and keep the preferred server in state Connecting")
	servers.stop(t, 1)
	servers.start(t, 0)

	l, err := net.Listen("tcp", servers.addrs[1])
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	c.EmptyCall(t.Context(), &pb.Empty{}) // Allow one call to fail for gRPC to realize the server is gone
	servers.resetHits()
	for range 100 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{}, grpc.WaitForReady(true))
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	if got := servers.numHits(0); got != 100 {
		t.Errorf("Server[1] got %d hits, wanted 100", got)
	}
	if got := servers.numHits(1); got != 0 {
		t.Errorf("Server[0] got %d hits, wanted 0", got)
	}
}

type testService struct {
	pb.UnimplementedTestServiceServer
	hits atomic.Int32
}

func (s *testService) EmptyCall(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	s.hits.Add(1)
	return req, nil
}
