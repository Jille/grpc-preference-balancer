package preferencebalancer

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	_ "github.com/Jille/grpc-multi-resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

func TestPreferenceBalancer(t *testing.T) {
	servers := make([]testService, 3)
	addrs := make([]string, 3)
	for i := range 3 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = l.Addr().String()
		s := grpc.NewServer()
		pb.RegisterTestServiceServer(s, &servers[i])
		go s.Serve(l)
		defer s.Stop()
		defer l.Close()
	}

	sc, err := SimpleServiceConfig(addrs[1:2])
	if err != nil {
		t.Fatal(err)
	}

	conn, err := grpc.NewClient("multi:///"+strings.Join(addrs, ","), grpc.WithDisableServiceConfig(), grpc.WithDefaultServiceConfig(sc), grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c := pb.NewTestServiceClient(conn)

	// Wait to be connected to servers[1]
	for servers[1].hits.Load() == 0 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{})
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	for i := range servers {
		servers[i].hits.Store(0)
	}
	// servers[1] is READY, no future calls should go to the other servers.

	for range 100 {
		_, err := c.EmptyCall(t.Context(), &pb.Empty{})
		if err != nil {
			t.Errorf("RPC failed: %v", err)
		}
	}
	if got := servers[0].hits.Load(); got != 0 {
		t.Errorf("Server[1] got %d hits, wanted 0", got)
	}
	if got := servers[1].hits.Load(); got != 100 {
		t.Errorf("Server[0] got %d hits, wanted 100", got)
	}
	if got := servers[2].hits.Load(); got != 0 {
		t.Errorf("Server[2] got %d hits, wanted 0", got)
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
