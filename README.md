[![Go Reference](https://pkg.go.dev/badge/github.com/Jille/grpc-preference-balancer.svg)](https://pkg.go.dev/github.com/Jille/grpc-preference-balancer)

a gRPC-Go balancer that strictly prefers some servers over others.
If multiple preferred servers are available, they are used in round robin.
If no preferred server is available, the rest are used in round robin.

Make sure to import this package, for example with:

	import _ "github.com/Jille/grpc-preference-balancer"

This balancer can be used with a service config like:

	{"loadBalancingPolicy": "preference_balancer", "loadBalancingConfig": [{"preference_balancer": {"preferredEndpoints": ["127.0.0.1:1234"]}}]}

A service config can for example be given with [google.golang.org/grpc.WithDefaultServiceConfig] (and enforced with grpc.WithDisableServiceConfig to ignore the resolvers ServiceConfig).
