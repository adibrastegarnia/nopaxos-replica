// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nopaxos

import (
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	nopaxos "github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"google.golang.org/grpc"
	"net"
	"sync"
)

// NewServer returns a new NOPaxos consensus protocol server
func NewServer(clusterConfig cluster.Cluster, registry *node.Registry, protocolConfig *config.ProtocolConfig) *Server {
	member, ok := clusterConfig.Members[clusterConfig.MemberID]
	if !ok {
		panic("Local member is not present in cluster configuration!")
	}

	cluster := nopaxos.NewCluster(clusterConfig)
	nopaxos := nopaxos.NewNOPaxos(cluster, registry, protocolConfig)
	server := &Server{
		nopaxos: nopaxos,
		port:    member.Port,
		mu:      sync.Mutex{},
	}
	return server
}

// Server implements the NOPaxos consensus protocol server
type Server struct {
	nopaxos *nopaxos.NOPaxos
	server  *grpc.Server
	port    int
	mu      sync.Mutex
}

// Start starts the NOPaxos server
func (s *Server) Start() error {
	s.mu.Lock()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	nopaxos.RegisterClientServiceServer(s.server, s.nopaxos)
	nopaxos.RegisterReplicaServiceServer(s.server, s.nopaxos)
	s.mu.Unlock()
	return s.server.Serve(lis)
}

// timeout times out the server to start a view change
func (s *Server) timeout() {
	s.nopaxos.Timeout()
}

// Stop shuts down the NOPaxos server
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		s.server.Stop()
	}
	return nil
}
