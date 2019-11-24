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
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/state"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/store"
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
	store := store.NewMemoryStore()
	state := state.NewManager(cluster.Member(), store, registry)
	nopaxos := nopaxos.NewNOPaxos(cluster, state, store, protocolConfig)
	server := &Server{
		nopaxos: nopaxos,
		state:   state,
		store:   store,
		port:    member.Port,
		mu:      sync.Mutex{},
	}
	return server
}

// Server implements the NOPaxos consensus protocol server
type Server struct {
	nopaxos *nopaxos.NOPaxos
	state   state.Manager
	store   store.Store
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
	nopaxos.RegisterNOPaxosServiceServer(s.server, s.nopaxos)
	s.mu.Unlock()
	return s.server.Serve(lis)
}

// Stop shuts down the NOPaxos server
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		s.server.Stop()
	}
	s.state.Close()
	s.store.Close()
	return nil
}
