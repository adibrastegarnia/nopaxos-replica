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
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/client"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	nopaxos "github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
)

// NewProtocol returns a new NOPaxos Protocol instance
func NewProtocol(config *config.ProtocolConfig) *Protocol {
	return &Protocol{
		config: config,
	}
}

// Protocol is an implementation of the Client interface providing the NOPaxos consensus protocol
type Protocol struct {
	node.Protocol
	config *config.ProtocolConfig
	client *client.Client
	server *Server
}

// Start starts the NOPaxos protocol
func (p *Protocol) Start(cluster cluster.Cluster, registry *node.Registry) error {
	p.client = client.NewClient(cluster, nopaxos.ReadConsistency_SEQUENTIAL)
	p.server = NewServer(cluster, registry, p.config)
	go p.server.Start()
	return p.server.WaitForReady()
}

// Client returns the NOPaxos protocol client
func (p *Protocol) Client() node.Client {
	return p.client
}

// Stop stops the NOPaxos protocol
func (p *Protocol) Stop() error {
	_ = p.client.Close()
	return p.server.Stop()
}
