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
	"context"
	atomix "github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/registry"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestRaftProtocol(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	leaderTimeout := 10 * time.Second
	config := &config.ProtocolConfig{LeaderTimeout: &leaderTimeout}

	clusterFoo := atomix.Cluster{
		MemberID: "foo",
		Members: map[string]atomix.Member{
			"foo": {
				ID:   "foo",
				Host: "localhost",
				Port: 5678,
			},
			"bar": {
				ID:   "bar",
				Host: "localhost",
				Port: 5679,
			},
			"baz": {
				ID:   "baz",
				Host: "localhost",
				Port: 5680,
			},
		},
	}
	serverFoo := NewServer(clusterFoo, registry.Registry, config)

	clusterBar := atomix.Cluster{
		MemberID: "bar",
		Members: map[string]atomix.Member{
			"foo": {
				ID:   "foo",
				Host: "localhost",
				Port: 5678,
			},
			"bar": {
				ID:   "bar",
				Host: "localhost",
				Port: 5679,
			},
			"baz": {
				ID:   "baz",
				Host: "localhost",
				Port: 5680,
			},
		},
	}
	serverBar := NewServer(clusterBar, registry.Registry, config)

	clusterBaz := atomix.Cluster{
		MemberID: "baz",
		Members: map[string]atomix.Member{
			"foo": {
				ID:   "foo",
				Host: "localhost",
				Port: 5678,
			},
			"bar": {
				ID:   "bar",
				Host: "localhost",
				Port: 5679,
			},
			"baz": {
				ID:   "baz",
				Host: "localhost",
				Port: 5680,
			},
		},
	}
	serverBaz := NewServer(clusterBaz, registry.Registry, config)

	go serverFoo.Start()
	go serverBar.Start()
	go serverBaz.Start()

	time.Sleep(5 * time.Second)

	connFoo, err := grpc.Dial("localhost:5678", grpc.WithInsecure())
	assert.NoError(t, err)
	clientFoo := protocol.NewClientServiceClient(connFoo)
	streamFoo, err := clientFoo.ClientStream(context.Background())
	assert.NoError(t, err)

	connBar, err := grpc.Dial("localhost:5679", grpc.WithInsecure())
	assert.NoError(t, err)
	clientBar := protocol.NewClientServiceClient(connBar)
	streamBar, err := clientBar.ClientStream(context.Background())
	assert.NoError(t, err)

	connBaz, err := grpc.Dial("localhost:5680", grpc.WithInsecure())
	assert.NoError(t, err)
	clientBaz := protocol.NewClientServiceClient(connBaz)
	streamBaz, err := clientBaz.ClientStream(context.Background())
	assert.NoError(t, err)

	streamFoo.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 1,
				Timestamp:  time.Now(),
				Value:      []byte{},
			},
		},
	})
	streamBar.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 1,
				Timestamp:  time.Now(),
				Value:      []byte{},
			},
		},
	})
	streamBaz.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 1,
				Timestamp:  time.Now(),
				Value:      []byte{},
			},
		},
	})

	messageFoo, err := streamFoo.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, messageFoo)

	messageBar, err := streamBar.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, messageBar)

	messageBaz, err := streamBaz.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, messageBaz)

	time.Sleep(5 * time.Second)
}
