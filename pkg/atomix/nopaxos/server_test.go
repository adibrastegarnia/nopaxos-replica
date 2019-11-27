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
	"github.com/atomix/atomix-api/proto/atomix/counter"
	atomix "github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/registry"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestRaftProtocol(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	pingInterval := 1 * time.Second
	leaderTimeout := 2 * time.Second
	config := &config.ProtocolConfig{PingInterval: &pingInterval, LeaderTimeout: &leaderTimeout}

	members := map[string]atomix.Member{
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
	}

	clusterFoo := atomix.Cluster{
		MemberID: "foo",
		Members:  members,
	}
	serverFoo := NewServer(clusterFoo, registry.Registry, config)

	clusterBar := atomix.Cluster{
		MemberID: "bar",
		Members:  members,
	}
	serverBar := NewServer(clusterBar, registry.Registry, config)

	clusterBaz := atomix.Cluster{
		MemberID: "baz",
		Members:  members,
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

	bytes, _ := proto.Marshal(&counter.SetRequest{
		Value: 1,
	})
	bytes, _ = proto.Marshal(&service.CommandRequest{
		Context: &service.RequestContext{},
		Name:    "set",
		Command: bytes,
	})
	bytes, _ = proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "counter",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Command{
			Command: bytes,
		},
	})

	_ = streamFoo.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 1,
				Timestamp:  time.Now(),
				Value:      bytes,
			},
		},
	})
	_ = streamBar.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 1,
				Timestamp:  time.Now(),
				Value:      bytes,
			},
		},
	})
	_ = streamBaz.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 1,
				Timestamp:  time.Now(),
				Value:      bytes,
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

	// Timeout bar to force a view change
	serverBar.timeout()

	time.Sleep(2 * time.Second)

	_ = streamFoo.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 2,
				Timestamp:  time.Now(),
				Value:      bytes,
			},
		},
	})
	_ = streamBar.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 2,
				Timestamp:  time.Now(),
				Value:      bytes,
			},
		},
	})
	_ = streamBaz.Send(&protocol.ClientMessage{
		Message: &protocol.ClientMessage_Command{
			Command: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: 2,
				Timestamp:  time.Now(),
				Value:      bytes,
			},
		},
	})

	messageFoo, err = streamFoo.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, messageFoo)

	messageBar, err = streamBar.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, messageBar)

	messageBaz, err = streamBaz.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, messageBaz)
}
