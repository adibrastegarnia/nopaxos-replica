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

package main

import (
	"bytes"
	"fmt"
	"github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/registry"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/nopaxos-replica/pkg/atomix/nopaxos"
	"github.com/atomix/nopaxos-replica/pkg/atomix/nopaxos/config"
	"github.com/gogo/protobuf/jsonpb"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/signal"
)

func main() {
	log.SetLevel(log.TraceLevel)
	log.SetOutput(os.Stdout)

	nodeID := os.Args[1]
	partitionConfig := parsePartitionConfig()
	protocolConfig := parseProtocolConfig()

	members := make(map[string]cluster.Member)
	for _, member := range partitionConfig.Members {
		members[member.ID] = cluster.Member{
			ID:           member.ID,
			Host:         member.Host,
			APIPort:      int(member.APIPort),
			ProtocolPort: int(member.ProtocolPort),
		}
	}

	cluster := cluster.Cluster{
		MemberID: nodeID,
		Members:  members,
	}

	// Start the node. The node will be started in its own goroutine.
	server := nopaxos.NewServer(cluster, registry.Registry, protocolConfig)
	if err := server.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Set the ready file to indicate startup of the protocol is complete.
	ready := util.NewFileReady()
	_ = ready.Set()

	// Wait for an interrupt signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch

	// Stop the node after an interrupt
	if err := server.Stop(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func parsePartitionConfig() *controller.PartitionConfig {
	nodeConfigFile := os.Args[2]
	nodeConfig := &controller.PartitionConfig{}
	nodeBytes, err := ioutil.ReadFile(nodeConfigFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(nodeBytes), nodeConfig); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return nodeConfig
}

func parseProtocolConfig() *config.ProtocolConfig {
	protocolConfigFile := os.Args[3]
	protocolConfig := &config.ProtocolConfig{}
	protocolBytes, err := ioutil.ReadFile(protocolConfigFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(protocolBytes), protocolConfig); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return protocolConfig
}
