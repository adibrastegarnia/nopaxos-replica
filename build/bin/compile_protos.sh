#!/bin/sh

proto_imports="./pkg:./test:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/nopaxos/config,plugins=grpc:pkg pkg/atomix/nopaxos/config/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/nopaxos/protocol,plugins=grpc:pkg pkg/atomix/nopaxos/protocol/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/nopaxos/snapshot,plugins=grpc:pkg pkg/atomix/nopaxos/store/snapshot/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=atomix/nopaxos/roles,plugins=grpc:pkg pkg/atomix/nopaxos/roles/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=test,plugins=grpc:test test/*.proto