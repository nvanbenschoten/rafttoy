package transport

import (
	"fmt"
	"io"
	"log"
	"net"

	transpb "github.com/nvanbenschoten/raft-toy/transport/transportpb"
	"go.etcd.io/etcd/raft/raftpb"
	rpc "google.golang.org/grpc"
)

type grpc struct {
	handler func(*raftpb.Message)
	rpc     *rpc.Server
}

func (g *grpc) Serve(h func(*raftpb.Message)) {
	g.handler = h
	g.rpc = rpc.NewServer()
	transpb.RegisterRaftServiceServer(g.rpc, g)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 12))
	if err != nil {
		log.Fatal(err)
	}
	if err := g.rpc.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func (g *grpc) RaftMessage(stream transpb.RaftService_RaftMessageServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		g.handler(in)
	}
}

func (*grpc) Send(msgs []raftpb.Message) {
	// TODO
}

func (g *grpc) Close() {
	g.rpc.Stop()
}
