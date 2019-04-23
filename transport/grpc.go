package transport

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	transpb "github.com/nvanbenschoten/raft-toy/transport/transportpb"
	"go.etcd.io/etcd/raft/raftpb"
	rpc "google.golang.org/grpc"
)

type grpc struct {
	addr    string
	peers   map[uint64]string
	handler RaftHandler

	rpc        *rpc.Server
	dialCtx    context.Context
	dialCancel func()
	clientMu   sync.Mutex
	clientBufs map[uint64]chan *transpb.RaftMsg
}

// NewGRPC creates a new Transport that uses gRPC streams.
func NewGRPC() Transport {
	return new(grpc)
}

func (g *grpc) Init(addr string, peers map[uint64]string) {
	g.addr = addr
	g.peers = peers
	g.clientBufs = make(map[uint64]chan *transpb.RaftMsg)
}

func (g *grpc) Serve(h RaftHandler) {
	g.handler = h
	g.rpc = rpc.NewServer()
	g.dialCtx, g.dialCancel = context.WithCancel(context.Background())
	transpb.RegisterRaftServiceServer(g.rpc, g)

	var lis net.Listener
	for {
		var err error
		lis, err = net.Listen("tcp", g.addr)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "address already in use") {
			log.Printf("waiting to listen %v", err)
			continue
		}
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
		g.handler.HandleMessage(in.Epoch, in.Msg)
	}
}

func (g *grpc) Send(epoch int32, msgs []raftpb.Message) {
	buf := make([]transpb.RaftMsg, len(msgs))
	for i := range buf {
		buf[i].Epoch = epoch
		buf[i].Msg = &msgs[i]
	}
	for i := range buf {
		m := &buf[i]
		g.sendAsync(m.Msg.To, m)
	}
}

func (g *grpc) sendAsync(to uint64, m *transpb.RaftMsg) {
	g.clientMu.Lock()
	buf, ok := g.clientBufs[to]
	g.clientMu.Unlock()
	if ok {
		buf <- m
		return
	}

	g.clientMu.Lock()
	defer g.clientMu.Unlock()
	url, ok := g.peers[to]
	if !ok {
		log.Fatalf("unknown peer %d", to)
	}
	conn, err := rpc.DialContext(g.dialCtx, url, rpc.WithInsecure(), rpc.WithBlock())
	if err != nil {
		switch err {
		case context.Canceled:
			return
		default:
			log.Fatalf("error when dialing %d: %v", to, err)
		}
	}
	c := make(chan *transpb.RaftMsg, 1024)
	g.clientBufs[to] = c
	go g.sender(to, conn, c)
}

func (g *grpc) sender(to uint64, conn *rpc.ClientConn, c <-chan *transpb.RaftMsg) {
	defer conn.Close()
	defer func() {
		g.clientMu.Lock()
		defer g.clientMu.Unlock()
		delete(g.clientBufs, to)
	}()

	client := transpb.NewRaftServiceClient(conn)
	stream, err := client.RaftMessage(g.dialCtx)
	if err != nil {
		switch err {
		case context.Canceled:
			return
		default:
			log.Fatal(err)
		}
	}
	for m := range c {
		if err := stream.Send(m); err != nil {
			switch err {
			case context.Canceled:
				return
			case io.EOF:
				return
			default:
				log.Fatal(err)
			}
		}
	}
}

func (g *grpc) Close() {
	g.rpc.Stop()
	g.dialCancel()
	g.clientMu.Lock()
	defer g.clientMu.Unlock()
	for _, c := range g.clientBufs {
		close(c)
	}
}