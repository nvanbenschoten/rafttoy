package transport

import (
	"context"
	"io"
	"log"
	"net"
	"sort"
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
	clientBufs map[uint64]chan<- *transpb.RaftMsg
}

// NewGRPC creates a new Transport that uses gRPC streams.
func NewGRPC() Transport {
	return new(grpc)
}

func (g *grpc) Init(addr string, peers map[uint64]string) {
	g.addr = addr
	g.peers = peers
	g.clientBufs = make(map[uint64]chan<- *transpb.RaftMsg)
	g.dialCtx, g.dialCancel = context.WithCancel(context.Background())
	g.rpc = rpc.NewServer()
	transpb.RegisterRaftServiceServer(g.rpc, g)
}

func (g *grpc) Serve(h RaftHandler) {
	g.handler = h

	var lis net.Listener
	for i := 0; ; i++ {
		var err error
		lis, err = net.Listen("tcp", g.addr)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "address already in use") {
			if i > 16 {
				log.Printf("waiting to listen %v", err)
			}
			continue
		}
		log.Fatal(err)
	}

	if err := g.rpc.Serve(lis); err != nil {
		switch err {
		case rpc.ErrServerStopped:
		default:
			log.Fatal(err)
		}
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
		g.handler.HandleMessage(in)
	}
}

func (g *grpc) Send(epoch int32, msgs []raftpb.Message) {
	// Group messages by destination and combine.
	sort.Slice(msgs, func(i, j int) bool {
		return msgs[i].To < msgs[j].To
	})
	st := 0
	to := msgs[0].To
	for i := 1; i < len(msgs); i++ {
		if msgs[i].To != to {
			g.sendAsync(to, &transpb.RaftMsg{
				Epoch: epoch,
				Msgs:  msgs[st:i],
			})
			to = msgs[i].To
			st = i
		}
	}
	g.sendAsync(to, &transpb.RaftMsg{
		Epoch: epoch,
		Msgs:  msgs[st:],
	})
}

func (g *grpc) sendAsync(to uint64, m *transpb.RaftMsg) {
	g.clientMu.Lock()
	buf, ok := g.clientBufs[to]
	g.clientMu.Unlock()
	if ok {
		select {
		case buf <- m:
		case <-g.dialCtx.Done():
		}
		return
	}

	g.clientMu.Lock()
	defer g.clientMu.Unlock()
	url, ok := g.peers[to]
	if !ok {
		log.Fatalf("unknown peer %d", to)
	}
	conn, err := rpc.DialContext(g.dialCtx, url,
		rpc.WithInsecure(), rpc.WithBlock(), rpc.WithInitialWindowSize(1<<20),
	)
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
		// TODO This doesn't seem to help performance.
		// compressMsg(m)
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

func compressMsg(m *transpb.RaftMsg) {
	old := m.Msgs
	m.Msgs = m.Msgs[:0]
	var lastAppResp raftpb.Message
	for i := range old {
		mm := &old[i]
		switch mm.Type {
		case raftpb.MsgApp:
		case raftpb.MsgAppResp:
			// A successful (non-reject) MsgAppResp contains one piece of
			// information: the highest log index. Raft currently queues up
			// one MsgAppResp per incoming MsgApp, and we may process
			// multiple messages in one handleRaftReady call (because
			// multiple messages may arrive while we're blocked syncing to
			// disk). If we get redundant MsgAppResps, drop all but the
			// last (we've seen that too many MsgAppResps can overflow
			// message queues on the receiving side).
			//
			// Note that this reorders the chosen MsgAppResp relative to
			// other messages (including any MsgAppResps with the Reject flag),
			// but raft is fine with this reordering.
			if !mm.Reject {
				if lastAppResp.Type == 0 || mm.Index > lastAppResp.Index {
					lastAppResp = *mm
				}
				continue
			}
		default:
		}
		m.Msgs = append(m.Msgs, *mm)
	}
	if lastAppResp.Type != 0 {
		m.Msgs = append(m.Msgs, lastAppResp)
	}
}

func (g *grpc) Close() {
	g.rpc.Stop()
	g.dialCancel()
	g.clientMu.Lock()
	defer g.clientMu.Unlock()
	for id, c := range g.clientBufs {
		close(c)
		delete(g.clientBufs, id)
	}
}
