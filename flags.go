package main

import (
	"flag"
	"log"

	"github.com/nvanbenschoten/raft-toy/peer"
	"github.com/nvanbenschoten/raft-toy/util"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/raft"
)

var raftID uint64
var raftPeerIDs []int
var raftPeerAddrs []string
var runLoad bool
var verbose bool

func init() {
	pflag.Uint64Var(&raftID, "id", 1, "raft.Config.ID")
	pflag.IntSliceVar(&raftPeerIDs, "peer-ids", []int{1}, "raft.Peers")
	pflag.StringSliceVar(&raftPeerAddrs, "peer-addrs", []string{"localhost:1234"}, "IP addresses for raft.Peers")
	pflag.BoolVar(&runLoad, "run-load", false, "Propose changes to raft")
	pflag.BoolVar(&verbose, "verbose", false, "Verbose logging")
	pflag.Parse()

	// Add the set of pflags to Go's flag package so that they are usable
	// in tests and benchmarks.
	pflag.CommandLine.VisitAll(func(f *pflag.Flag) {
		flag.String(f.Name, f.DefValue, f.Usage)
	})

	if !verbose {
		util.DisableRaftLogging()
	}
}

func parseFlags() peer.PeerConfig {
	cfg := peer.PeerConfig{ID: raftID}

	self := -1
	cfg.Peers = make([]raft.Peer, len(raftPeerIDs))
	for i, id := range raftPeerIDs {
		cfg.Peers[i] = raft.Peer{ID: uint64(id)}
		if uint64(id) == cfg.ID {
			self = i
		}
	}
	if self == -1 {
		log.Fatalf("missing own ID (%d) in peers (%v)", cfg.ID, cfg.Peers)
	}

	if len(raftPeerAddrs) != len(cfg.Peers) {
		log.Fatalf("missing peer ip mapping")
	}
	cfg.SelfAddr = raftPeerAddrs[self]
	cfg.PeerAddrs = make(map[uint64]string, len(cfg.Peers))
	for i, p := range cfg.Peers {
		cfg.PeerAddrs[p.ID] = raftPeerAddrs[i]
	}
	return cfg
}
