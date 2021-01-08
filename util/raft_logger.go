package util

import (
	"log"
	"os"

	"go.etcd.io/etcd/raft/v3"
)

// SetRaftLoggingVerbosity sets the logger inside etcd/raft.
func SetRaftLoggingVerbosity(verbose bool) {
	var l raft.Logger
	if verbose {
		stdLogger := log.New(os.Stderr, log.Prefix(), log.Flags())
		def := &raft.DefaultLogger{Logger: stdLogger}
		def.EnableDebug()
		def.EnableTimestamps()
		l = def
	} else {
		l = discardLogger{}
	}
	raft.SetLogger(l)
}

type discardLogger struct{}

func (discardLogger) Debug(v ...interface{})                   {}
func (discardLogger) Debugf(format string, v ...interface{})   {}
func (discardLogger) Error(v ...interface{})                   {}
func (discardLogger) Errorf(format string, v ...interface{})   {}
func (discardLogger) Info(v ...interface{})                    {}
func (discardLogger) Infof(format string, v ...interface{})    {}
func (discardLogger) Warning(v ...interface{})                 {}
func (discardLogger) Warningf(format string, v ...interface{}) {}
func (discardLogger) Fatal(v ...interface{})                   {}
func (discardLogger) Fatalf(format string, v ...interface{})   {}
func (discardLogger) Panic(v ...interface{})                   {}
func (discardLogger) Panicf(format string, v ...interface{})   {}
