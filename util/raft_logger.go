package util

import "go.etcd.io/etcd/raft"

// DisableRaftLogging disables all logging from inside etcd/raft.
func DisableRaftLogging() {
	raft.SetLogger(discardLogger{})
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
