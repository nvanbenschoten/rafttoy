module github.com/nvanbenschoten/rafttoy

go 1.18

require (
	github.com/cockroachdb/pebble v0.0.0-20221005185728-eec7375f9c44
	github.com/gogo/protobuf v1.3.2
	github.com/pkg/errors v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/spf13/pflag v1.0.5
	go.etcd.io/etcd/raft/v3 v3.6.0-alpha.0
	go.etcd.io/etcd/server/v3 v3.5.5
	go.uber.org/zap v1.23.0
	golang.org/x/net v0.0.0-20221004154528-8021a29435af
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/time v0.1.0
	google.golang.org/grpc v1.50.0
)

require (
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cockroachdb/errors v1.9.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/getsentry/sentry-go v0.14.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	go.etcd.io/etcd/api/v3 v3.6.0-alpha.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.0-alpha.0 // indirect
	go.etcd.io/etcd/pkg/v3 v3.6.0-alpha.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/exp v0.0.0-20221006183845-316c7553db56 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220930163606-c98284e70a91 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

replace go.etcd.io/etcd/api/v3 => ../etcd/api

replace go.etcd.io/etcd/server/v3 => ../etcd/server

replace go.etcd.io/etcd/client/pkg/v3 => ../etcd/client/pkg

replace go.etcd.io/etcd/pkg/v3 => ../etcd/pkg

replace go.etcd.io/etcd/raft/v3 => ../etcd/raft
