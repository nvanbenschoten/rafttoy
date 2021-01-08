module github.com/nvanbenschoten/rafttoy

require (
	github.com/certifi/gocertifi v0.0.0-20200211180108-c7c1fbc02894 // indirect
	github.com/cockroachdb/errors v1.8.2 // indirect
	github.com/cockroachdb/pebble v0.0.0-20210108023705-06cbd4dddcb4
	github.com/cockroachdb/redact v1.0.9 // indirect
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.2 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.13.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/spf13/pflag v1.0.5
	github.com/ugorji/go/codec v0.0.0-20190204201341-e444a5086c43 // indirect
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738 // indirect
	go.etcd.io/etcd/raft/v3 v3.0.0-20201109164711-01844fd28560
	go.etcd.io/etcd/server/v3 v3.0.0-20201109164711-01844fd28560
	go.uber.org/zap v1.16.0
	golang.org/x/exp v0.0.0-20201229011636-eab1b5eb1a03 // indirect
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20201006153459-a7d1128ccaa0
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20210105210732-16f7687f5001 // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/genproto v0.0.0-20200903010400-9bfcb5116336 // indirect
	google.golang.org/grpc v1.31.1
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	honnef.co/go/tools v0.0.1-2020.1.5 // indirect
)

go 1.13

// At the time of writing (i.e. as of this version below) the `etcd` repo is in the process of properly introducing
// modules, and as part of that uses an unsatisfiable version for this dependency (v3.0.0-00010101000000-000000000000).
// We just force it to the same SHA as the `go.etcd.io/etcd/raft/v3` module (they live in the same VCS root).
//
// While this is necessary, make sure that the require block above does not diverge.
replace go.etcd.io/etcd/api/v3 => go.etcd.io/etcd/api/v3 v3.0.0-20201109164711-01844fd28560

replace go.etcd.io/etcd/client/v2 => go.etcd.io/etcd/client/v2 v2.0.0-20201109164711-01844fd28560

replace go.etcd.io/etcd/client/v3 => go.etcd.io/etcd/client/v3 v3.0.0-20201109164711-01844fd28560

replace go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201109164711-01844fd28560

replace go.etcd.io/etcd/raft/v3 => go.etcd.io/etcd/raft/v3 v3.0.0-20201109164711-01844fd28560
