# rafttoy

`rafttoy` is a playground to experiment with Raft proposal pipeline optimizations
build around the [`etcd/raft`](https://github.com/etcd-io/etcd/tree/master/raft)
library. The project aims to be a speed-of-light measurement tool for a Raft
proposal pipeline implementation similar to the one in [CockroachDB](https://github.com/cockroachdb/cockroach).

## Structure

The project is split into a collection of pluggable components. These components
are driven by a `Peer` object, which performs scheduling and orchestrates their
interactions. Each components is defined as an interface so that different
implementations can be experimented with.

| Component        |  Description                                                                           |  Implementations |
|------------------|----------------------------------------------------------------------------------------|------------------|
| `storage/wal`    | A write-ahead log used to store the Raft log                                           | `mem`, `pebble`, `etcdWal` |
| `storage/engine` | A storage engine that Raft entries are applied to                                      | `mem`, `pebble` |
| `storage`        | A combined Raft log and storage engine. Can be implemented by a single entity or split | `splitStorage(wal,engine)`, `pebble` |
| `transport`      | An interconnect between Raft peers to facilitate message passing                       | `grpc` |
| `pipeline`       | A Raft proposal pipeline loop which handles `raft.Ready` objects                       | `basic`, `parallelAppender`, `asyncApplier(earlyAck, lazyFollower)` |

The components are configured in `newPeer`. In the future they will all be hooked up to flags.

## Metrics

Metrics can be enabled using the `--metrics` flag. This turns on metrics
recording and instructs the process to dump the recorded metrics when it exits.
New metrics can be defined in `metrics/metrics.go`.

## Benchmarking using [Roachprod](https://github.com/cockroachdb/cockroach/tree/master/pkg/cmd/roachprod)

```
# Create a roachprod cluster
> export CLUSTER=$USER-raft
> roachprod create $CLUSTER --nodes=3 --clouds=aws --aws-machine-type-ssd=m5d.4xlarge

# Build the leader and follower binaries for linux. Requires Go 1.12.
> GOOS=linux GOARCH=amd64 make build

# Distribute binaries to the cluster
> roachprod put $CLUSTER:1   rafttoy-leader
> roachprod put $CLUSTER:2,3 rafttoy-follower

# Open three terminals and ssh into each of the machines.
# Start the two follower processes first. These currently need to be restarted
# after each leader process finishes.
CLUSTER:2> ./rafttoy-follower --peers='<vm1 ip>:1234,<vm2 ip>:1235,<vm3 ip>:1236' --id=2 \
    --data-dir=/mnt/data1 --pipeline=parallel-append
CLUSTER:3> ./rafttoy-follower --peers='<vm1 ip>:1234,<vm2 ip>:1235,<vm3 ip>:1236' --id=3 \
    --data-dir=/mnt/data1 --pipeline=parallel-append

# Start the leader process. This can be configured to run a series of benchmarks.
CLUSTER:1> ./rafttoy-leader --peers='<vm1 ip>:1234,<vm2 ip>:1235,<vm3 ip>:1236' --id=1 \
    --data-dir=/mnt/data1 --pipeline=parallel-append \
    --test.bench=BenchmarkRaft/conc=./bytes=256 --test.benchtime=2s --test.count=3

...
BenchmarkRaft/conc=128/bytes=256-16       	  300000	      9607 ns/op	  26.64 MB/s
BenchmarkRaft/conc=256/bytes=256-16       	  300000	      7136 ns/op	  35.87 MB/s
BenchmarkRaft/conc=256/bytes=256-16       	  300000	      7108 ns/op	  36.02 MB/s
BenchmarkRaft/conc=256/bytes=256-16       	  500000	      6839 ns/op	  37.43 MB/s
BenchmarkRaft/conc=512/bytes=256-16       	  500000	      6149 ns/op	  41.63 MB/s
BenchmarkRaft/conc=512/bytes=256-16       	  500000	      6341 ns/op	  40.37 MB/s
BenchmarkRaft/conc=512/bytes=256-16       	  300000	      6691 ns/op	  38.26 MB/s
BenchmarkRaft/conc=1024/bytes=256-16      	  500000	      5575 ns/op	  45.91 MB/s
BenchmarkRaft/conc=1024/bytes=256-16      	  500000	      5571 ns/op	  45.95 MB/s
BenchmarkRaft/conc=1024/bytes=256-16      	  500000	      5369 ns/op	  47.68 MB/s
BenchmarkRaft/conc=2048/bytes=256-16      	  500000	      5448 ns/op	  46.99 MB/s
...
```
