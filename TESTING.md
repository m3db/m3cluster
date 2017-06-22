# m3cluster testing

## Unit tests

m3cluster's per-package unit tests are run as part of the default `make` target.

## Etcd integration tests

In addition to the unit tests, some packages may also include integration tests
to test behavior against docker-based etcd clusters. Management of these
clusters is controlled by the `internal/etcdcluster` package. These integration
tests are run as part of CI builds, but not included in the default `make`
target as users may not have docker installed locally. To run integration tests,
run `make test-integration`.

### Limitations

- Integration tests cannot yet be run in parallel as they use the same
  docker-compose target config.
- The cluster size for the docker etcd integration cluster is fixed at 3.
- Integration tests require docker (and `docker-compose`) to be installed
  (available on travis-ci). Attempts to create in-memory multi-node clusters via
  methods such as [`etcd/embed`](https://godoc.org/github.com/coreos/etcd/embed)
  and [`etcdlabs/cluster`](https://godoc.org/github.com/coreos/etcdlabs/cluster)
  did not fully mirror behavior observed with regard to killing instances when
  using clusters that were spread across multiple servers or multiple processes
  on a single server.
- Integration tests must have a build constraint (`// +build integration`) so
  that they are not run as part of default test target. This is standard for
  `m3db` repos leveraging `m3db/ci-scripts`.
