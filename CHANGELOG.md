
# change log

# 2.0.0

- maintained by clia
- impl password for redis cluster

# 1.3.3

- avoid infinite call when cluster endpoint was down
- enable DNS resolver for backend
- upgrade dependencies to 2021-01-19

# 1.3.2

- replace inotify library with hotwatch
- change setup meta process as one thread
- fixed typo

## 1.3.1

- fixed reload for file rename support
- make clippy happy again XD

## 1.3.0

- refactor aster project structure
- remove fuzz
- remove retry feature
- add refetch 

## 1.2.1

- fixed: cpu 100% cost

## 1.2.0

- add aster reload feature for standalone proxy mode.

## 1.1.8.1

- hot fixed: prevent panic when operate closed socket.

## 1.1.8

- add default tcp connection timeout for backend.

## 1.1.7

- fixed unboot when seed meets down

## 1.1.6

- add thread controller by environment variables.

## 1.1.5

- add retry policy for aster when backend failover

## 1.1.4

- add active triggers of cluster fetcher

## 1.1.3

- fixed itoa bugs
- fixed exists and del bugs

## 1.1.2

- set TCP_NODELAY flags for reply conn
- fixed exists and del bugs

## 1.1.0

- change timer from seconds to microseconds.
- never panic when `cluster.servers` is empty but warn it.
- never panic when `cluster.name` is empty but warn it.

## 1.0.4

- add CLUSTER SLOTS and CLUSTER NODES command support

## 1.0.3

- fixed client hang of redis cluster mode
- add read_from_slave feature

## 1.0.2

- metrics: add aster_front_connection_incr to remeasure count of client connections
- metrics: change aster_front_connection as current client connection gauge.

## 1.0.1

- metrics: add system cpu/memory/thread metric
- chore: compitable with rust stable (remove option_flattening and cell_update feature gate)
