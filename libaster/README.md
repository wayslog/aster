libaster
=================

libaster is the library for aster proxy.

## change log

## 1.1.0

* change timer from seconds to microseconds.
* never panic when `cluster.servers` is empty but warn it.
* never panic when `cluster.name` is empty but warn it.

## 1.0.4

* add CLUSTER SLOTS and CLUSTER NODES command support

## 1.0.3
* fixed client hang of redis cluster mode
* add read_from_slave feature

## 1.0.2

* metrics: add aster_front_connection_incr to remeasure count of client connections
* metrics: change aster_front_connection as current client connection gauge.

## 1.0.1

* metrics: add system cpu/memory/thread metric
* chore: compitable with rust stable (remove option_flattening and cell_update feature gate)