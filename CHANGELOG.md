## 0.1.4

* make compatible config with [overlord](https://github.com/bilibili/overlord/cmd/proxy)
* add set_read_write_timeout support for non-windows server
* rename asswecan as aster
* add criterion for benchmark test in stable
* upgrade aster to rust edition 2018

## 0.1.3

* support auto drop/clone counter for notify.
* change atomic usize as Cell<usize>
* fixed ketama hash bugs.

## 0.1.2
* support memcache/redis singleton protocol
* replace all hash map with `hashbrown` and make the qps upper to 167w 
* support with ping and auto rehash.
* compitable with overlord config files.
* fixed some bugs.

## 0.1.1

* support redis cluster (with redirect and moved)
* support real multi thread model
* add timer fetcher for each thread
* qps is reached to 157W (32c Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz x2) and upper 99 line is 1.2 ms in average.
