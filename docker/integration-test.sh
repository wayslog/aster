#!/bin/sh
set -euo pipefail

wait_for_port() {
  host="$1"
  port="$2"
  timeout="${3:-30}"
  start="$(date +%s)"
  while true; do
    if redis-cli -h "$host" -p "$port" ping >/dev/null 2>&1; then
      return 0
    fi
    now="$(date +%s)"
    if [ $((now - start)) -ge "$timeout" ]; then
      echo "Timeout waiting for $host:$port" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_port aster-proxy 6380 60
wait_for_port aster-proxy 6381 60

redis-cli -h aster-proxy -p 6380 SET standalone foo
value_standalone="$(redis-cli -h aster-proxy -p 6380 GET standalone)"
if [ "$value_standalone" != "foo" ]; then
  echo "Unexpected reply from standalone proxy: $value_standalone" >&2
  exit 1
fi

redis-cli -h aster-proxy -p 6381 SET cluster foo
value_cluster="$(redis-cli -h aster-proxy -p 6381 GET cluster)"
if [ "$value_cluster" != "foo" ]; then
  echo "Unexpected reply from cluster proxy: $value_cluster" >&2
  exit 1
fi

redis-cli -h aster-proxy -p 6381 DEL cluster >/dev/null
redis-cli -h aster-proxy -p 6380 DEL standalone >/dev/null

echo "Integration tests passed."
