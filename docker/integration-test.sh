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

wait_for_cluster() {
  attempts=0
  while [ $attempts -lt 30 ]; do
    if redis-cli -h aster-proxy -p 6381 CLUSTER INFO 2>/dev/null | grep -q "cluster_state:ok"; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done
  echo "Timeout waiting for cluster slot metadata" >&2
  return 1
}

wait_for_cluster

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

redis-cli -h aster-proxy -p 6381 DEL e2e:list >/dev/null
blpop_tmp="$(mktemp)"
timeout 5 redis-cli -h aster-proxy -p 6381 --raw BLPOP e2e:list 5 >"$blpop_tmp" &
blpop_pid=$!
sleep 1
redis-cli -h aster-proxy -p 6381 RPUSH e2e:list value >/dev/null
if ! wait "$blpop_pid"; then
  echo "BLPOP via cluster proxy did not complete" >&2
  cat "$blpop_tmp" >&2 || true
  exit 1
fi
blpop_key="$(sed -n '1p' "$blpop_tmp")"
blpop_value="$(sed -n '2p' "$blpop_tmp")"
rm -f "$blpop_tmp"
if [ "$blpop_key" != "e2e:list" ] || [ "$blpop_value" != "value" ]; then
  echo "Unexpected BLPOP reply: key=$blpop_key value=$blpop_value" >&2
  exit 1
fi

sub_tmp="$(mktemp)"
redis-cli -h aster-proxy -p 6381 --raw SUBSCRIBE e2e.channel >"$sub_tmp" &
sub_pid=$!
sleep 1
redis-cli -h aster-proxy -p 6381 PUBLISH e2e.channel payload >/dev/null
sleep 1
kill "$sub_pid" >/dev/null 2>&1 || true
wait "$sub_pid" >/dev/null 2>&1 || true
if ! grep -q '^subscribe$' "$sub_tmp"; then
  echo "Missing subscribe acknowledgement" >&2
  cat "$sub_tmp" >&2 || true
  rm -f "$sub_tmp"
  exit 1
fi
if ! grep -q '^payload$' "$sub_tmp"; then
  echo "SUBSCRIBE did not forward published payload" >&2
  cat "$sub_tmp" >&2 || true
  rm -f "$sub_tmp"
  exit 1
fi
rm -f "$sub_tmp"

echo "Integration tests passed."
