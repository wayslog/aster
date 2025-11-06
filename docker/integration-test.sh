#!/bin/sh
set -euo pipefail

wait_for_command() {
  timeout="$1"
  shift
  start="$(date +%s)"
  while true; do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    now="$(date +%s)"
    if [ $((now - start)) -ge "$timeout" ]; then
      echo "Timeout waiting for command: $*" >&2
      return 1
    fi
    sleep 1
  done
}

STANDALONE_PASS="front-standalone-secret"
CLUSTER_PASS="front-cluster-secret"
CLUSTER_USER="ops"
CLUSTER_USER_PASS="ops-secret"

wait_for_command 60 /bin/sh -c "REDISCLI_AUTH=$STANDALONE_PASS redis-cli -h aster-proxy -p 6380 ping"
wait_for_command 60 /bin/sh -c "REDISCLI_AUTH=$CLUSTER_PASS redis-cli -h aster-proxy -p 6381 ping"

wait_for_cluster() {
  attempts=0
  while [ $attempts -lt 30 ]; do
    if REDISCLI_AUTH="$CLUSTER_PASS" redis-cli -h aster-proxy -p 6381 CLUSTER INFO 2>/dev/null | grep -q "cluster_state:ok"; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done
  echo "Timeout waiting for cluster slot metadata" >&2
  return 1
}

wait_for_cluster

if ! hello_standalone="$(REDISCLI_AUTH="$STANDALONE_PASS" redis-cli -h aster-proxy -p 6380 --raw HELLO 3 AUTH default "$STANDALONE_PASS")"; then
  echo "HELLO 3 handshake against standalone proxy failed" >&2
  exit 1
fi
if ! printf "%s\n" "$hello_standalone" | awk 'BEGIN{found=0} {if(prev=="proto" && $0=="3"){found=1} prev=$0} END{exit(found?0:1)}'; then
  echo "HELLO 3 response from standalone proxy missing proto=3:" >&2
  printf "%s\n" "$hello_standalone" >&2
  exit 1
fi

if ! hello_cluster="$(REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" --raw HELLO 3 AUTH "$CLUSTER_USER" "$CLUSTER_USER_PASS")"; then
  echo "HELLO 3 handshake against cluster proxy failed" >&2
  exit 1
fi
if ! printf "%s\n" "$hello_cluster" | awk 'BEGIN{found=0} {if(prev=="proto" && $0=="3"){found=1} prev=$0} END{exit(found?0:1)}'; then
  echo "HELLO 3 response from cluster proxy missing proto=3:" >&2
  printf "%s\n" "$hello_cluster" >&2
  exit 1
fi

noauth_output="$(redis-cli -h aster-proxy -p 6380 PING 2>&1 || true)"
if echo "$noauth_output" | grep -q "PONG"; then
  echo "Expected standalone proxy to require authentication" >&2
  echo "$noauth_output" >&2
  exit 1
fi
if ! echo "$noauth_output" | grep -qi "NOAUTH"; then
  echo "Unauthenticated standalone request did not return NOAUTH" >&2
  echo "$noauth_output" >&2
  exit 1
fi

REDISCLI_AUTH="$STANDALONE_PASS" redis-cli -h aster-proxy -p 6380 SET standalone foo
value_standalone="$(REDISCLI_AUTH="$STANDALONE_PASS" redis-cli -h aster-proxy -p 6380 GET standalone)"
if [ "$value_standalone" != "foo" ]; then
  echo "Unexpected reply from standalone proxy: $value_standalone" >&2
  exit 1
fi

REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" SET cluster foo
value_cluster="$(REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" GET cluster)"
if [ "$value_cluster" != "foo" ]; then
  echo "Unexpected reply from cluster proxy: $value_cluster" >&2
  exit 1
fi

REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" DEL cluster >/dev/null
REDISCLI_AUTH="$STANDALONE_PASS" redis-cli -h aster-proxy -p 6380 DEL standalone >/dev/null

REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" DEL e2e:list >/dev/null
blpop_tmp="$(mktemp)"
timeout 5 env REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" --raw BLPOP e2e:list 5 >"$blpop_tmp" &
blpop_pid=$!
sleep 1
REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" RPUSH e2e:list value >/dev/null
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
REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" --raw SUBSCRIBE e2e.channel >"$sub_tmp" &
sub_pid=$!
sleep 1
REDISCLI_AUTH="$CLUSTER_USER_PASS" redis-cli -h aster-proxy -p 6381 --user "$CLUSTER_USER" PUBLISH e2e.channel payload >/dev/null
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
