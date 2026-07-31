#!/usr/bin/env bash
# T101 spike, bet 3 (throwaway): does the shard path report terminal state when
# the pod dies badly? Four kill modes, each landing while a shard is being
# written, plus the blob round-trip the ticket title names.
#
# usage: bet3.sh <blob-dir-on-host>
set -u
BLOBS=${1:?blob dir}
NS=t101
NODE=dagr-t101-control-plane
DAGR=${DAGR:-./target/debug/dagr}

k() { kubectl -n "$NS" "$@"; }

# The container id of a pod's only container, as the node's runtime sees it.
cid() { docker exec "$NODE" sh -c "crictl ps -q --label io.kubernetes.pod.name=$1" 2>/dev/null | head -1; }

mkpod() { # mkpod NAME SHELL-COMMAND [EXTRA-RESOURCE-YAML]
  local name=$1 cmd=$2 extra=${3:-"{}"}
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: { name: $name, namespace: $NS, labels: { spike: t101, bet: "3" } }
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
    - name: w
      image: busybox:1.36
      command: ["sh","-c","$cmd"]
      resources: $extra
      volumeMounts: [{ name: blobs, mountPath: /dagr-blobs }]
  volumes:
    - name: blobs
      hostPath: { path: /dagr-blobs, type: Directory }
EOF
}

report() { # report NAME SHARD
  local name=$1 shard=$2 f="$BLOBS/$2"
  echo "### MODE=$name"
  k get pod "$name" -o jsonpath='POD phase={.status.phase} reason={.status.reason} message={.status.message}{"\n"}' 2>/dev/null
  k get pod "$name" -o jsonpath='CONTAINER state={.status.containerStatuses[0].state} {"\n"}' 2>/dev/null
  if [ -f "$f" ]; then
    local bytes lines lastc
    bytes=$(wc -c <"$f" | tr -d ' ')
    lines=$(grep -c '' "$f" | tr -d ' ')
    lastc=$(tail -c 1 "$f" | od -c | head -1 | awk '{print $2}')
    echo "SHARD bytes=$bytes complete_lines=$lines last_byte=$lastc"
    echo "SHARD tail: $(tail -c 120 "$f")"
    echo -n "FOLD "
    if out=$("$DAGR" fold <"$f" 2>&1); then
      echo "ok exit=0"
      echo "$out" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('FOLD interrupted=%s outcome=%s attempts=%s nodes=%s' % (
    d.get('interrupted'), d.get('overall_outcome'),
    sum(len(n.get('attempts', [])) for n in d.get('nodes', [])), len(d.get('nodes', []))))
print('FOLD trailing_partial_discarded=%s' % d.get('trailing_partial_discarded'))
" 2>/dev/null || echo "FOLD (artifact printed, not JSON-parsed)"
    else
      echo "REFUSED exit=$? detail: $(echo "$out" | head -2 | tr '\n' ' ')"
    fi
  else
    echo "SHARD absent (no file at $f)"
    echo "FOLD n/a — nothing to read"
  fi
  echo
}

echo "=== preparing ==="
k delete pod -l spike=t101 --grace-period=0 --force >/dev/null 2>&1
rm -f "$BLOBS"/shard-*.ndjson
cp "$(dirname "$0")/writer.sh" "$BLOBS/writer.sh"
chmod 755 "$BLOBS/writer.sh"

# --- A: OOM past a memory limit, mid-write ------------------------------
mkpod oom "sh /dagr-blobs/writer.sh shard-oom.ndjson 200 0.25 0 & sleep 3; exec tail /dev/zero" \
  '{ "requests": { "memory": "32Mi" }, "limits": { "memory": "64Mi" } }'

# --- B: kubelet eviction past an ephemeral-storage limit, mid-write -----
mkpod evict "sh /dagr-blobs/writer.sh shard-evict.ndjson 400 0.25 0 & sleep 3; dd if=/dev/zero of=/tmp/fill bs=1M count=200 2>/dev/null; sleep 600" \
  '{ "requests": { "ephemeral-storage": "16Mi" }, "limits": { "ephemeral-storage": "32Mi" } }'

# --- C: SIGKILL from the node, mid-write --------------------------------
mkpod sigkill "sh /dagr-blobs/writer.sh shard-sigkill.ndjson 400 0.25 0 & sleep 600"

# --- D: killed before it writes anything --------------------------------
mkpod prewrite "sh /dagr-blobs/writer.sh shard-prewrite.ndjson 400 0.25 20 & sleep 600"

sleep 6
for p in sigkill prewrite; do
  c=$(cid "$p")
  echo "killing $p container=$c"
  docker exec "$NODE" sh -c "crictl stop -t 0 $c" >/dev/null 2>&1
done

echo "=== waiting for terminal phases (eviction is on the kubelet's 10s cycle) ==="
for _ in $(seq 1 40); do
  pending=$(k get pods -l bet=3 -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c -E 'Running|Pending')
  [ "$pending" = "0" ] && break
  sleep 5
done
k get pods -l bet=3 -o wide 2>&1 | sed 's/^/  /'
echo

for m in oom evict sigkill prewrite; do report "$m" "shard-$m.ndjson"; done
