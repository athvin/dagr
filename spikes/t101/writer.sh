#!/bin/sh
# T101 spike shard writer (throwaway). Runs INSIDE the pod, writing an
# attempt-scoped event-stream shard to the shared blob volume one record at a
# time, and splitting each record across a gap so that a kill has a high
# probability of landing *mid-record* rather than tidily between records —
# which is the case bet 3 is about.
#
# usage: writer.sh <shard-name> [records] [delay-seconds] [pre-write-sleep]
set -u
B="/dagr-blobs/$1"
N="${2:-200}"
DELAY="${3:-0.25}"
PRE="${4:-0}"
RID="018f4a1e-6c2a-7b3d-9e10-0123456789ab"
SV="dagr.event-stream@1"
W="2026-07-31T00:00:00.000Z"

[ "$PRE" -gt 0 ] && sleep "$PRE"

# A self-describing shard: it opens with its own run-started so a *partial*
# shard is still foldable on its own by the shipped reader. (T106 note.)
printf '{"header":{"captured_environment":{},"data_interval":{"end":"2026-07-31T00:00:00Z","start":"2026-07-30T00:00:00Z"},"fingerprint_algorithm_version":1,"fingerprint_policy":"blake3:2222222222222222222222222222222222222222222222222222222222222222","fingerprint_structural":"blake3:1111111111111111111111111111111111111111111111111111111111111111","parameters":{},"pipeline":"t101-spike","resume_lineage":null,"run_id":"%s"},"kind":"run-started","offset_ns":0,"run_id":"%s","schema_version":"%s","seq":0,"wall":"%s"}\n' \
  "$RID" "$RID" "$SV" "$W" >> "$B"

i=1
while [ "$i" -le "$N" ]; do
  # first half of the record, flushed, then a gap, then the closing half
  printf '{"attempt":1,"kind":"attempt-outcome","message":"ok","node":"n%s",' "$i" >> "$B"
  sleep "$DELAY"
  printf '"offset_ns":%s,"run_id":"%s","schema_version":"%s","seq":%s,"status":"succeeded","wall":"%s"}\n' \
    "$i" "$RID" "$SV" "$i" "$W" >> "$B"
  i=$((i + 1))
done
printf 'WRITER-COMPLETE %s\n' "$N"
