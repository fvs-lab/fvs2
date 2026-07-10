#!/usr/bin/env bash
# Reproducible FVS benchmark against restic and borg.
#
# Scenario: a synthetic game-prefix-like dataset (many small files plus a few
# large binaries) goes through four edits; after each one every tool takes a
# snapshot. We measure snapshot wall time and repository growth, then restore
# and (optionally) mount the final state.
#
# Usage:
#   bench/run.sh [outdir]
#
# Environment:
#   BIG_MB       size of the main binary in MiB (default 256)
#   SMALL_FILES  number of small files (default 2000)
#   WITH_MOUNT=1 also benchmark mount + first read (needs /dev/fuse)
#   FVS2_BIN / FVS2D_BIN  prebuilt binaries (default: go build from this repo)

set -euo pipefail

here="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:-$(mktemp -d /tmp/fvs-bench-XXXX)}"
big_mb="${BIG_MB:-256}"
small_files="${SMALL_FILES:-2000}"
with_mount="${WITH_MOUNT:-0}"

cleanup() {
    mountpoint -q "$out/mnt" 2>/dev/null && fusermount3 -uz "$out/mnt"
    return 0
}
trap cleanup EXIT

mkdir -p "$out"
data="$out/data"
restore="$out/restore"
csv="$out/results.csv"
export RESTIC_PASSWORD=bench
export BORG_PASSPHRASE=

fvs2="${FVS2_BIN:-$out/fvs2}"
fvs2d="${FVS2D_BIN:-$out/fvs2d}"
if [ ! -x "$fvs2" ]; then (cd "$here" && go build -o "$fvs2" ./cmd/fvs2); fi
if [ "$with_mount" = 1 ] && [ ! -x "$fvs2d" ]; then
    (cd "$here/../fvs2d" && go build -o "$fvs2d" ./cmd/fvs2d)
fi

now_ms() { echo $(($(date +%s%N) / 1000000)); }

# repo_size <tool>
repo_size() {
    case "$1" in
    fvs) du -sb "$data/.fvs2" | cut -f1 ;;
    restic) du -sb "$out/restic-repo" | cut -f1 ;;
    borg) du -sb "$out/borg-repo" | cut -f1 ;;
    esac
}

# snapshot <tool> <label>
snapshot() {
    local tool=$1 label=$2 t0 t1 size
    t0=$(now_ms)
    case "$tool" in
    fvs) "$fvs2" --path="$data" commit -m "$label" >/dev/null ;;
    restic) restic -r "$out/restic-repo" backup --quiet --exclude "$data/.fvs2" "$data" ;;
    borg) borg create --noflags --noacls --noxattrs --exclude "$data/.fvs2" "$out/borg-repo::$label" "$data" ;;
    esac
    t1=$(now_ms)
    size=$(repo_size "$tool")
    echo "$tool,$label,$((t1 - t0)),$size" >>"$csv"
}

echo "== dataset: $small_files small files + ${big_mb}MiB + $((big_mb / 2))MiB binaries =="
python3 - "$data" "$big_mb" "$small_files" <<'PY'
import os, random, sys
data, big_mb, small_files = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rng = random.Random(1)
os.makedirs(f"{data}/small", exist_ok=True)
os.makedirs(f"{data}/big", exist_ok=True)
for i in range(small_files):
    size = rng.randint(1024, 16384)
    with open(f"{data}/small/f{i:05d}.dll", "wb") as f:
        f.write(rng.randbytes(size))
with open(f"{data}/big/game.pak", "wb") as f:
    for _ in range(big_mb):
        f.write(rng.randbytes(1 << 20))
with open(f"{data}/big/textures.pak", "wb") as f:
    for _ in range(big_mb // 2):
        f.write(rng.randbytes(1 << 20))
PY
dataset_bytes=$(du -sb "$data" | cut -f1)

echo "== init repositories =="
"$fvs2" --path="$data" init >/dev/null
restic -r "$out/restic-repo" init >/dev/null 2>&1
borg init --encryption=none "$out/borg-repo" 2>/dev/null

echo "tool,step,ms,repo_bytes" >"$csv"

echo "== s1: initial snapshot =="
for tool in fvs restic borg; do snapshot "$tool" s1-initial; done

echo "== s2: overwrite 1MiB inside game.pak (in-place edit) =="
python3 - "$data" <<'PY'
import sys
with open(f"{sys.argv[1]}/big/game.pak", "r+b") as f:
    f.seek(64 << 20)
    f.write(b"\xab" * (1 << 20))
PY
touch "$data/big/game.pak"
for tool in fvs restic borg; do snapshot "$tool" s2-edit; done

echo "== s3: insert 4KiB near the start of textures.pak (shift) =="
python3 - "$data" <<'PY'
import sys
p = f"{sys.argv[1]}/big/textures.pak"
d = open(p, "rb").read()
open(p, "wb").write(d[:1000] + b"\xcd" * 4096 + d[1000:])
PY
for tool in fvs restic borg; do snapshot "$tool" s3-insert; done

echo "== s4: duplicate all small files =="
cp -r "$data/small" "$data/small-copy"
for tool in fvs restic borg; do snapshot "$tool" s4-dup; done

echo "== restore of the final state =="
state=$("$fvs2" --path="$data" states | head -1 | cut -d' ' -f1)
t0=$(now_ms); "$fvs2" --path="$data" restore -s "$state" --to "$restore/fvs" >/dev/null; t1=$(now_ms)
echo "fvs,restore,$((t1 - t0))," >>"$csv"
t0=$(now_ms); restic -r "$out/restic-repo" restore latest --target "$restore/restic" >/dev/null; t1=$(now_ms)
echo "restic,restore,$((t1 - t0))," >>"$csv"
t0=$(now_ms); (cd "$restore" && mkdir -p borg && cd borg && borg extract "$out/borg-repo::s4-dup"); t1=$(now_ms)
echo "borg,restore,$((t1 - t0))," >>"$csv"

if [ "$with_mount" = 1 ]; then
    echo "== mount + first read of game.pak =="
    mnt="$out/mnt"; mkdir -p "$mnt"
    t0=$(now_ms)
    "$fvs2d" -repo "$data" -mount "$mnt" & pid=$!
    until mountpoint -q "$mnt"; do sleep 0.05; done
    head -c 1048576 "$mnt/big/game.pak" >/dev/null
    t1=$(now_ms)
    fusermount3 -u "$mnt"; wait $pid 2>/dev/null || true
    echo "fvs,mount-first-read,$((t1 - t0))," >>"$csv"

    t0=$(now_ms)
    restic -r "$out/restic-repo" mount "$mnt" >/dev/null 2>&1 & pid=$!
    until mountpoint -q "$mnt"; do sleep 0.05; done
    head -c 1048576 "$(find "$mnt/snapshots/latest/" -name game.pak | head -1)" >/dev/null
    t1=$(now_ms)
    fusermount3 -u "$mnt"; wait $pid 2>/dev/null || true
    echo "restic,mount-first-read,$((t1 - t0))," >>"$csv"

    t0=$(now_ms)
    borg mount "$out/borg-repo::s4-dup" "$mnt"
    head -c 1048576 "$(find "$mnt" -name game.pak | head -1)" >/dev/null
    t1=$(now_ms)
    borg umount "$mnt"
    echo "borg,mount-first-read,$((t1 - t0))," >>"$csv"
fi

echo
echo "dataset bytes: $dataset_bytes"
echo "fvs2: $(cd "$here" && git describe --always 2>/dev/null)  restic: $(restic version | cut -d' ' -f2)  borg: $(borg --version | cut -d' ' -f2)"
echo
column -s, -t "$csv"
echo
echo "results: $csv"
