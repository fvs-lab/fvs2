# FVS

**Fused Versioned Storage.** Time-travel for big data: snapshot, dedupe, mount.

[![Go](https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

FVS gives you git-style snapshots for large, mutable data (datasets, VM and disk
images, ML artifacts, databases, environments). It is content-addressed and
deduplicated at the block level, so committing a 40 GB directory twice costs
40 GB plus the few blocks that actually changed. And any past state can be
**mounted as a read-only filesystem**, so you browse history instead of waiting
for a restore. No special filesystem, no server, a single Go binary.

> **Why "Fused"?** Identical blocks are *fused* into one (deduplication), and any
> committed state can be *fused* into your filesystem via FUSE (`mount`), with no
> restore step. The name is the pitch.

![FVS demo](docs/demo.gif)

## Why it exists

Versioning big, mutable data is a gap:

- **git / git-LFS** store whole-file blobs. A large file that changes a little
  bloats the repo; LFS is a server-bound bandaid.
- **restic / borg** are backup tools (snapshot and restore), not a
  commit/branch/checkout/mount workflow.
- **ZFS / btrfs snapshots** are great, but only if you already run that exact
  filesystem.

FVS sits in the empty quadrant: VCS-style states and branches, block-level
deduplication, a mountable view, on any filesystem, in userspace.

## How it compares

|                                   | git / LFS | restic / borg | ZFS / btrfs | **FVS** |
|-----------------------------------|:---------:|:-------------:|:-----------:|:-------:|
| VCS semantics (commit/branch/checkout) | ✅ | ❌ | ❌ | ✅ |
| Block-level dedup on large files  | ❌ | ✅ | ✅ | ✅ |
| **Mount a version as a filesystem** | ❌ | ❌ | ✅ | ✅ |
| Works on any filesystem           | ✅ | ✅ | ❌ | ✅ |
| No server, single binary          | ❌ (LFS) | ✅ | ✅ | ✅ |
| Integrity verified on read        | ✅ | ✅ | ✅ | ✅ |

## Deduplication in practice

- 50 identical files commit to **1** stored block.
- A 4-byte change inside a 20 MB file adds **0** extra bytes to storage (only the
  touched block is rewritten, and it hashed to the same content here).
- Appending to the end of a file stores only the new tail blocks.

Every block is named by its BLAKE3 hash, so reads re-verify content and surface
corruption or bit-rot instead of silently returning bad data.

## Install

```bash
# CLI (static, CGO-free)
CGO_ENABLED=0 go build -o ./bin/fvs2 ./cmd/fvs2
./bin/fvs2 --help
```

The optional mount daemon (`fvs2d`) lives in its own repo and needs FUSE; see
[Mounting a state](#mounting-a-state).

## Quickstart

```bash
mkdir repo && cd repo
fvs2 init

# put a large dataset / disk image / model here, then snapshot it
fvs2 commit -m "week 1"

# change a few megabytes, snapshot again: only changed blocks are stored
fvs2 commit -m "week 2"

fvs2 states          # list saved states
fvs2 status          # HEAD, active branch, dirty state

# bring an old state back into the working dir (exact checkout)
fvs2 restore -s <state-id> --clean
```

## Commands

Global flag: `--path` sets the repo root (default: current directory).

| Command | What it does | Useful flags |
|---|---|---|
| `init` | initialize a directory for versioning | `--block-size` |
| `commit` | create a new state (snapshot) | `-m/--message`, `--allow-empty`, `-v` |
| `states` | list saved states | |
| `restore` | restore a state into a directory | `-s/--state`, `--to`, `--clean`, `--reset` |
| `status` | show HEAD, branch, and dirty state | `--check-dirty` |
| `branch` | manage branches | `list`, `create <name>`, `delete <name>` |
| `checkout` | move HEAD to a branch or commit | |
| `drop` | delete a state | |
| `gc` | remove unreferenced blocks and orphan states | `--dry-run` |

Notes:

- `restore --clean` is an exact checkout: files in the destination that are not
  part of the state are removed (the `.fvs2` metadata is always preserved).
- `commit` skips a no-op snapshot when nothing changed; use `--allow-empty` to
  force a state anyway.
- empty files and symlinks are versioned and restored faithfully.

## Mounting a state

Mounting exposes committed states through the separate `fvs2d` FUSE daemon:

```bash
# in the fvs2d repo
go build -o ./bin/fvs2d ./cmd/fvs2d

# mount the current HEAD of a repo (read-only)
./bin/fvs2d -repo /path/to/repo -mount /mnt/state

# or a specific branch / state
./bin/fvs2d -repo /path/to/repo -branch main  -mount /mnt/state
./bin/fvs2d -repo /path/to/repo -state 1f0247 -mount /mnt/state
```

For programmatic mount lifecycle, run `fvs2d` as a persistent gRPC manager;
`fvs2` now handles repository operations only.

The mounted tree mirrors the committed state (nested directories, symlinks,
empty files). Blocks are fetched on demand from the content-addressed store and
verified on read.

## Use it as a Go library

The engine is a small, dependency-light core (`fvs-v2-core`): a BLAKE3
content-addressed block store plus a copy-on-write file abstraction. Import it
directly to build your own snapshot/dedup logic.

## Format and performance

FVS uses **content-defined chunking** (FastCDC-style), so inserts and shifted
data still deduplicate; unreferenced blocks are reclaimed with `gc`. The
on-disk format, its guarantees (integrity on read, crash safety, stable chunk
boundaries) and the compatibility policy are specified in
[docs/FORMAT.md](docs/FORMAT.md).

Numbers against restic and borg, with the reproducible harness in `bench/`,
are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md); the short version: snapshots
2-4x faster, comparable dedup, and a mounted state serves its first read in
under 100 ms.

## Limitations and roadmap

- commit metadata is uncompressed JSON and grows with file count; packing it
  is the next format change.
- no remote yet: repositories are local (push/pull is the next milestone).

Contributions are very welcome.

## License

MIT. See [LICENSE](LICENSE).
