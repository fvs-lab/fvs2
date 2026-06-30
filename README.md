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

Notes:

- `restore --clean` is an exact checkout: files in the destination that are not
  part of the state are removed (the `.fvs2` metadata is always preserved).
- `commit` skips a no-op snapshot when nothing changed; use `--allow-empty` to
  force a state anyway.
- empty files and symlinks are versioned and restored faithfully.

## Mounting a state

Mounting exposes a committed state as a read-only filesystem through `fvs2d`
(the FUSE daemon). Build it with the `fuse3` tag (requires `libfuse3-dev` and
`pkg-config`):

```bash
# in the fvs2d repo
CGO_ENABLED=1 go build -tags fuse3 -o ./bin/fvs2d ./cmd/fvs2d

# mount the current HEAD of a repo (read-only)
./bin/fvs2d -repo /path/to/repo -mount /mnt/state

# or a specific branch / state
./bin/fvs2d -repo /path/to/repo -branch main  -mount /mnt/state
./bin/fvs2d -repo /path/to/repo -state 1f0247 -mount /mnt/state
```

Or let the `fvs2` CLI manage it: `mount` spawns `fvs2d` and waits until it is
ready, `unmount` stops it. Both talk to the daemon over its gRPC control API.

```bash
fvs2 --path /path/to/repo mount --fvs2d ./bin/fvs2d --readonly main /mnt/state
fvs2 unmount /mnt/state
```

The mounted tree mirrors the committed state (nested directories, symlinks,
empty files). Blocks are fetched on demand from the content-addressed store and
verified on read.

## Use it as a Go library

The engine is a small, dependency-light core (`fvs-v2-core`): a BLAKE3
content-addressed block store plus a copy-on-write file abstraction. Import it
directly to build your own snapshot/dedup logic.

## Limitations and roadmap

FVS uses **fixed-size blocks**. That is excellent for in-place edits and appends
(VM images, databases, growing logs), but weak when bytes are inserted or
prepended, since every following block shifts and is re-stored.
Content-defined chunking (CDC), like restic and borg use, is on the roadmap and
is the single biggest lever for dedup quality.

Also planned:

- garbage collection / refcounting in the block store.

Contributions are very welcome, especially on CDC and the mount workflow.

## License

MIT. See [LICENSE](LICENSE).
