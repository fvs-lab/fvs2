# On-disk format

This document specifies the FVS repository format and the guarantees that come
with it. The current format is **3**. Everything under `.fvs2/` is covered;
anything not described here is an implementation detail.

## Layout

```
<repo>/.fvs2/
  config.json         repo configuration (format version, chunking parameters)
  index.json          ordered list of states (id, time, message)
  HEAD.json           current head: a branch name or a detached state id
  refs/heads/<name>   one file per branch, containing a state id
  commits/<id>.json   one document per state
  blocks/<blake3>     content-addressed blocks, named by their BLAKE3-256 hex
  lock                advisory lock file (flock) for mutating operations
```

## Configuration

```json
{
  "format": 2,
  "block_size": 4096,
  "chunking": { "min_size": 4096, "avg_size": 16384, "max_size": 65536 }
}
```

- `format` is the repo format version. Absent means 1. Readers must refuse
  formats newer than they support.
- Format 1 repos chunk files into fixed `block_size` blocks. Format 2 repos use
  content-defined chunking with the `chunking` parameters.

## Chunking (format 2)

Files are split with a FastCDC-style gear rolling hash with normalized
chunking. The 256-entry gear table is generated with splitmix64 from the fixed
seed `0x3779b97f4a7c15f6` and is **part of the format**: it determines every
chunk boundary, so it can never change within format 2. The same applies to a
repo's chunking parameters: changing them re-chunks everything and defeats
dedup, so they are fixed at `init`.

`min == avg == max` degenerates to fixed-size chunking, which is how format 1
repos are processed by the same code path.

## Chunking policy

Any parameter that influences chunk boundaries is part of the format,
versioned, and recorded in the state document (`chunking_policy`). Policy 0
uses the repository parameters for every file. Policy 1 (default for format
3 repositories) sniffs the first 8 KiB of each file: content with no NUL
byte and at least 70% printable bytes chunks with a fine text target
(min 1 KiB, avg 4 KiB, max 16 KiB); everything else keeps the repository
parameters. The rule depends only on content, never on the file name, so
identical content chunks identically everywhere and dedup survives renames.
A file that changes classification between versions re-chunks from scratch
for that transition; determinism is worth that rare, single-file cost.

## Block encoding at rest

Blocks are identified by the BLAKE3 of their **uncompressed** content, but
stored zstd-compressed when that wins (readers sniff the zstd magic, so raw
legacy blocks stay readable and incompressible content stays raw). Ids, dedup
and the remote protocol never see compression: it is purely a storage detail.

## Metadata trees (format 3)

Format 3 moves a state's file list out of the commit document and into
**tree objects**: one content-addressed object per directory, stored in the
same block store as file content. A tree object is a JSON array of entries
(`n` name, `k` kind: `f`/`d`/`l`, `m` mode, `s` size, `t` mtime, `b` blocks,
`z` block sizes, `l` link target, `d` child tree id), sorted by name. An
unchanged directory hashes to the same object across states, so metadata
deduplicates exactly like content, and gc and sync treat tree objects as
ordinary blocks. The commit document keeps only `root_tree`, `file_count` and
`total_size`.

Format 2 repositories (inline `files` in the commit document) remain fully
readable and can still be created with `fvs2 init --format 2` for consumers
that pin older tooling.

## States

A state (commit) document:

```json
{
  "id": "<blake3 of time, message and file list>",
  "format": 2,
  "time_utc": 1783689683,
  "message": "before installer",
  "block_size": 4096,
  "files": [
    {
      "path": "drive_c/game.exe",
      "mode": 493,
      "size": 123456,
      "mod_time": 1783689600,
      "blocks": ["<blake3>", "..."],
      "block_sizes": [16384, 8123]
    },
    { "path": "link", "mode": 511, "mod_time": 1783689600, "link": "target" }
  ]
}
```

- `blocks` lists the chunks of the file in order; `block_sizes` their byte
  lengths (present in format 2, absent in format 1 where offsets derive from
  the fixed `block_size`).
- Symlinks carry `link` and no blocks. Empty files carry an empty block list.
- States are independent: there are no parent links, and any state can be
  deleted (`drop`) without affecting the others.

## Guarantees

- **Integrity on read.** Blocks are content-addressed; every read re-hashes
  and fails with a corruption error instead of returning bad data.
- **Crash safety.** Metadata is written with temp-file + fsync + atomic rename
  + directory fsync; blocks are synced before becoming visible. Operations
  order writes so that a crash at any point leaves at most *orphans* (a block
  or commit document nothing references), never a reference to missing data.
  Orphans are reclaimed by `gc`, which is itself safe to re-run.
- **Concurrency.** Mutating operations (commit, drop, gc) take an exclusive
  advisory lock; readers are lock-free and always observe a consistent state.
- **Compatibility.** Format 2 readers read format 1 repos. Unknown newer
  formats are rejected with an explicit error, never misread. Within format 2,
  chunk boundaries are stable forever (fixed gear table and per-repo
  parameters), so blocks written today keep deduplicating against blocks
  written years from now.

## Non-guarantees

- Commit documents are uncompressed JSON; metadata size grows with file count
  (see the benchmarks). Packing metadata is planned and will be a format bump.
- The store has no refcounting; space is reclaimed only by `gc` (which, on
  packed repositories, rewrites the store around the live set: see
  [docs/PACK.md](PACK.md)).
- Nothing in the format is encrypted or authenticated beyond content hashes.
