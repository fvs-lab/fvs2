# Benchmarks

FVS compared with [restic](https://restic.net) and [borg](https://www.borgbackup.org)
on a synthetic game-prefix-like dataset. Reproduce with:

```bash
WITH_MOUNT=1 bench/run.sh
```

## Method

The dataset is 400 MiB of seeded random (incompressible) data: 2000 small
files (1-16 KiB) plus a 256 MiB and a 128 MiB binary. It goes through four
steps; after each one every tool takes a snapshot of the whole tree:

| Step | Change |
|---|---|
| s1 | initial snapshot |
| s2 | overwrite 1 MiB inside the 256 MiB binary (in-place edit) |
| s3 | insert 4 KiB near the start of the 128 MiB binary (shifts everything after it) |
| s4 | duplicate all 2000 small files |

We measure snapshot wall time and total repository size after each step, then
restore the final state and mount it (time from mount start to the first
1 MiB read). restic and borg exclude the `.fvs2` store; borg runs with
`--noflags --noacls --noxattrs`; restic and borg use their default chunkers
and compression.

## Results

Machine: i7-13700H (20 threads), Linux 6.17, tmpfs-backed `/tmp`, Go 1.26.
Versions: fvs2 `24e6700`, restic 0.18.1, borg 1.4.4. Dataset 419,754,290 bytes.

### Snapshot wall time (ms)

| Step | FVS | restic | borg |
|---|---:|---:|---:|
| s1 initial | 1509 | 1787 | 1963 |
| s2 in-place edit | 602 | 1412 | 1441 |
| s3 insert | 319 | 1082 | 1430 |
| s4 duplicates | 325 | 840 | 947 |

### Repository size (bytes)

| After | FVS | restic | borg |
|---|---:|---:|---:|
| s1 | 422,210,933 | 420,188,444 | 420,152,162 |
| s2 (+1 MiB edit) | +2,634,723 | +2,567,475 | +4,727,919 |
| s3 (+4 KiB insert) | +2,482,401 | +1,415,125 | +676,347 |
| s4 (+17 MiB duplicates) | +2,999,441 | +117,145 | +176,175 |

### Restore and mount (ms)

| Operation | FVS | restic | borg |
|---|---:|---:|---:|
| restore final state | 1234 | 1453 | 1294 |
| mount + first 1 MiB read | **88** | 812 | 525 |

## Reading the numbers

- **Snapshots are consistently the fastest** (2-4x): the mtime/size shortcut
  skips unchanged files entirely and only changed content is chunked.
- **Content-defined chunking works**: the 4 KiB insert in s3 stores well under
  1 MiB of new blocks instead of re-storing the shifted 127 MiB tail.
- **Duplicate files cost zero block storage** (identical content hashes to the
  same blocks). The FVS growth visible in s3/s4 is almost entirely the commit
  document: each state stores its full file list as uncompressed JSON, about
  1.5 MiB for this tree, plus one more MiB when 2000 entries are added in s4.
  Compressing or delta-encoding commit metadata is the obvious next win and is
  on the roadmap; restic and borg already pack their metadata.
- **Mount is the differentiator**: first byte in 88 ms. This is the operation
  the others treat as an afterthought and FVS treats as the product: a state
  is browsable instantly, without a restore.
