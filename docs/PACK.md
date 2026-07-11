# Pack format

Packs store chunks in immutable files of independent zstd frames. They are a
**storage layer detail**: chunk identity stays the BLAKE3 of the uncompressed
content, and nothing in the logical model (states, trees, manifests, sync)
knows packs exist.

## Design rules

1. **Bounded read cost.** Reading any chunk costs at most one sequential
   frame read plus one zstd decompression of at most the frame target size.
   There are no chains: no frame or chunk depends on another to be read.
2. **Packs are immutable.** A pack is written whole (temp file, fsync,
   rename) and never modified. Compaction writes a new pack and deletes old
   ones afterwards; a reader holding an old pack open keeps reading it
   safely (epoch scheme).
3. **The index is a cache.** Every frame header lists its chunks, so a
   forward scan of the headers (no decompression) rebuilds the index. The
   in-memory index is never a source of truth and there is no index file.
4. **Frame amnesty.** Dead chunks inside packs are reclaimed only by
   compaction, which rewrites the store around the live set. Deleting a
   packed chunk individually is a no-op by design.
5. **One appender per repository.** Pack creation and compaction run under
   the repository lock. Concurrent commits keep writing loose chunks, which
   a later pack absorbs.
6. **Lineage ordering.** The packer receives chunks ordered by file path and
   version (oldest to newest), so consecutive versions of the same file land
   in the same frame and the compression window captures their redundancy.
   Tree and manifest objects are grouped at the tail, where they compress
   against each other.

## Binary layout

All integers are big-endian.

```
pack file  := magic version frame*
magic      := "FVSP"                      4 bytes
version    := 0x01                        1 byte

frame      := fmagic compLen checksum count entry[count] payload
fmagic     := "FRM1"                      4 bytes
compLen    := u32                         compressed payload length
checksum   := u64                         first 8 bytes of BLAKE3(payload-compressed)
count      := u32                         number of chunks in the frame
entry      := id offset length
id         := 32 bytes                    raw BLAKE3 of the uncompressed chunk
offset     := u32                         chunk offset in the uncompressed payload
length     := u32                         chunk length
payload    := zstd frame                  the concatenated chunk bytes
```

The frame target is 256 KiB of uncompressed payload. Entries precede the
payload so a scan indexes the pack by reading headers only.

## Recovery

A truncated or corrupt trailing region stops the scan at the last valid
frame; every chunk read re-verifies its own BLAKE3 after extraction, so a
corrupt frame can fail loudly but never return wrong bytes.

## Interaction with the rest of the system

- `fvs2 pack` compacts a repository into lineage-ordered frames.
- `fvs2 gc` on a packed repository performs the amnesty: it rewrites the
  store around the live set instead of deleting chunks one by one.
- Loose chunks (fresh commits) coexist with packs; reads check loose first.
- Sync, mounts, restore and servers read through the same store API and are
  oblivious to packing.
