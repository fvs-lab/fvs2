package repo

import "fvs2/internal/meta"

// Typed errors, re-exported from the internal metadata package so embedders
// (the mount daemon, the hub) can classify failures without string matching
// and map them to RPC codes. Permission problems are not wrapped: they
// surface as *fs.PathError, so errors.Is(err, fs.ErrPermission) classifies
// them directly.
var (
	// ErrNotInitialized means the directory has no .fvs2 repository.
	ErrNotInitialized = meta.ErrNotInitialized
	// ErrStateNotFound means no state matches the given id, prefix or ref.
	ErrStateNotFound = meta.ErrStateNotFound
	// ErrLockTimeout means the repository advisory lock stayed held past the
	// wait deadline.
	ErrLockTimeout = meta.ErrLockTimeout
	// ErrFormatUnsupported means the repository (or a state) uses an on-disk
	// format newer than this build understands.
	ErrFormatUnsupported = meta.ErrFormatUnsupported
)
