//go:build !darwin

package repo

import "github.com/fvs-lab/fvs2/internal/meta"

func preflightNativeNames(string, []meta.FileEntry) error { return nil }
