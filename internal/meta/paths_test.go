package meta

import (
	"runtime"
	"testing"
)

func TestValidateEntryNameUsesNativeSeparators(t *testing.T) {
	err := ValidateEntryName(`system-systemd\x2dmute\x2dconsole.slice`)
	if runtime.GOOS == "windows" && err == nil {
		t.Fatal("Windows path separator was accepted")
	}
	if runtime.GOOS != "windows" && err != nil {
		t.Fatalf("Unix filename was rejected: %v", err)
	}
}
