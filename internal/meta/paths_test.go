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

func TestEntryNamesRejectInvalidUTF8(t *testing.T) {
	if err := ValidateEntryName(string([]byte{0xff})); err == nil {
		t.Fatal("JSON would replace the invalid filename byte")
	}
}
