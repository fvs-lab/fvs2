package repo

import (
	"bytes"
	"context"
	"testing"
)

func TestWriteFileStreamsCommittedContent(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("payload")
	if err := writer.Add(Entry{Path: "app/value", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	result, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := WriteFile(context.Background(), root, result.StateID, "app/value", &output); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), content) {
		t.Fatalf("content = %q", output.Bytes())
	}
}
