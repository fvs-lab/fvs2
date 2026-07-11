package remote

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	core "fvs-v2-core"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// BlockInfo describes a stored block for garbage collection.
type BlockInfo struct {
	ID      core.BlockID
	Size    int64
	ModTime time.Time
}

// BlockBackend is the pluggable block store: blocks are immutable and
// content-addressed, so any object store fits. States and refs stay on the
// server's local filesystem, where compare-and-swap on refs is cheap.
type BlockBackend interface {
	Has(id core.BlockID) (bool, error)
	Get(id core.BlockID) ([]byte, error)
	Put(data []byte) (core.BlockID, error)
	Delete(id core.BlockID) error
	List() ([]BlockInfo, error)
}

// fsBackend keeps blocks in a directory, content-addressed.
type fsBackend struct {
	dir   string
	store *core.DiskBlockStore
}

func newFSBackend(dir string) (*fsBackend, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	store, err := core.NewDiskBlockStore(dir)
	if err != nil {
		return nil, err
	}
	return &fsBackend{dir: dir, store: store}, nil
}

func (b *fsBackend) Has(id core.BlockID) (bool, error) { return b.store.Has(id) }
func (b *fsBackend) Get(id core.BlockID) ([]byte, error) {
	return b.store.Get(id)
}
func (b *fsBackend) Put(data []byte) (core.BlockID, error) { return b.store.Put(data) }
func (b *fsBackend) Delete(id core.BlockID) error          { return b.store.Delete(id) }

func (b *fsBackend) List() ([]BlockInfo, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, err
	}
	out := make([]BlockInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !hexID.MatchString(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, BlockInfo{ID: core.BlockID(e.Name()), Size: info.Size(), ModTime: info.ModTime()})
	}
	return out, nil
}

// S3Config configures an S3-compatible block backend (AWS S3, MinIO,
// Cloudflare R2, ...).
type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	UseSSL    bool
	Prefix    string // key prefix inside the bucket
}

// s3Backend stores blocks as objects in an S3-compatible bucket.
type s3Backend struct {
	client *minio.Client
	bucket string
	prefix string
}

// NewS3Backend connects to an S3-compatible bucket, creating it if missing.
func NewS3Backend(cfg S3Config) (BlockBackend, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			return nil, err
		}
	}
	prefix := cfg.Prefix
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return &s3Backend{client: client, bucket: cfg.Bucket, prefix: prefix}, nil
}

func (b *s3Backend) key(id core.BlockID) string { return b.prefix + string(id) }

func (b *s3Backend) Has(id core.BlockID) (bool, error) {
	_, err := b.client.StatObject(context.Background(), b.bucket, b.key(id), minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *s3Backend) Get(id core.BlockID) ([]byte, error) {
	obj, err := b.client.GetObject(context.Background(), b.bucket, b.key(id), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, core.ErrBlockNotFound
		}
		return nil, err
	}
	return data, nil
}

func (b *s3Backend) Put(data []byte) (core.BlockID, error) {
	id := core.ContentID(data)
	// Blocks are immutable and content-addressed: an existing key is already
	// the exact bytes, so uploading again is redundant but harmless.
	_, err := b.client.PutObject(context.Background(), b.bucket, b.key(id),
		bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (b *s3Backend) Delete(id core.BlockID) error {
	return b.client.RemoveObject(context.Background(), b.bucket, b.key(id), minio.RemoveObjectOptions{})
}

func (b *s3Backend) List() ([]BlockInfo, error) {
	ctx := context.Background()
	var out []BlockInfo
	for obj := range b.client.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: b.prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		id := core.BlockID(obj.Key[len(b.prefix):])
		if !hexID.MatchString(string(id)) {
			continue
		}
		out = append(out, BlockInfo{ID: id, Size: obj.Size, ModTime: obj.LastModified})
	}
	return out, nil
}

// NewFSBackend opens a filesystem block store rooted at dir, for embedders
// that share one store across several Servers.
func NewFSBackend(dir string) (BlockBackend, error) { return newFSBackend(dir) }

// Compacter is implemented by backends that support pack compaction: the
// caller passes the full live set in lineage order and the backend rewrites
// itself around it (frame amnesty).
type Compacter interface {
	Compact(orderedLive []core.BlockID) error
}

func (b *fsBackend) Compact(orderedLive []core.BlockID) error {
	return b.store.Compact(orderedLive)
}
