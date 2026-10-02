// Backend defines the storage contract used by the S3 handler.
package s3

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNoBucket     = errors.New("bucket does not exist")
	ErrNoKey        = errors.New("object does not exist")
	ErrNoUpload     = errors.New("multipart upload does not exist")
	ErrBucketExists = errors.New("bucket already exists")
	ErrNotEmpty     = errors.New("bucket is not empty")
	ErrIntegrity    = errors.New("payload integrity check failed")
	ErrInvalidPart  = errors.New("multipart part is invalid")
	ErrPartOrder    = errors.New("multipart parts are not in ascending order")
	ErrTooLarge     = errors.New("object exceeds configured limit")
)

type Scope struct{ Tenant string }
type Bucket struct {
	Name    string
	Created time.Time
}
type Object struct {
	Key      string
	Size     int64
	ETag     string
	Modified time.Time
	Headers  map[string]string
	Metadata map[string]string
}
type PutRequest struct {
	Bucket, Key       string
	Size              int64 // -1 when unknown
	Headers, Metadata map[string]string
	MD5, SHA256       []byte // optional expected digests
}
type ObjectReader struct {
	Object
	Body io.ReadCloser
}

type CopyRequest struct {
	SourceBucket, SourceKey string
	Destination             PutRequest
	ReplaceMetadata         bool
}

type MultipartUpload struct {
	Bucket, Key, UploadID string
	Created               time.Time
	Headers, Metadata     map[string]string
	Parts                 []Part
}

type Part struct {
	Number   int
	Size     int64
	ETag     string
	Modified time.Time
}

type UploadPartRequest struct {
	Bucket, Key, UploadID string
	Number                int
	Size                  int64
	MD5, SHA256           []byte
}

type CompletedPart struct {
	Number int
	ETag   string
}

type CompleteMultipartRequest struct {
	Bucket, Key, UploadID string
	Parts                 []CompletedPart
	MaxSize               int64
}

// ListRequest is normalized by the HTTP layer. The store returns all live
// objects in S3 key order; S3's V1/V2 pagination and delimiter behavior stay
// at the protocol boundary.
type ListRequest struct {
	Bucket string
}

// MultipartListRequest identifies the last upload returned by a listing.
// Storage resolves its initiation time, including retained terminal records.
type MultipartListRequest struct {
	Bucket, KeyMarker, UploadIDMarker string
}

// Backend operations are scoped to a trusted, server-selected tenant.
// Get returns metadata and bytes from the same committed generation.
// Put consumes and validates the entire body before making it visible.
// The creator owns the backend lifetime; handlers do not close it.
type Backend interface {
	CreateBucket(context.Context, Scope, string) error
	ListBuckets(context.Context, Scope) ([]Bucket, error)
	HeadBucket(context.Context, Scope, string) error
	DeleteBucket(context.Context, Scope, string) error
	PutObject(context.Context, Scope, PutRequest, io.Reader) (Object, error)
	GetObject(context.Context, Scope, string, string) (ObjectReader, error)
	HeadObject(context.Context, Scope, string, string) (Object, error)
	DeleteObject(context.Context, Scope, string, string) error
	ListObjects(context.Context, Scope, ListRequest) ([]Object, error)
	CopyObject(context.Context, Scope, CopyRequest) (Object, error)
	CreateMultipartUpload(context.Context, Scope, PutRequest) (MultipartUpload, error)
	UploadPart(context.Context, Scope, UploadPartRequest, io.Reader) (Part, error)
	ListParts(context.Context, Scope, string, string, string) (MultipartUpload, error)
	ListMultipartUploads(context.Context, Scope, MultipartListRequest) ([]MultipartUpload, error)
	CompleteMultipartUpload(context.Context, Scope, CompleteMultipartRequest) (Object, error)
	AbortMultipartUpload(context.Context, Scope, string, string, string) error
}
