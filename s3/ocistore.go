// OCIStore maps S3 storage operations onto an injected OCI registry.
package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ohseeeye/oci"
	"github.com/ohseeeye/oci/pkg/ocidigest"
)

const annotation = "io.essthree."
const emptyType = "application/vnd.oci.empty.v1+json"

type OCIOptions struct{ Namespace string }
type OCIStore struct {
	registry  oci.Registry
	namespace string
	mu        sync.RWMutex
	listCache map[string][]Object
}

var _ Backend = (*OCIStore)(nil)

func newOCIStore(registry oci.Registry, opts OCIOptions) (*OCIStore, error) {
	if registry == nil {
		return nil, errors.New("nil OCI registry")
	}
	if opts.Namespace == "" {
		opts.Namespace = "essthree"
	}
	if !safeName(opts.Namespace) {
		return nil, errors.New("namespace must contain lowercase letters, digits or hyphens")
	}
	s := &OCIStore{registry: registry, namespace: opts.Namespace, listCache: make(map[string][]Object)}
	// The HTTP server currently has one trusted scope. Recover it before serving
	// requests; other scopes are reconciled before mutations through writeBucket.
	if err := s.recoverScope(context.Background(), Scope{Tenant: "local"}); err != nil {
		return nil, fmt.Errorf("recover multipart uploads: %w", err)
	}
	return s, nil
}
func safeName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return s[0] != '-' && s[len(s)-1] != '-'
}
func hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func id() string           { return rand.Text() }
func (s *OCIStore) catalog(scope Scope) string {
	return s.namespace + "/t-" + hash(scope.Tenant) + "/catalog"
}
func (s *OCIStore) read(ctx context.Context, repo, tag string) (oci.IndexOrManifest, error) {
	r, err := s.registry.GetTag(ctx, repo, tag)
	if err != nil {
		return oci.IndexOrManifest{}, err
	}
	defer r.Close()
	var m oci.IndexOrManifest
	if r.Descriptor().Size > 4<<20 {
		return m, errors.New("manifest exceeds limit")
	}
	err = json.NewDecoder(io.LimitReader(r, (4<<20)+1)).Decode(&m)
	if err == nil && m.Annotations[annotation+"schema"] != "1" {
		err = errors.New("unsupported storage schema")
	}
	return m, err
}
func absent(err error) bool {
	return errors.Is(err, oci.ErrNameUnknown) || errors.Is(err, oci.ErrManifestUnknown)
}
func (s *OCIStore) publish(ctx context.Context, repo, tag, kind string, a map[string]string, layers []oci.Descriptor) error {
	empty := []byte("{}")
	d := oci.Descriptor{MediaType: emptyType, Digest: ocidigest.FromBytes(empty), Size: 2}
	if _, err := s.registry.PushBlob(ctx, repo, d, bytes.NewReader(empty)); err != nil {
		return err
	}
	a[annotation+"schema"] = "1"
	a[annotation+"kind"] = kind
	a[annotation+"generation"] = id()
	if len(layers) == 0 {
		layers = []oci.Descriptor{d}
	}
	m := oci.IndexOrManifest{SchemaVersion: 2, MediaType: oci.MediaTypeImageManifest, ArtifactType: "application/vnd.essthree." + kind + ".v1", Config: &d, Layers: layers, Annotations: a}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = s.registry.PushManifest(ctx, repo, raw, oci.MediaTypeImageManifest, &oci.PushManifestParameters{Tags: []string{tag}})
	return err
}
func (s *OCIStore) bucket(ctx context.Context, scope Scope, name string) (oci.IndexOrManifest, error) {
	m, err := s.read(ctx, s.catalog(scope), "bucket-"+hash(name))
	if absent(err) {
		return m, ErrNoBucket
	}
	if err != nil {
		return m, err
	}
	if m.Annotations[annotation+"name"] != name {
		return m, errors.New("bucket record key mismatch")
	}
	if m.Annotations[annotation+"kind"] == "deleted-bucket" {
		return m, ErrNoBucket
	}
	if m.Annotations[annotation+"kind"] != "bucket" {
		return m, errors.New("invalid bucket record")
	}
	return m, nil
}
func (s *OCIStore) CreateBucket(ctx context.Context, scope Scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.bucket(ctx, scope, name); err == nil {
		return ErrBucketExists
	} else if !errors.Is(err, ErrNoBucket) {
		return err
	}
	repo := s.namespace + "/t-" + hash(scope.Tenant) + "/b-" + strings.ToLower(id())
	return s.publish(ctx, s.catalog(scope), "bucket-"+hash(name), "bucket", map[string]string{annotation + "name": name, annotation + "repo": repo, annotation + "created": time.Now().UTC().Format(time.RFC3339Nano)}, nil)
}
func (s *OCIStore) ListBuckets(ctx context.Context, scope Scope) ([]Bucket, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []Bucket{}
	for tag, err := range s.registry.Tags(ctx, s.catalog(scope), nil) {
		if errors.Is(err, oci.ErrNameUnknown) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(tag, "bucket-") {
			continue
		}
		m, err := s.read(ctx, s.catalog(scope), tag)
		if err != nil {
			return nil, err
		}
		if m.Annotations[annotation+"kind"] == "deleted-bucket" {
			continue
		}
		name := m.Annotations[annotation+"name"]
		if tag != "bucket-"+hash(name) {
			return nil, errors.New("bucket catalog mismatch")
		}
		t, err := time.Parse(time.RFC3339Nano, m.Annotations[annotation+"created"])
		if err != nil {
			return nil, err
		}
		result = append(result, Bucket{Name: name, Created: t})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}
func (s *OCIStore) HeadBucket(ctx context.Context, scope Scope, name string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, err := s.bucket(ctx, scope, name)
	return err
}
func (s *OCIStore) DeleteBucket(ctx context.Context, scope Scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.writeBucket(ctx, scope, name)
	if err != nil {
		return err
	}
	repo := b.Annotations[annotation+"repo"]
	for tag, err := range s.registry.Tags(ctx, repo, nil) {
		if errors.Is(err, oci.ErrNameUnknown) {
			break
		}
		if err != nil {
			return err
		}
		if strings.HasPrefix(tag, "upload-") {
			m, err := s.read(ctx, repo, tag)
			if err != nil {
				return err
			}
			if m.Annotations[annotation+"kind"] == "multipart-upload" {
				return ErrNotEmpty
			}
			continue
		}
		if !strings.HasPrefix(tag, "obj-") {
			continue
		}
		m, err := s.read(ctx, repo, tag)
		if err != nil {
			return err
		}
		if m.Annotations[annotation+"kind"] != "deleted-object" {
			return ErrNotEmpty
		}
	}
	if err := s.publish(ctx, s.catalog(scope), "bucket-"+hash(name), "deleted-bucket", map[string]string{annotation + "name": name}, nil); err != nil {
		return err
	}
	delete(s.listCache, repo)
	return nil
}
func objectInfo(m oci.IndexOrManifest) (Object, error) {
	a := m.Annotations
	n, err := strconv.ParseInt(a[annotation+"size"], 10, 64)
	if err != nil || n < 0 {
		return Object{}, errors.New("invalid object size")
	}
	t, err := time.Parse(time.RFC3339Nano, a[annotation+"modified"])
	if err != nil {
		return Object{}, err
	}
	o := Object{Key: a[annotation+"key"], Size: n, ETag: a[annotation+"etag"], Modified: t, Headers: map[string]string{}, Metadata: map[string]string{}}
	for k, v := range a {
		if strings.HasPrefix(k, annotation+"header.") {
			o.Headers[strings.TrimPrefix(k, annotation+"header.")] = v
		}
		if strings.HasPrefix(k, annotation+"meta.") {
			o.Metadata[strings.TrimPrefix(k, annotation+"meta.")] = v
		}
	}
	return o, nil
}
func (s *OCIStore) object(ctx context.Context, scope Scope, bucket, key string) (string, oci.IndexOrManifest, error) {
	b, err := s.bucket(ctx, scope, bucket)
	if err != nil {
		return "", oci.IndexOrManifest{}, err
	}
	repo := b.Annotations[annotation+"repo"]
	m, err := s.read(ctx, repo, "obj-"+hash(key))
	if absent(err) {
		err = ErrNoKey
	}
	if err != nil {
		return repo, m, err
	}
	if m.Annotations[annotation+"key"] != key {
		return repo, m, errors.New("object key mismatch")
	}
	if m.Annotations[annotation+"kind"] == "deleted-object" {
		return repo, m, ErrNoKey
	}
	if m.Annotations[annotation+"kind"] != "object" {
		return repo, m, errors.New("invalid object record")
	}
	return repo, m, nil
}
func (s *OCIStore) HeadObject(ctx context.Context, scope Scope, bucket, key string) (Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, m, err := s.object(ctx, scope, bucket, key)
	if err != nil {
		return Object{}, err
	}
	return objectInfo(m)
}

// ListObjects returns a sorted snapshot of the bucket's live objects. The
// cache is internal to the OCI adapter because it owns both OCI mutations and
// cache invalidation. A process restart or cache miss rebuilds it by scanning
// current object tags and their manifest annotations.
func (s *OCIStore) ListObjects(ctx context.Context, scope Scope, request ListRequest) ([]Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, err := s.bucket(ctx, scope, request.Bucket)
	if err != nil {
		return nil, err
	}
	repo := bucket.Annotations[annotation+"repo"]
	if cached, ok := s.listCache[repo]; ok {
		return append([]Object(nil), cached...), nil
	}

	objects := make([]Object, 0)
	for tag, err := range s.registry.Tags(ctx, repo, nil) {
		if errors.Is(err, oci.ErrNameUnknown) {
			break
		}
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(tag, "obj-") {
			continue
		}

		manifest, err := s.read(ctx, repo, tag)
		if err != nil {
			return nil, err
		}
		key := manifest.Annotations[annotation+"key"]
		if tag != "obj-"+hash(key) {
			return nil, errors.New("object tag key mismatch")
		}
		if manifest.Annotations[annotation+"kind"] == "deleted-object" {
			continue
		}
		if manifest.Annotations[annotation+"kind"] != "object" {
			return nil, errors.New("invalid object record")
		}
		object, err := objectInfo(manifest)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}

	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	s.listCache[repo] = objects
	return append([]Object(nil), objects...), nil
}
func (s *OCIStore) GetObject(ctx context.Context, scope Scope, bucket, key string) (ObjectReader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	repo, m, err := s.object(ctx, scope, bucket, key)
	if err != nil {
		return ObjectReader{}, err
	}
	o, err := objectInfo(m)
	if err != nil {
		return ObjectReader{}, err
	}
	if len(m.Layers) == 0 {
		return ObjectReader{}, errors.New("invalid object layers")
	}
	var size int64
	for _, layer := range m.Layers {
		if layer.Size < 0 {
			return ObjectReader{}, errors.New("invalid object layer size")
		}
		size += layer.Size
	}
	if size != o.Size {
		return ObjectReader{}, errors.New("invalid object layers")
	}
	return ObjectReader{Object: o, Body: &layerReadCloser{ctx: ctx, registry: s.registry, repo: repo, layers: m.Layers}}, nil
}

type layerReadCloser struct {
	ctx      context.Context
	registry oci.Registry
	repo     string
	layers   []oci.Descriptor
	next     int
	current  io.ReadCloser
	closed   bool
}

func (r *layerReadCloser) Read(p []byte) (int, error) {
	for {
		if r.closed {
			return 0, io.ErrClosedPipe
		}
		if r.current == nil {
			if r.next == len(r.layers) {
				return 0, io.EOF
			}
			reader, err := r.registry.GetBlob(r.ctx, r.repo, r.layers[r.next].Digest)
			if err != nil {
				return 0, err
			}
			r.current = reader
			r.next++
		}
		n, err := r.current.Read(p)
		if err != io.EOF {
			return n, err
		}
		closeErr := r.current.Close()
		r.current = nil
		if n > 0 {
			return n, nil
		}
		if closeErr != nil {
			return 0, closeErr
		}
	}
}

func (r *layerReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.current != nil {
		return r.current.Close()
	}
	return nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
func (s *OCIStore) PutObject(ctx context.Context, scope Scope, p PutRequest, r io.Reader) (Object, error) {
	s.mu.RLock()
	b, err := s.bucket(ctx, scope, p.Bucket)
	s.mu.RUnlock()
	if err != nil {
		return Object{}, err
	}
	repo := b.Annotations[annotation+"repo"]
	w, err := s.registry.PushBlobChunked(ctx, repo, 1<<20)
	if err != nil {
		return Object{}, err
	}
	defer w.Cancel()
	md, sha := md5.New(), sha256.New()
	n, err := io.Copy(io.MultiWriter(w, md, sha), contextReader{ctx, r})
	if err != nil {
		return Object{}, err
	}
	if p.Size >= 0 && n != p.Size {
		return Object{}, ErrIntegrity
	}
	if len(p.MD5) > 0 && !bytes.Equal(md.Sum(nil), p.MD5) || len(p.SHA256) > 0 && !bytes.Equal(sha.Sum(nil), p.SHA256) {
		return Object{}, ErrIntegrity
	}
	if err = ctx.Err(); err != nil {
		return Object{}, err
	}
	if err = w.Close(); err != nil {
		return Object{}, err
	}
	d, err := w.Commit(oci.Digest("sha256:" + hex.EncodeToString(sha.Sum(nil))))
	if err != nil {
		return Object{}, err
	}
	d.MediaType = "application/octet-stream"
	a := objectAnnotations(p.Key, n, hex.EncodeToString(md.Sum(nil)), time.Now(), p.Headers, p.Metadata)
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.writeBucket(ctx, scope, p.Bucket)
	if err != nil {
		return Object{}, err
	}
	if current.Annotations[annotation+"repo"] != repo {
		return Object{}, ErrNoBucket
	}
	if err = s.publish(ctx, repo, "obj-"+hash(p.Key), "object", a, []oci.Descriptor{d}); err != nil {
		return Object{}, fmt.Errorf("publish object: %w", err)
	}
	delete(s.listCache, repo)
	return objectInfo(oci.IndexOrManifest{Annotations: a})
}

func objectAnnotations(key string, size int64, etag string, modified time.Time, headers, metadata map[string]string) map[string]string {
	a := map[string]string{
		annotation + "key":      key,
		annotation + "size":     strconv.FormatInt(size, 10),
		annotation + "etag":     etag,
		annotation + "modified": modified.UTC().Format(time.RFC3339Nano),
	}
	for k, v := range headers {
		a[annotation+"header."+k] = v
	}
	for k, v := range metadata {
		a[annotation+"meta."+k] = v
	}
	return a
}
func (s *OCIStore) DeleteObject(ctx context.Context, scope Scope, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.writeBucket(ctx, scope, bucket)
	if err != nil {
		return err
	}
	repo := b.Annotations[annotation+"repo"]
	if err := s.publish(ctx, repo, "obj-"+hash(key), "deleted-object", map[string]string{annotation + "key": key}, nil); err != nil {
		return err
	}
	delete(s.listCache, repo)
	return nil
}
