package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ohseeeye/oci"
)

const partMediaType = "application/vnd.essthree.part.v1"

func cloneStrings(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func (s *OCIStore) multipart(ctx context.Context, scope Scope, bucket, key, uploadID string) (string, oci.IndexOrManifest, error) {
	b, err := s.bucket(ctx, scope, bucket)
	if err != nil {
		return "", oci.IndexOrManifest{}, err
	}
	repo := b.Annotations[annotation+"repo"]
	m, err := s.read(ctx, repo, "upload-"+hash(uploadID))
	if absent(err) {
		err = ErrNoUpload
	}
	if err != nil {
		return repo, m, err
	}
	if m.Annotations[annotation+"upload-id"] != uploadID || m.Annotations[annotation+"key"] != key || m.Annotations[annotation+"kind"] != "multipart-upload" {
		return repo, m, ErrNoUpload
	}
	return repo, m, nil
}

func multipartInfo(bucket string, m oci.IndexOrManifest) (MultipartUpload, error) {
	created, err := time.Parse(time.RFC3339Nano, m.Annotations[annotation+"created"])
	if err != nil {
		return MultipartUpload{}, err
	}
	upload := MultipartUpload{
		Bucket: bucket, Key: m.Annotations[annotation+"key"], UploadID: m.Annotations[annotation+"upload-id"],
		Created: created, Headers: map[string]string{}, Metadata: map[string]string{},
	}
	for key, value := range m.Annotations {
		if strings.HasPrefix(key, annotation+"header.") {
			upload.Headers[strings.TrimPrefix(key, annotation+"header.")] = value
		}
		if strings.HasPrefix(key, annotation+"meta.") {
			upload.Metadata[strings.TrimPrefix(key, annotation+"meta.")] = value
		}
	}
	for _, layer := range m.Layers {
		partNumber := layer.Annotations[annotation+"part-number"]
		if partNumber == "" {
			continue
		}
		number, err := strconv.Atoi(partNumber)
		if err != nil || number < 1 || number > 10000 {
			return MultipartUpload{}, errors.New("invalid multipart part number")
		}
		modified, err := time.Parse(time.RFC3339Nano, layer.Annotations[annotation+"modified"])
		if err != nil {
			return MultipartUpload{}, err
		}
		upload.Parts = append(upload.Parts, Part{Number: number, Size: layer.Size, ETag: layer.Annotations[annotation+"etag"], Modified: modified})
	}
	sort.Slice(upload.Parts, func(i, j int) bool { return upload.Parts[i].Number < upload.Parts[j].Number })
	return upload, nil
}

func (s *OCIStore) CreateMultipartUpload(ctx context.Context, scope Scope, p PutRequest) (MultipartUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.writeBucket(ctx, scope, p.Bucket)
	if err != nil {
		return MultipartUpload{}, err
	}
	created := time.Now().UTC()
	uploadID := id()
	a := map[string]string{
		annotation + "upload-id": uploadID,
		annotation + "key":       p.Key,
		annotation + "created":   created.Format(time.RFC3339Nano),
	}
	for key, value := range p.Headers {
		a[annotation+"header."+key] = value
	}
	for key, value := range p.Metadata {
		a[annotation+"meta."+key] = value
	}
	repo := b.Annotations[annotation+"repo"]
	if err := s.publish(ctx, repo, "upload-"+hash(uploadID), "multipart-upload", a, nil); err != nil {
		return MultipartUpload{}, err
	}
	return MultipartUpload{Bucket: p.Bucket, Key: p.Key, UploadID: uploadID, Created: created, Headers: cloneStrings(p.Headers), Metadata: cloneStrings(p.Metadata)}, nil
}

func (s *OCIStore) UploadPart(ctx context.Context, scope Scope, p UploadPartRequest, r io.Reader) (Part, error) {
	if p.Number < 1 || p.Number > 10000 {
		return Part{}, ErrInvalidPart
	}
	s.mu.RLock()
	repo, _, err := s.multipart(ctx, scope, p.Bucket, p.Key, p.UploadID)
	s.mu.RUnlock()
	if err != nil {
		return Part{}, err
	}
	w, err := s.registry.PushBlobChunked(ctx, repo, 1<<20)
	if err != nil {
		return Part{}, err
	}
	defer w.Cancel()
	md, sha := md5.New(), sha256.New()
	size, err := io.Copy(io.MultiWriter(w, md, sha), contextReader{ctx, r})
	if err != nil {
		return Part{}, err
	}
	if p.Size >= 0 && size != p.Size || len(p.MD5) > 0 && !bytes.Equal(md.Sum(nil), p.MD5) || len(p.SHA256) > 0 && !bytes.Equal(sha.Sum(nil), p.SHA256) {
		return Part{}, ErrIntegrity
	}
	if err := ctx.Err(); err != nil {
		return Part{}, err
	}
	if err := w.Close(); err != nil {
		return Part{}, err
	}
	descriptor, err := w.Commit(oci.Digest("sha256:" + hex.EncodeToString(sha.Sum(nil))))
	if err != nil {
		return Part{}, err
	}
	modified := time.Now().UTC()
	etag := hex.EncodeToString(md.Sum(nil))
	descriptor.MediaType = partMediaType
	descriptor.Annotations = map[string]string{
		annotation + "part-number": strconv.Itoa(p.Number),
		annotation + "etag":        etag,
		annotation + "modified":    modified.Format(time.RFC3339Nano),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.writeBucket(ctx, scope, p.Bucket); err != nil {
		return Part{}, err
	}
	_, state, err := s.multipart(ctx, scope, p.Bucket, p.Key, p.UploadID)
	if err != nil {
		return Part{}, err
	}
	layers := make([]oci.Descriptor, 0, len(state.Layers)+1)
	for _, layer := range state.Layers {
		number, _ := strconv.Atoi(layer.Annotations[annotation+"part-number"])
		if number != 0 && number != p.Number {
			layers = append(layers, layer)
		}
	}
	layers = append(layers, descriptor)
	sort.Slice(layers, func(i, j int) bool {
		left, _ := strconv.Atoi(layers[i].Annotations[annotation+"part-number"])
		right, _ := strconv.Atoi(layers[j].Annotations[annotation+"part-number"])
		return left < right
	})
	if err := s.publish(ctx, repo, "upload-"+hash(p.UploadID), "multipart-upload", cloneStrings(state.Annotations), layers); err != nil {
		return Part{}, err
	}
	return Part{Number: p.Number, Size: size, ETag: etag, Modified: modified}, nil
}

func (s *OCIStore) ListParts(ctx context.Context, scope Scope, bucket, key, uploadID string) (MultipartUpload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, state, err := s.multipart(ctx, scope, bucket, key, uploadID)
	if err != nil {
		return MultipartUpload{}, err
	}
	return multipartInfo(bucket, state)
}

func (s *OCIStore) CompleteMultipartUpload(ctx context.Context, scope Scope, p CompleteMultipartRequest) (Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.writeBucket(ctx, scope, p.Bucket)
	if err != nil {
		return Object{}, err
	}
	repo := b.Annotations[annotation+"repo"]
	state, err := s.read(ctx, repo, "upload-"+hash(p.UploadID))
	if absent(err) {
		return Object{}, ErrNoUpload
	}
	if err != nil {
		return Object{}, err
	}
	if state.Annotations[annotation+"upload-id"] != p.UploadID || state.Annotations[annotation+"key"] != p.Key {
		return Object{}, ErrNoUpload
	}
	if state.Annotations[annotation+"kind"] == "completed-upload" {
		if state.Annotations[annotation+"commit-id"] == "" {
			return Object{}, ErrNoUpload
		}
		if !matchesCompletion(state.Layers, p.Parts) {
			return Object{}, ErrInvalidPart
		}
		return objectInfo(state)
	}
	if state.Annotations[annotation+"kind"] != "multipart-upload" {
		return Object{}, ErrNoUpload
	}
	if len(p.Parts) == 0 {
		return Object{}, ErrInvalidPart
	}
	byNumber := make(map[int]oci.Descriptor, len(state.Layers))
	for _, layer := range state.Layers {
		number, _ := strconv.Atoi(layer.Annotations[annotation+"part-number"])
		if number != 0 {
			byNumber[number] = layer
		}
	}
	layers := make([]oci.Descriptor, 0, len(p.Parts))
	partDigests := make([]byte, 0, md5.Size*len(p.Parts))
	var size int64
	previous := 0
	for _, completed := range p.Parts {
		if completed.Number <= previous {
			return Object{}, ErrPartOrder
		}
		previous = completed.Number
		layer, ok := byNumber[completed.Number]
		if !ok || strings.Trim(completed.ETag, "\"") != layer.Annotations[annotation+"etag"] {
			return Object{}, ErrInvalidPart
		}
		digest, err := hex.DecodeString(layer.Annotations[annotation+"etag"])
		if err != nil || len(digest) != md5.Size {
			return Object{}, ErrInvalidPart
		}
		partDigests = append(partDigests, digest...)
		if layer.Size < 0 || layer.Size > int64(^uint64(0)>>1)-size {
			return Object{}, ErrTooLarge
		}
		size += layer.Size
		if p.MaxSize >= 0 && size > p.MaxSize {
			return Object{}, ErrTooLarge
		}
		layers = append(layers, layer)
	}
	multipartMD5 := md5.Sum(partDigests)
	etag := fmt.Sprintf("%x-%d", multipartMD5, len(layers))
	upload, err := multipartInfo(p.Bucket, state)
	if err != nil {
		return Object{}, err
	}
	a := objectAnnotations(p.Key, size, etag, time.Now(), upload.Headers, upload.Metadata)
	a[annotation+"upload-id"] = p.UploadID
	a[annotation+"created"] = state.Annotations[annotation+"created"]
	a[annotation+"commit-id"] = id()
	current, err := s.read(ctx, repo, "obj-"+hash(p.Key))
	if err != nil && !absent(err) {
		return Object{}, err
	}
	if err == nil && (current.Annotations[annotation+"key"] != p.Key || (current.Annotations[annotation+"kind"] != "object" && current.Annotations[annotation+"kind"] != "deleted-object")) {
		return Object{}, errors.New("invalid completion destination record")
	}
	a[annotation+"base-generation"] = current.Annotations[annotation+"generation"]
	// Freezing the selected descriptors makes all subsequent attempts use the
	// same object generation, even if a publication succeeds but returns an error.
	if err := s.publish(ctx, repo, "upload-"+hash(p.UploadID), "completing-upload", a, layers); err != nil {
		return Object{}, fmt.Errorf("freeze multipart upload: %w", err)
	}
	state = oci.IndexOrManifest{Annotations: a, Layers: layers}
	if err := s.finishCompletion(ctx, repo, state); err != nil {
		return Object{}, err
	}
	return objectInfo(state)
}

func (s *OCIStore) AbortMultipartUpload(ctx context.Context, scope Scope, bucket, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.writeBucket(ctx, scope, bucket); err != nil {
		return err
	}
	repo, state, err := s.multipart(ctx, scope, bucket, key, uploadID)
	if err != nil {
		return err
	}
	return s.publish(ctx, repo, "upload-"+hash(uploadID), "aborted-upload", map[string]string{annotation + "upload-id": uploadID, annotation + "key": key, annotation + "created": state.Annotations[annotation+"created"]}, state.Layers)
}

func (s *OCIStore) ListMultipartUploads(ctx context.Context, scope Scope, p MultipartListRequest) ([]MultipartUpload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucket := p.Bucket
	b, err := s.bucket(ctx, scope, bucket)
	if err != nil {
		return nil, err
	}
	repo := b.Annotations[annotation+"repo"]
	var markerTime time.Time
	if p.KeyMarker != "" && p.UploadIDMarker != "" {
		marker, err := s.read(ctx, repo, "upload-"+hash(p.UploadIDMarker))
		if err != nil && !absent(err) {
			return nil, err
		}
		if err == nil && marker.Annotations[annotation+"upload-id"] == p.UploadIDMarker && marker.Annotations[annotation+"key"] == p.KeyMarker && marker.Annotations[annotation+"created"] != "" {
			markerTime, err = time.Parse(time.RFC3339Nano, marker.Annotations[annotation+"created"])
			if err != nil {
				return nil, err
			}
		}
	}
	uploads := make([]MultipartUpload, 0)
	for tag, err := range s.registry.Tags(ctx, repo, nil) {
		if errors.Is(err, oci.ErrNameUnknown) {
			break
		}
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(tag, "upload-") {
			continue
		}
		state, err := s.read(ctx, repo, tag)
		if err != nil {
			return nil, err
		}
		if tag != "upload-"+hash(state.Annotations[annotation+"upload-id"]) {
			return nil, errors.New("upload tag mismatch")
		}
		switch state.Annotations[annotation+"kind"] {
		case "multipart-upload":
			u, err := multipartInfo(bucket, state)
			if err != nil {
				return nil, err
			}
			u.Parts = nil
			if u.Key < p.KeyMarker {
				continue
			}
			if u.Key == p.KeyMarker {
				if p.UploadIDMarker == "" {
					continue
				}
				if !markerTime.IsZero() {
					if u.Created.Before(markerTime) || (u.Created.Equal(markerTime) && u.UploadID <= p.UploadIDMarker) {
						continue
					}
				} else if u.UploadID <= p.UploadIDMarker {
					continue
				}
			}
			uploads = append(uploads, u)
		case "completing-upload", "completed-upload", "aborted-upload":
		default:
			return nil, errors.New("invalid upload record")
		}
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		if !uploads[i].Created.Equal(uploads[j].Created) {
			return uploads[i].Created.Before(uploads[j].Created)
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})
	return uploads, nil
}

func (s *OCIStore) CopyObject(ctx context.Context, scope Scope, p CopyRequest) (Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sourceRepo, sourceManifest, err := s.object(ctx, scope, p.SourceBucket, p.SourceKey)
	if err != nil {
		return Object{}, err
	}
	source, err := objectInfo(sourceManifest)
	if err != nil {
		return Object{}, err
	}
	destinationBucket, err := s.writeBucket(ctx, scope, p.Destination.Bucket)
	if err != nil {
		return Object{}, err
	}
	destinationRepo := destinationBucket.Annotations[annotation+"repo"]
	layers := make([]oci.Descriptor, 0, len(sourceManifest.Layers))
	for _, layer := range sourceManifest.Layers {
		mounted := layer
		if sourceRepo != destinationRepo {
			mounted, err = s.registry.MountBlob(ctx, sourceRepo, destinationRepo, layer.Digest)
			if err != nil {
				return Object{}, err
			}
			mounted.MediaType = layer.MediaType
			mounted.Annotations = cloneStrings(layer.Annotations)
		}
		layers = append(layers, mounted)
	}
	headers, metadata := source.Headers, source.Metadata
	if p.ReplaceMetadata {
		headers, metadata = p.Destination.Headers, p.Destination.Metadata
	}
	a := objectAnnotations(p.Destination.Key, source.Size, source.ETag, time.Now(), headers, metadata)
	if err := s.publish(ctx, destinationRepo, "obj-"+hash(p.Destination.Key), "object", a, layers); err != nil {
		return Object{}, fmt.Errorf("publish copied object: %w", err)
	}
	delete(s.listCache, destinationRepo)
	return objectInfo(oci.IndexOrManifest{Annotations: a})
}
