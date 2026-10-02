package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ohseeeye/oci"
)

// writeBucket must be called under the store's exclusive lock (or at startup).
// Reconcile frozen completions before another operation can change this bucket.
func (s *OCIStore) writeBucket(ctx context.Context, scope Scope, name string) (oci.IndexOrManifest, error) {
	b, err := s.bucket(ctx, scope, name)
	if err != nil {
		return b, err
	}
	return b, s.recoverUploads(ctx, b.Annotations[annotation+"repo"])
}

func (s *OCIStore) recoverScope(ctx context.Context, scope Scope) error {
	for tag, err := range s.registry.Tags(ctx, s.catalog(scope), nil) {
		if errors.Is(err, oci.ErrNameUnknown) {
			return nil
		}
		if err != nil {
			return err
		}
		if !strings.HasPrefix(tag, "bucket-") {
			continue
		}
		b, err := s.read(ctx, s.catalog(scope), tag)
		if err != nil {
			return err
		}
		if b.Annotations[annotation+"kind"] == "deleted-bucket" {
			continue
		}
		if tag != "bucket-"+hash(b.Annotations[annotation+"name"]) || b.Annotations[annotation+"kind"] != "bucket" || b.Annotations[annotation+"repo"] == "" {
			return errors.New("invalid bucket recovery record")
		}
		if err := s.recoverUploads(ctx, b.Annotations[annotation+"repo"]); err != nil {
			return err
		}
	}
	return nil
}

func (s *OCIStore) recoverUploads(ctx context.Context, repo string) error {
	// Collect first: publishing while iterating can invalidate a registry's iterator.
	var pending []oci.IndexOrManifest
	for tag, err := range s.registry.Tags(ctx, repo, nil) {
		if errors.Is(err, oci.ErrNameUnknown) {
			return nil
		}
		if err != nil {
			return err
		}
		if !strings.HasPrefix(tag, "upload-") {
			continue
		}
		state, err := s.read(ctx, repo, tag)
		if err != nil {
			return err
		}
		if tag != "upload-"+hash(state.Annotations[annotation+"upload-id"]) {
			return errors.New("upload tag mismatch")
		}
		if state.Annotations[annotation+"kind"] == "completing-upload" {
			pending = append(pending, state)
		}
	}
	for _, state := range pending {
		if err := s.finishCompletion(ctx, repo, state); err != nil {
			return err
		}
	}
	return nil
}

func matchesCompletion(layers []oci.Descriptor, parts []CompletedPart) bool {
	if len(layers) != len(parts) || len(parts) == 0 {
		return false
	}
	for i, part := range parts {
		if strconv.Itoa(part.Number) != layers[i].Annotations[annotation+"part-number"] || strings.Trim(part.ETag, "\"") != layers[i].Annotations[annotation+"etag"] {
			return false
		}
	}
	return true
}

func (s *OCIStore) finishCompletion(ctx context.Context, repo string, state oci.IndexOrManifest) error {
	a := state.Annotations
	if a[annotation+"commit-id"] == "" || a[annotation+"upload-id"] == "" || a[annotation+"key"] == "" || len(state.Layers) == 0 || len(state.Layers) > 10000 {
		return errors.New("invalid completing upload")
	}
	if _, err := time.Parse(time.RFC3339Nano, a[annotation+"created"]); err != nil {
		return fmt.Errorf("invalid completion creation time: %w", err)
	}
	object, err := objectInfo(state)
	if err != nil {
		return err
	}
	var size int64
	previous := 0
	partDigests := make([]byte, 0, md5.Size*len(state.Layers))
	for _, layer := range state.Layers {
		n, err := strconv.Atoi(layer.Annotations[annotation+"part-number"])
		if err != nil || n <= previous || n > 10000 || layer.Size < 0 || layer.Size > object.Size-size {
			return errors.New("invalid completion layers")
		}
		previous = n
		md, err := hex.DecodeString(layer.Annotations[annotation+"etag"])
		if err != nil || len(md) != md5.Size || layer.MediaType != partMediaType {
			return errors.New("invalid completion part metadata")
		}
		partDigests = append(partDigests, md...)
		d, err := s.registry.ResolveBlob(ctx, repo, layer.Digest)
		if err != nil {
			return fmt.Errorf("resolve completion part: %w", err)
		}
		if d.Size != layer.Size {
			return errors.New("completion blob size mismatch")
		}
		size += layer.Size
	}
	if size != object.Size {
		return errors.New("completion object size mismatch")
	}
	if fmt.Sprintf("%x-%d", md5.Sum(partDigests), len(state.Layers)) != object.ETag {
		return errors.New("completion ETag mismatch")
	}
	current, err := s.read(ctx, repo, "obj-"+hash(object.Key))
	if err != nil && !absent(err) {
		return err
	}
	if err == nil && (current.Annotations[annotation+"key"] != object.Key || (current.Annotations[annotation+"kind"] != "object" && current.Annotations[annotation+"kind"] != "deleted-object")) {
		return errors.New("invalid recovery destination record")
	}
	// Invalidate even on an uncertain publication result: the object might have
	// changed before the registry returned an error.
	delete(s.listCache, repo)
	if current.Annotations[annotation+"commit-id"] != a[annotation+"commit-id"] {
		if current.Annotations[annotation+"generation"] != a[annotation+"base-generation"] {
			return errors.New("multipart recovery conflicts with a later object generation")
		}
		if err := s.publish(ctx, repo, "obj-"+hash(object.Key), "object", cloneStrings(a), state.Layers); err != nil {
			return fmt.Errorf("publish multipart object: %w", err)
		}
	}
	if err := s.publish(ctx, repo, "upload-"+hash(a[annotation+"upload-id"]), "completed-upload", cloneStrings(a), state.Layers); err != nil {
		return fmt.Errorf("mark multipart upload complete: %w", err)
	}
	return nil
}
