package s3

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ohseeeye/oci"
)

func (s *OCIStore) indexedObject(ctx context.Context, repo string, key string) (oci.Descriptor, oci.IndexOrManifest, error) {
	root, err := s.readIndex(ctx, repo)
	if err != nil {
		return oci.Descriptor{}, oci.IndexOrManifest{}, err
	}
	d, err := s.findEntry(ctx, repo, root.objects, key)
	if err != nil {
		return d, oci.IndexOrManifest{}, err
	}
	m, err := s.readDigest(ctx, repo, d)
	if err != nil {
		return d, m, err
	}
	if m.Annotations[annotation+"schema"] != "1" || m.Annotations[annotation+"kind"] != "object" || m.Annotations[annotation+"key"] != key {
		return d, m, errors.New("invalid indexed object")
	}
	// Listing metadata and content must describe the exact same generation.
	for _, name := range []string{"key", "size", "etag", "modified", "generation", "commit-id", "created", "version-id"} {
		if d.Annotations[annotation+name] != m.Annotations[annotation+name] {
			return d, m, errors.New("object index metadata mismatch")
		}
	}
	return d, m, nil
}

// Only a changed root is a retryable CAS conflict. ErrManifestInvalid also
// covers malformed manifests in the OCI API and must not be retried blindly.
func (s *OCIStore) indexConflict(ctx context.Context, repo string, old bucketIndex, err error) bool {
	if !errors.Is(err, oci.ErrManifestInvalid) {
		return false
	}
	current, readErr := s.readIndex(ctx, repo)
	return readErr == nil && current.descriptor.Digest != old.descriptor.Digest
}
func (s *OCIStore) putIndexedObject(ctx context.Context, repo string, a map[string]string, layers []oci.Descriptor) error {
	for attempt := 0; attempt < maxIndexRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := s.readIndex(ctx, repo)
		if err != nil {
			return err
		}
		current, err := s.findEntry(ctx, repo, root.objects, a[annotation+"key"])
		if err != nil && !errors.Is(err, ErrNoKey) {
			return err
		}
		a[annotation+"created"] = a[annotation+"modified"]
		if current.Annotations[annotation+"created"] != "" {
			a[annotation+"created"] = current.Annotations[annotation+"created"]
		}
		if err := s.prepareVersion(ctx, repo, root, a); err != nil {
			return err
		}
		d, err := s.pushArtifact(ctx, repo, "object", a, layers, nil)
		if err != nil {
			return err
		}
		d = objectDescriptor(d, oci.IndexOrManifest{Annotations: a})
		updated := root
		updated.objects, err = s.changeTree(ctx, repo, root.objects, a[annotation+"key"], &d)
		if err != nil {
			return err
		}
		updated.versions, err = s.appendVersion(ctx, repo, root, d)
		if err != nil {
			return err
		}
		_, err = s.commitIndex(ctx, repo, updated)
		if err == nil {
			return nil
		}
		if !s.indexConflict(ctx, repo, root, err) {
			return fmt.Errorf("publish object index: %w", err)
		}
	}
	return errors.New("bucket index update contention exceeded retry limit")
}
func (s *OCIStore) deleteIndexedObject(ctx context.Context, repo, key string) error {
	for attempt := 0; attempt < maxIndexRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := s.readIndex(ctx, repo)
		if err != nil {
			return err
		}
		updated := root
		updated.objects, err = s.changeTree(ctx, repo, root.objects, key, nil)
		if err != nil {
			return err
		}
		a := objectAnnotations(key, 0, "", time.Now(), nil, nil)
		a[annotation+"created"] = a[annotation+"modified"]
		a[annotation+"deleted"] = "true"
		if err := s.prepareVersion(ctx, repo, root, a); err != nil {
			return err
		}
		d, err := s.pushArtifact(ctx, repo, "object", a, nil, nil)
		if err != nil {
			return err
		}
		updated.versions, err = s.appendVersion(ctx, repo, root, objectDescriptor(d, oci.IndexOrManifest{Annotations: a}))
		if err != nil {
			return err
		}
		_, err = s.commitIndex(ctx, repo, updated)
		if err == nil {
			return nil
		}
		if !s.indexConflict(ctx, repo, root, err) {
			return fmt.Errorf("publish object deletion: %w", err)
		}
	}
	return errors.New("bucket index update contention exceeded retry limit")
}

func (s *OCIStore) commitCompletion(ctx context.Context, repo string, state oci.IndexOrManifest) error {
	a := state.Annotations
	for attempt := 0; attempt < maxIndexRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := s.readIndex(ctx, repo)
		if err != nil {
			return err
		}
		receipt, err := s.findEntry(ctx, repo, root.completions, a[annotation+"commit-id"])
		if err == nil {
			committed, err := s.readDigest(ctx, repo, receipt)
			if err != nil {
				return err
			}
			for _, name := range []string{"key", "commit-id", "etag", "size", "modified"} {
				if committed.Annotations[annotation+name] != a[annotation+name] {
					return errors.New("completion receipt mismatch")
				}
			}
			a[annotation+"version-id"] = committed.Annotations[annotation+"version-id"]
			return nil
		}
		if !errors.Is(err, ErrNoKey) {
			return err
		}
		current, err := s.findEntry(ctx, repo, root.objects, a[annotation+"key"])
		if err != nil && !errors.Is(err, ErrNoKey) {
			return err
		}
		if current.Annotations[annotation+"generation"] != a[annotation+"base-generation"] {
			return errors.New("multipart recovery conflicts with a later object generation")
		}
		published := cloneStrings(a)
		if err := s.prepareVersion(ctx, repo, root, published); err != nil {
			return err
		}
		d, err := s.pushArtifact(ctx, repo, "object", published, state.Layers, nil)
		if err != nil {
			return err
		}
		d = objectDescriptor(d, oci.IndexOrManifest{Annotations: published})
		updated := root
		updated.objects, err = s.changeTree(ctx, repo, root.objects, a[annotation+"key"], &d)
		if err != nil {
			return err
		}
		receipt = d
		receipt.Annotations = cloneStrings(d.Annotations)
		receipt.Annotations[annotation+"key"] = a[annotation+"commit-id"]
		updated.completions, err = s.changeTree(ctx, repo, root.completions, a[annotation+"commit-id"], &receipt)
		if err != nil {
			return err
		}
		updated.versions, err = s.appendVersion(ctx, repo, root, d)
		if err != nil {
			return err
		}
		_, err = s.commitIndex(ctx, repo, updated)
		if err == nil {
			a[annotation+"version-id"] = published[annotation+"version-id"]
			return nil
		}
		if !s.indexConflict(ctx, repo, root, err) {
			return fmt.Errorf("publish multipart index: %w", err)
		}
	}
	return errors.New("bucket index update contention exceeded retry limit")
}

// A cursor pins an immutable tree and reads only the path to the next key and
// subsequent leaves. Seeking past a common prefix skips its entire key range.
type indexFrame struct {
	node     indexNode
	position int
}
type indexCursor struct {
	store  *OCIStore
	ctx    context.Context
	repo   string
	root   oci.Descriptor
	frames []indexFrame
}

func (c *indexCursor) seek(key string) error {
	c.frames = nil
	d := c.root
	for depth := 0; depth <= maxIndexDepth; depth++ {
		n, err := c.store.loadNode(c.ctx, c.repo, d)
		if err != nil {
			return err
		}
		position := 0
		if n.height == 0 {
			position = sort.Search(len(n.entries), func(i int) bool { return indexKey(n.entries[i], 0) >= key })
		} else {
			position = childPosition(n, key)
		}
		c.frames = append(c.frames, indexFrame{node: n, position: position})
		if n.height == 0 {
			return nil
		}
		d = n.entries[position]
	}
	return errors.New("index depth limit exceeded")
}
func (c *indexCursor) next() (oci.Descriptor, bool, error) {
	for len(c.frames) > 0 {
		last := len(c.frames) - 1
		frame := &c.frames[last]
		if frame.node.height == 0 && frame.position < len(frame.node.entries) {
			d := frame.node.entries[frame.position]
			frame.position++
			return d, true, nil
		}
		c.frames = c.frames[:last]
		for len(c.frames) > 0 {
			parent := &c.frames[len(c.frames)-1]
			parent.position++
			if parent.position >= len(parent.node.entries) {
				c.frames = c.frames[:len(c.frames)-1]
				continue
			}
			d := parent.node.entries[parent.position]
			for {
				n, err := c.store.loadNode(c.ctx, c.repo, d)
				if err != nil {
					return oci.Descriptor{}, false, err
				}
				c.frames = append(c.frames, indexFrame{node: n})
				if len(c.frames) > maxIndexDepth+1 {
					return oci.Descriptor{}, false, errors.New("index depth limit exceeded")
				}
				if n.height == 0 {
					break
				}
				d = n.entries[0]
			}
			break
		}
	}
	return oci.Descriptor{}, false, nil
}
func prefixEnd(prefix string) (string, bool) {
	bytes := []byte(prefix)
	for i := len(bytes) - 1; i >= 0; i-- {
		if bytes[i] < 255 {
			bytes[i]++
			return string(bytes[:i+1]), true
		}
	}
	return "", false
}
func (s *OCIStore) listIndexedObjects(ctx context.Context, repo string, p ListRequest) ([]Object, error) {
	root, err := s.readIndex(ctx, repo)
	if err != nil {
		return nil, err
	}
	cursor := indexCursor{store: s, ctx: ctx, repo: repo, root: root.objects}
	if err := cursor.seek(max(p.Prefix, p.After)); err != nil {
		return nil, err
	}
	objects := make([]Object, 0)
	for {
		d, ok, err := cursor.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		key := d.Annotations[annotation+"key"]
		if !strings.HasPrefix(key, p.Prefix) {
			break
		}
		name := key
		grouped := false
		if p.Delimiter != "" {
			rest := strings.TrimPrefix(key, p.Prefix)
			if i := strings.Index(rest, p.Delimiter); i >= 0 {
				name = p.Prefix + rest[:i+len(p.Delimiter)]
				grouped = true
			}
		}
		if name > p.After {
			object, err := objectInfo(oci.IndexOrManifest{Annotations: d.Annotations})
			if err != nil {
				return nil, err
			}
			objects = append(objects, object)
			if p.Limit > 0 && len(objects) >= p.Limit {
				break
			}
		}
		if grouped {
			end, ok := prefixEnd(name)
			if !ok {
				break
			}
			if err := cursor.seek(end); err != nil {
				return nil, err
			}
		}
	}
	return objects, nil
}
