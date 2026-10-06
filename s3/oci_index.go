package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/ohseeeye/oci"
)

const bucketIndexTag = "bucket-index"
const manifestLimit = 4 << 20
const defaultIndexEntries = 2000
const defaultIndexBytes = 3 << 20
const maxIndexDepth = 32
const maxIndexRetries = 32
const indexCacheBudget = 8 << 20

// A bucket root commits current objects, retained versions, and completion receipts. All
// descendants are immutable, addressed by digest, and ordered by key ranges.
type bucketIndex struct {
	descriptor                     oci.Descriptor
	objects, completions, versions oci.Descriptor
}
type indexNode struct {
	height  int
	entries []oci.Descriptor
}

type indexCacheKey struct {
	repo   string
	digest oci.Digest
}
type cachedIndexNode struct {
	node indexNode
	size int64
}

func cloneIndexNode(n indexNode) indexNode {
	// Descriptor annotations are immutable; mutations replace descriptors.
	n.entries = append([]oci.Descriptor(nil), n.entries...)
	return n
}
func validateNodeBoundary(d oci.Descriptor, n indexNode) error {
	lower := ""
	if len(n.entries) > 0 {
		lower = indexKey(n.entries[0], n.height)
	}
	if d.MediaType != oci.MediaTypeImageIndex || d.Annotations[annotation+"height"] != strconv.Itoa(n.height) || d.Annotations[annotation+"lower"] != lower {
		return errors.New("index boundary mismatch")
	}
	return nil
}
func (s *OCIStore) cacheNode(repo string, d oci.Descriptor, n indexNode) {
	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()
	key := indexCacheKey{repo: repo, digest: d.Digest}
	if _, exists := s.nodeCache[key]; exists {
		return
	}
	if s.nodeCacheBytes+int(d.Size) > indexCacheBudget {
		s.nodeCache = make(map[indexCacheKey]cachedIndexNode)
		s.nodeCacheBytes = 0
	}
	s.nodeCache[key] = cachedIndexNode{node: cloneIndexNode(n), size: d.Size}
	s.nodeCacheBytes += int(d.Size)
}

func decodeStoredManifest(r oci.BlobReader) (oci.IndexOrManifest, error) {
	defer r.Close()
	if r.Descriptor().Size < 0 || r.Descriptor().Size > manifestLimit {
		return oci.IndexOrManifest{}, errors.New("manifest exceeds limit")
	}
	raw, err := io.ReadAll(io.LimitReader(r, manifestLimit+1))
	if err != nil {
		return oci.IndexOrManifest{}, err
	}
	if len(raw) > manifestLimit || int64(len(raw)) != r.Descriptor().Size {
		return oci.IndexOrManifest{}, errors.New("invalid manifest length")
	}
	var m oci.IndexOrManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	return m, nil
}

func (s *OCIStore) readDigest(ctx context.Context, repo string, d oci.Descriptor) (oci.IndexOrManifest, error) {
	r, err := s.registry.GetManifest(ctx, repo, d.Digest)
	if err != nil {
		return oci.IndexOrManifest{}, err
	}
	if r.Descriptor().Digest != d.Digest || r.Descriptor().Size != d.Size {
		r.Close()
		return oci.IndexOrManifest{}, errors.New("manifest descriptor mismatch")
	}
	return decodeStoredManifest(r)
}

func (s *OCIStore) readIndex(ctx context.Context, repo string) (bucketIndex, error) {
	r, err := s.registry.GetTag(ctx, repo, bucketIndexTag)
	if err != nil {
		return bucketIndex{}, err
	}
	d := r.Descriptor()
	m, err := decodeStoredManifest(r)
	if err != nil {
		return bucketIndex{}, err
	}
	if m.MediaType != oci.MediaTypeImageIndex || m.SchemaVersion != 2 || m.Annotations[annotation+"schema"] != "2" || m.Annotations[annotation+"kind"] != "bucket-root" || len(m.Manifests) != 3 {
		return bucketIndex{}, errors.New("invalid bucket root")
	}
	root := bucketIndex{descriptor: d}
	for _, child := range m.Manifests {
		if child.MediaType != oci.MediaTypeImageIndex || child.Digest == "" || child.Size < 0 || child.Size > manifestLimit {
			return root, errors.New("invalid root child")
		}
		switch child.Annotations[annotation+"tree"] {
		case "objects":
			if root.objects.Digest != "" {
				return root, errors.New("duplicate object tree")
			}
			root.objects = child
		case "versions":
			if root.versions.Digest != "" {
				return root, errors.New("duplicate version tree")
			}
			root.versions = child
		case "completions":
			if root.completions.Digest != "" {
				return root, errors.New("duplicate receipt tree")
			}
			root.completions = child
		default:
			return root, errors.New("unknown root tree")
		}
	}
	if root.objects.Digest == "" || root.completions.Digest == "" || root.versions.Digest == "" {
		return root, errors.New("missing root tree")
	}
	return root, nil
}

func indexKey(d oci.Descriptor, height int) string {
	if height == 0 {
		return d.Annotations[annotation+"key"]
	}
	return d.Annotations[annotation+"lower"]
}
func nodeManifest(n indexNode) oci.IndexOrManifest {
	entries := n.entries
	if entries == nil {
		entries = []oci.Descriptor{}
	}
	return oci.IndexOrManifest{SchemaVersion: 2, MediaType: oci.MediaTypeImageIndex, Manifests: entries, Annotations: map[string]string{annotation + "schema": "2", annotation + "kind": "key-index", annotation + "height": strconv.Itoa(n.height)}}
}
func (s *OCIStore) loadNode(ctx context.Context, repo string, d oci.Descriptor) (indexNode, error) {
	if err := ctx.Err(); err != nil {
		return indexNode{}, err
	}
	s.nodeMu.Lock()
	cached, exists := s.nodeCache[indexCacheKey{repo: repo, digest: d.Digest}]
	s.nodeMu.Unlock()
	if exists {
		if cached.size != d.Size {
			return indexNode{}, errors.New("cached index descriptor mismatch")
		}
		if err := validateNodeBoundary(d, cached.node); err != nil {
			return indexNode{}, err
		}
		return cloneIndexNode(cached.node), nil
	}
	m, err := s.readDigest(ctx, repo, d)
	if err != nil {
		return indexNode{}, err
	}
	h, err := strconv.Atoi(m.Annotations[annotation+"height"])
	if err != nil || h < 0 || h > maxIndexDepth || m.MediaType != oci.MediaTypeImageIndex || m.SchemaVersion != 2 || m.Annotations[annotation+"schema"] != "2" || m.Annotations[annotation+"kind"] != "key-index" {
		return indexNode{}, errors.New("invalid index node")
	}
	n := indexNode{height: h, entries: m.Manifests}
	if h > 0 && len(n.entries) == 0 {
		return n, errors.New("empty internal index")
	}
	for i, child := range n.entries {
		key := indexKey(child, h)
		if key == "" || (i > 0 && key <= indexKey(n.entries[i-1], h)) || child.Digest == "" || child.Size < 0 || child.Size > manifestLimit {
			return n, errors.New("invalid index ordering or descriptor")
		}
		if h > 0 {
			if child.MediaType != oci.MediaTypeImageIndex || child.Annotations[annotation+"height"] != strconv.Itoa(h-1) {
				return n, errors.New("invalid child index height")
			}
		} else {
			if child.MediaType != oci.MediaTypeImageManifest {
				return n, errors.New("invalid object descriptor")
			}
			if _, err := objectInfo(oci.IndexOrManifest{Annotations: child.Annotations}); err != nil {
				return n, fmt.Errorf("invalid listing metadata: %w", err)
			}
		}
	}
	if d.Annotations[annotation+"height"] != strconv.Itoa(h) {
		return n, errors.New("index height mismatch")
	}
	lower := ""
	if len(n.entries) > 0 {
		lower = indexKey(n.entries[0], h)
	}
	if d.Annotations[annotation+"lower"] != lower {
		return n, errors.New("index lower boundary mismatch")
	}
	s.cacheNode(repo, d, n)
	return cloneIndexNode(n), nil
}

func (s *OCIStore) saveNode(ctx context.Context, repo string, n indexNode) (oci.Descriptor, error) {
	raw, err := json.Marshal(nodeManifest(n))
	if err != nil {
		return oci.Descriptor{}, err
	}
	if len(raw) > s.indexBytes || len(n.entries) > s.indexEntries {
		return oci.Descriptor{}, errors.New("index node exceeds configured limit")
	}
	d, err := s.registry.PushManifest(ctx, repo, raw, oci.MediaTypeImageIndex, nil)
	if err != nil {
		return d, err
	}
	lower := ""
	if len(n.entries) > 0 {
		lower = indexKey(n.entries[0], n.height)
	}
	d.Annotations = map[string]string{annotation + "lower": lower, annotation + "height": strconv.Itoa(n.height)}
	s.cacheNode(repo, d, n)
	return d, nil
}
func (s *OCIStore) fitsNode(n indexNode) bool {
	if len(n.entries) > s.indexEntries {
		return false
	}
	raw, err := json.Marshal(nodeManifest(n))
	return err == nil && len(raw) <= s.indexBytes
}

// Split by bytes as well as count. A long common key prefix never prevents a
// split: the separator is the first full key in the right subtree.
func (s *OCIStore) saveSplit(ctx context.Context, repo string, n indexNode) ([]oci.Descriptor, error) {
	if s.fitsNode(n) {
		d, err := s.saveNode(ctx, repo, n)
		return []oci.Descriptor{d}, err
	}
	if len(n.entries) < 2 {
		return nil, errors.New("single index entry exceeds configured limit")
	}
	sizes := make([]int, len(n.entries))
	total := 0
	for i, d := range n.entries {
		raw, _ := json.Marshal(d)
		sizes[i] = len(raw)
		total += sizes[i]
	}
	midpoint, used := 1, 0
	for i, size := range sizes[:len(sizes)-1] {
		used += size
		midpoint = i + 1
		if used >= total/2 {
			break
		}
	}
	left, err := s.saveSplit(ctx, repo, indexNode{height: n.height, entries: n.entries[:midpoint]})
	if err != nil {
		return nil, err
	}
	right, err := s.saveSplit(ctx, repo, indexNode{height: n.height, entries: n.entries[midpoint:]})
	return append(left, right...), err
}
func childPosition(n indexNode, key string) int {
	return max(0, sort.Search(len(n.entries), func(i int) bool { return indexKey(n.entries[i], n.height) > key })-1)
}

func (s *OCIStore) mutateTree(ctx context.Context, repo string, d oci.Descriptor, key string, value *oci.Descriptor) ([]oci.Descriptor, error) {
	n, err := s.loadNode(ctx, repo, d)
	if err != nil {
		return nil, err
	}
	if n.height == 0 {
		pos := sort.Search(len(n.entries), func(i int) bool { return indexKey(n.entries[i], 0) >= key })
		present := pos < len(n.entries) && indexKey(n.entries[pos], 0) == key
		if value == nil {
			if !present {
				return []oci.Descriptor{d}, nil
			}
			n.entries = append(n.entries[:pos:pos], n.entries[pos+1:]...)
		} else if present {
			if n.entries[pos].Digest == value.Digest {
				return []oci.Descriptor{d}, nil
			}
			n.entries[pos] = *value
		} else {
			n.entries = append(n.entries, oci.Descriptor{})
			copy(n.entries[pos+1:], n.entries[pos:])
			n.entries[pos] = *value
		}
	} else {
		pos := childPosition(n, key)
		replacements, err := s.mutateTree(ctx, repo, n.entries[pos], key, value)
		if err != nil {
			return nil, err
		}
		if len(replacements) == 1 && replacements[0].Digest == n.entries[pos].Digest {
			return []oci.Descriptor{d}, nil
		}
		updated := append([]oci.Descriptor(nil), n.entries[:pos]...)
		updated = append(updated, replacements...)
		updated = append(updated, n.entries[pos+1:]...)
		n.entries = updated
		// Merge adjacent siblings when they fit; redistribute underfilled siblings
		// otherwise. This keeps deletion from leaving a tree of nearly empty nodes.
		if value == nil && len(n.entries) > 1 {
			first := max(0, min(pos, len(n.entries)-1)-1)
			left, err := s.loadNode(ctx, repo, n.entries[first])
			if err != nil {
				return nil, err
			}
			right, err := s.loadNode(ctx, repo, n.entries[first+1])
			if err != nil {
				return nil, err
			}
			joined := indexNode{height: left.height, entries: append(left.entries, right.entries...)}
			if s.fitsNode(joined) || len(left.entries) < s.indexEntries/4 || len(right.entries) < s.indexEntries/4 {
				merged, err := s.saveSplit(ctx, repo, joined)
				if err != nil {
					return nil, err
				}
				updated = append([]oci.Descriptor(nil), n.entries[:first]...)
				updated = append(updated, merged...)
				n.entries = append(updated, n.entries[first+2:]...)
			}
		}
	}
	if len(n.entries) == 0 {
		return nil, nil
	}
	return s.saveSplit(ctx, repo, n)
}
func (s *OCIStore) changeTree(ctx context.Context, repo string, tree oci.Descriptor, key string, value *oci.Descriptor) (oci.Descriptor, error) {
	if key == "" {
		return oci.Descriptor{}, errors.New("empty index key")
	}
	if value != nil {
		if value.Annotations[annotation+"key"] != key || value.MediaType != oci.MediaTypeImageManifest || value.Digest == "" || value.Size < 0 || value.Size > manifestLimit {
			return oci.Descriptor{}, errors.New("invalid new index entry")
		}
		if _, err := objectInfo(oci.IndexOrManifest{Annotations: value.Annotations}); err != nil {
			return oci.Descriptor{}, err
		}
	}
	nodes, err := s.mutateTree(ctx, repo, tree, key, value)
	if err != nil {
		return oci.Descriptor{}, err
	}
	if len(nodes) == 0 {
		return s.saveNode(ctx, repo, indexNode{})
	}
	for len(nodes) > 1 {
		height, _ := strconv.Atoi(nodes[0].Annotations[annotation+"height"])
		if height >= maxIndexDepth {
			return oci.Descriptor{}, errors.New("index depth limit exceeded")
		}
		nodes, err = s.saveSplit(ctx, repo, indexNode{height: height + 1, entries: nodes})
		if err != nil {
			return oci.Descriptor{}, err
		}
	}
	result := nodes[0]
	for {
		n, err := s.loadNode(ctx, repo, result)
		if err != nil {
			return result, err
		}
		if n.height == 0 || len(n.entries) != 1 {
			return result, nil
		}
		result = n.entries[0]
	}
}

func (s *OCIStore) findEntry(ctx context.Context, repo string, tree oci.Descriptor, key string) (oci.Descriptor, error) {
	for depth := 0; depth <= maxIndexDepth; depth++ {
		n, err := s.loadNode(ctx, repo, tree)
		if err != nil {
			return oci.Descriptor{}, err
		}
		if n.height == 0 {
			pos := sort.Search(len(n.entries), func(i int) bool { return indexKey(n.entries[i], 0) >= key })
			if pos == len(n.entries) || indexKey(n.entries[pos], 0) != key {
				return oci.Descriptor{}, ErrNoKey
			}
			return n.entries[pos], nil
		}
		tree = n.entries[childPosition(n, key)]
	}
	return oci.Descriptor{}, errors.New("index depth limit exceeded")
}

func rootManifest(root bucketIndex) oci.IndexOrManifest {
	objects, completions, versions := root.objects, root.completions, root.versions
	objects.Annotations = cloneStrings(objects.Annotations)
	objects.Annotations[annotation+"tree"] = "objects"
	completions.Annotations = cloneStrings(completions.Annotations)
	completions.Annotations[annotation+"tree"] = "completions"
	versions.Annotations = cloneStrings(versions.Annotations)
	versions.Annotations[annotation+"tree"] = "versions"
	return oci.IndexOrManifest{SchemaVersion: 2, MediaType: oci.MediaTypeImageIndex, Manifests: []oci.Descriptor{objects, completions, versions}, Annotations: map[string]string{annotation + "schema": "2", annotation + "kind": "bucket-root", annotation + "generation": id()}}
}
func (s *OCIStore) commitIndex(ctx context.Context, repo string, root bucketIndex) (oci.Descriptor, error) {
	raw, err := json.Marshal(rootManifest(root))
	if err != nil {
		return oci.Descriptor{}, err
	}
	params := &oci.PushManifestParameters{Tags: []string{bucketIndexTag}}
	if root.descriptor.Digest != "" {
		params.IfMatch = strconv.Quote(root.descriptor.Digest.String())
	}
	return s.registry.PushManifest(ctx, repo, raw, oci.MediaTypeImageIndex, params)
}

// Reject backends that ignore If-Match. Use an identical immutable root body
// with a deliberately mismatched validator, so even a broken backend cannot
// change the logical tree. GetTag first also performs remote ETag discovery.
func (s *OCIStore) verifyIndexConditions(ctx context.Context, repo string) error {
	if s.indexVerified[repo] {
		return nil
	}
	r, err := s.registry.GetTag(ctx, repo, bucketIndexTag)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(r, manifestLimit+1))
	r.Close()
	if err != nil {
		return err
	}
	_, err = s.registry.PushManifest(ctx, repo, raw, oci.MediaTypeImageIndex, &oci.PushManifestParameters{Tags: []string{bucketIndexTag}, IfMatch: strconv.Quote("sha256:" + strings.Repeat("0", 64))})
	if !errors.Is(err, oci.ErrManifestInvalid) {
		if err != nil {
			return fmt.Errorf("verify conditional index updates: %w", err)
		}
		return errors.New("OCI backend does not enforce conditional manifest updates")
	}
	s.indexVerified[repo] = true
	return nil
}

func objectDescriptor(d oci.Descriptor, m oci.IndexOrManifest) oci.Descriptor {
	d.Annotations = map[string]string{}
	for _, name := range []string{"key", "size", "etag", "created", "modified", "generation", "commit-id", "version-id", "deleted"} {
		if value := m.Annotations[annotation+name]; value != "" {
			d.Annotations[annotation+name] = value
		}
	}
	return d
}

// initializeIndex runs only for a fresh, unpublished bucket repository.
func (s *OCIStore) initializeIndex(ctx context.Context, repo string) error {
	empty, err := s.saveNode(ctx, repo, indexNode{})
	if err != nil {
		return err
	}
	if _, err := s.commitIndex(ctx, repo, bucketIndex{objects: empty, completions: empty, versions: empty}); err != nil {
		return err
	}
	return s.verifyIndexConditions(ctx, repo)
}

func (s *OCIStore) ensureIndex(ctx context.Context, repo string) error {
	if _, err := s.readIndex(ctx, repo); err != nil {
		return fmt.Errorf("read bucket index: %w", err)
	}
	return s.verifyIndexConditions(ctx, repo)
}
