package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ohseeeye/oci"
	"github.com/ohseeeye/oci/ocilayout"
	"github.com/ohseeeye/oci/ocimem"
)

func indexFixture(t *testing.T, reg oci.Registry) *OCIStore {
	t.Helper()
	s, err := newOCIStore(reg, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateBucket(context.Background(), Scope{Tenant: "local"}, "bucket"); err != nil {
		t.Fatal(err)
	}
	return s
}
func indexRepo(t *testing.T, s *OCIStore) string {
	t.Helper()
	b, err := s.bucket(context.Background(), Scope{Tenant: "local"}, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	return b.Annotations[annotation+"repo"]
}
func putIndexKey(t *testing.T, s *OCIStore, key string) {
	t.Helper()
	_, err := s.PutObject(context.Background(), Scope{Tenant: "local"}, PutRequest{Bucket: "bucket", Key: key, Size: -1}, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
}
func checkIndexTree(t *testing.T, s *OCIStore, repo string, d oci.Descriptor) []string {
	t.Helper()
	n, err := s.loadNode(context.Background(), repo, d)
	if err != nil {
		t.Fatal(err)
	}
	if d.Size > int64(s.indexBytes) || len(n.entries) > s.indexEntries {
		t.Fatalf("oversized node: %d bytes / %d entries", d.Size, len(n.entries))
	}
	var keys []string
	if n.height == 0 {
		for _, entry := range n.entries {
			keys = append(keys, indexKey(entry, 0))
		}
	} else {
		for _, child := range n.entries {
			keys = append(keys, checkIndexTree(t, s, repo, child)...)
		}
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("unordered subtree: %q >= %q", keys[i-1], keys[i])
		}
	}
	return keys
}

func TestIndexSplitMergeAndReopen(t *testing.T) {
	for _, disk := range []bool{false, true} {
		name, count, minHeight := "memory", 80, 2
		if disk {
			name, count, minHeight = "disk", 8, 1
		}
		t.Run(name, func(t *testing.T) {
			ctx, scope := context.Background(), Scope{Tenant: "local"}
			dir := t.TempDir()
			var reg oci.Registry = ocimem.New()
			var err error
			if disk {
				reg, err = ocilayout.New(dir, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			s := indexFixture(t, reg)
			s.indexEntries = 4
			var keys []string
			for i := 0; i < count; i++ {
				keys = append(keys, fmt.Sprintf("same/prefix/%03d", (i*37)%count))
			}
			keys = append(keys, "../key", "plus+space key", "日本語/", "same/prefix")
			for _, key := range keys {
				putIndexKey(t, s, key)
			}
			sort.Strings(keys)
			repo := indexRepo(t, s)
			root, err := s.readIndex(ctx, repo)
			if err != nil {
				t.Fatal(err)
			}
			node, err := s.loadNode(ctx, repo, root.objects)
			if err != nil {
				t.Fatal(err)
			}
			if node.height < minHeight {
				t.Fatalf("did not force recursive splits: height %d", node.height)
			}
			got := checkIndexTree(t, s, repo, root.objects)
			if fmt.Sprint(got) != fmt.Sprint(keys) {
				t.Fatalf("tree keys differ: %v", got)
			}
			for tag, err := range reg.Tags(ctx, repo, nil) {
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(tag, "obj-") {
					t.Fatal("new object acquired a tag")
				}
			}
			// An overwrite changes the generation while retaining the original creation time.
			_, before, err := s.indexedObject(ctx, repo, keys[0])
			if err != nil {
				t.Fatal(err)
			}
			putIndexKey(t, s, keys[0])
			_, after, err := s.indexedObject(ctx, repo, keys[0])
			if err != nil {
				t.Fatal(err)
			}
			if before.Annotations[annotation+"created"] != after.Annotations[annotation+"created"] || before.Annotations[annotation+"generation"] == after.Annotations[annotation+"generation"] {
				t.Fatal("overwrite metadata lost creation/generation")
			}
			if disk {
				reg, err = ocilayout.New(dir, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			s, err = newOCIStore(reg, OCIOptions{})
			if err != nil {
				t.Fatal(err)
			}
			s.indexEntries = 4
			listed, err := s.ListObjects(ctx, scope, ListRequest{Bucket: "bucket"})
			if err != nil || len(listed) != len(keys) {
				t.Fatalf("reopen listing: %v %v", listed, err)
			}
			for _, key := range keys {
				o, err := s.GetObject(ctx, scope, "bucket", key)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(o.Body)
				o.Body.Close()
				if err != nil || string(data) != "payload" {
					t.Fatalf("read %q: %q %v", key, data, err)
				}
			}
			// Successive deletions exercise sibling merging and root collapse.
			for i := 0; i < len(keys); i++ {
				key := keys[i]
				if err := s.DeleteObject(ctx, scope, "bucket", key); err != nil {
					t.Fatal(err)
				}
				root, err = s.readIndex(ctx, repo)
				if err != nil {
					t.Fatal(err)
				}
				remaining := checkIndexTree(t, s, repo, root.objects)
				if len(remaining) != len(keys)-i-1 {
					t.Fatal("delete lost neighboring keys")
				}
			}
			node, err = s.loadNode(ctx, repo, root.objects)
			if err != nil || node.height != 0 || len(node.entries) != 0 {
				t.Fatalf("root did not collapse: %+v %v", node, err)
			}
			if err := s.DeleteBucket(ctx, scope, "bucket"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexSplitsByBytes(t *testing.T) {
	s := indexFixture(t, ocimem.New())
	s.indexBytes = 6000
	for i := 0; i < 24; i++ {
		putIndexKey(t, s, strings.Repeat("p", 700)+fmt.Sprintf("/%03d", i))
	}
	repo := indexRepo(t, s)
	root, err := s.readIndex(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.loadNode(context.Background(), repo, root.objects)
	if err != nil || n.height == 0 {
		t.Fatalf("byte threshold did not split: %v", err)
	}
	if keys := checkIndexTree(t, s, repo, root.objects); len(keys) != 24 {
		t.Fatalf("byte split lost keys: %d", len(keys))
	}
}

type observedIndexRegistry struct {
	oci.Registry
	denyTags                bool
	indexReads, objectReads atomic.Int32
}

func (r *observedIndexRegistry) Tags(ctx context.Context, repo string, p *oci.TagsParameters) iter.Seq2[string, error] {
	if r.denyTags {
		return func(yield func(string, error) bool) { yield("", errors.New("tag enumeration forbidden")) }
	}
	return r.Registry.Tags(ctx, repo, p)
}
func (r *observedIndexRegistry) GetManifest(ctx context.Context, repo string, d oci.Digest) (oci.BlobReader, error) {
	reader, err := r.Registry.GetManifest(ctx, repo, d)
	if err == nil {
		if reader.Descriptor().MediaType == oci.MediaTypeImageIndex {
			r.indexReads.Add(1)
		} else {
			r.objectReads.Add(1)
		}
	}
	return reader, err
}
func TestIndexListingSeeksAndSkipsPrefixes(t *testing.T) {
	reg := &observedIndexRegistry{Registry: ocimem.New()}
	s := indexFixture(t, reg)
	s.indexEntries = 4
	for i := 0; i < 160; i++ {
		putIndexKey(t, s, fmt.Sprintf("folder/%03d", i))
	}
	for _, key := range []string{"tail/a", "tail/b", "z"} {
		putIndexKey(t, s, key)
	}
	reg.denyTags = true
	s.nodeMu.Lock()
	s.nodeCache = make(map[indexCacheKey]cachedIndexNode)
	s.nodeCacheBytes = 0
	s.nodeMu.Unlock()
	reg.indexReads.Store(0)
	reg.objectReads.Store(0)
	objects, err := s.ListObjects(context.Background(), Scope{Tenant: "local"}, ListRequest{Bucket: "bucket", Delimiter: "/", Limit: 2})
	if err != nil || len(objects) != 2 || !strings.HasPrefix(objects[0].Key, "folder/") || !strings.HasPrefix(objects[1].Key, "tail/") {
		t.Fatalf("delimiter traversal: %v %v", objects, err)
	}
	if reg.objectReads.Load() != 0 || reg.indexReads.Load() > 20 {
		t.Fatalf("listing fetched objects or scanned all leaves: objects=%d indexes=%d", reg.objectReads.Load(), reg.indexReads.Load())
	}
	reg.indexReads.Store(0)
	objects, err = s.ListObjects(context.Background(), Scope{Tenant: "local"}, ListRequest{Bucket: "bucket", Prefix: "folder/", After: "folder/150", Limit: 3})
	if err != nil || len(objects) != 3 || objects[0].Key != "folder/151" || objects[2].Key != "folder/153" {
		t.Fatalf("marker seek: %v %v", objects, err)
	}
	if reg.indexReads.Load() > 12 {
		t.Fatalf("marker scanned preceding leaves: %d", reg.indexReads.Load())
	}
	// HEAD/GET follow the same root and never resolve an object tag.
	if _, err := s.HeadObject(context.Background(), Scope{Tenant: "local"}, "bucket", "tail/a"); err != nil {
		t.Fatal(err)
	}
}

type ignoreConditionsRegistry struct{ oci.Registry }

func (r ignoreConditionsRegistry) PushManifest(ctx context.Context, repo string, raw []byte, media string, p *oci.PushManifestParameters) (oci.Descriptor, error) {
	if p != nil {
		copy := *p
		copy.IfMatch = ""
		p = &copy
	}
	return r.Registry.PushManifest(ctx, repo, raw, media, p)
}
func TestIndexRequiresConditionalUpdates(t *testing.T) {
	reg := ocimem.New()
	s := indexFixture(t, reg)
	putIndexKey(t, s, "key")
	if _, err := newOCIStore(ignoreConditionsRegistry{Registry: reg}, OCIOptions{}); err == nil {
		t.Fatal("accepted backend that ignores If-Match")
	}
	bad, err := newOCIStore(ignoreConditionsRegistry{Registry: ocimem.New()}, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.CreateBucket(context.Background(), Scope{Tenant: "local"}, "bucket"); err == nil {
		t.Fatal("created bucket on unsafe backend")
	}
	if err := bad.HeadBucket(context.Background(), Scope{Tenant: "local"}, "bucket"); !errors.Is(err, ErrNoBucket) {
		t.Fatalf("unsafe bucket became visible: %v", err)
	}
}

type collidingIndexRegistry struct {
	oci.Registry
	mu        sync.Mutex
	remaining int
	arrived   chan struct{}
	release   chan struct{}
	conflicts atomic.Int32
}

func (r *collidingIndexRegistry) PushManifest(ctx context.Context, repo string, raw []byte, media string, p *oci.PushManifestParameters) (oci.Descriptor, error) {
	wait := false
	if p != nil && p.IfMatch != "" && p.IfMatch != strconv.Quote("sha256:"+strings.Repeat("0", 64)) {
		r.mu.Lock()
		if r.remaining > 0 {
			r.remaining--
			wait = true
		}
		r.mu.Unlock()
	}
	if wait {
		r.arrived <- struct{}{}
		select {
		case <-r.release:
		case <-ctx.Done():
			return oci.Descriptor{}, ctx.Err()
		}
	}
	d, err := r.Registry.PushManifest(ctx, repo, raw, media, p)
	if p != nil && p.IfMatch != "" && errors.Is(err, oci.ErrManifestInvalid) {
		r.conflicts.Add(1)
	}
	return d, err
}
func TestIndexCASRetriesPreserveBothWriters(t *testing.T) {
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	reg := &collidingIndexRegistry{Registry: ocimem.New(), arrived: make(chan struct{}, 2), release: make(chan struct{})}
	first := indexFixture(t, reg)
	second, err := newOCIStore(reg, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reg.conflicts.Store(0)
	reg.remaining = 2
	errs := make(chan error, 2)
	for i, s := range []*OCIStore{first, second} {
		go func(s *OCIStore, key string) {
			_, err := s.PutObject(ctx, scope, PutRequest{Bucket: "bucket", Key: key, Size: -1}, strings.NewReader(key))
			errs <- err
		}(s, fmt.Sprintf("key-%d", i))
	}
	for i := 0; i < 2; i++ {
		select {
		case <-reg.arrived:
		case <-time.After(5 * time.Second):
			close(reg.release)
			t.Fatal("writers did not reach CAS barrier")
		}
	}
	close(reg.release)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if reg.conflicts.Load() == 0 {
		t.Fatal("test did not force a stale root condition")
	}
	for _, s := range []*OCIStore{first, second} {
		objects, err := s.ListObjects(ctx, scope, ListRequest{Bucket: "bucket"})
		if err != nil || len(objects) != 2 {
			t.Fatalf("lost concurrent write: %v %v", objects, err)
		}
		versions, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket"})
		if err != nil || len(versions) != 2 || !versions[0].IsLatest || !versions[1].IsLatest {
			t.Fatalf("lost concurrent history: %v %v", versions, err)
		}

	}
}

func TestIndexFailureKeepsOldSnapshot(t *testing.T) {
	for _, kind := range []string{"object", "key-index", "bucket-root"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", kind, after), func(t *testing.T) {
				ctx, scope := context.Background(), Scope{Tenant: "local"}
				reg := &failingRegistry{Registry: ocimem.New()}
				s := indexFixture(t, reg)
				putIndexKey(t, s, "key")
				repo := indexRepo(t, s)
				before, err := s.readIndex(ctx, repo)
				if err != nil {
					t.Fatal(err)
				}
				reg.kind, reg.after, reg.fail = kind, after, true
				if _, err := s.PutObject(ctx, scope, PutRequest{Bucket: "bucket", Key: "key", Size: -1}, strings.NewReader("replacement")); err == nil {
					t.Fatal("expected failure")
				}
				reg.fail = false
				root, err := s.readIndex(ctx, repo)
				if err != nil {
					t.Fatal(err)
				}
				published := kind == "bucket-root" && after
				if (root.descriptor.Digest != before.descriptor.Digest) != published {
					t.Fatal("uncommitted artifacts changed visibility")
				}
				object, err := s.GetObject(ctx, scope, "bucket", "key")
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(object.Body)
				object.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				want := "payload"
				if published {
					want = "replacement"
				}
				if string(data) != want {
					t.Fatalf("visible bytes: %q want %q", data, want)
				}
				listed, err := s.ListObjects(ctx, scope, ListRequest{Bucket: "bucket"})
				if err != nil || len(listed) != 1 || listed[0].ETag != object.ETag {
					t.Fatal("listing and GET selected different generations")
				}
				versions, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket"})
				wantVersions := 1
				if published {
					wantVersions = 2
				}
				if err != nil || len(versions) != wantVersions || versions[0].ETag != object.ETag || !versions[0].IsLatest {
					t.Fatalf("history commit differed: %v %v", versions, err)
				}

			})
		}
	}
}

func TestCompletionReceiptSurvivesRivalDeletion(t *testing.T) {
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	s, u := multipartFixture(t)
	reg := s.registry
	part, err := s.UploadPart(ctx, scope, UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	completion := CompleteMultipartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Parts: []CompletedPart{{Number: 1, ETag: part.ETag}}, MaxSize: -1}
	s.registry = &failingRegistry{Registry: reg, kind: "completed-upload", fail: true}
	if _, err := s.CompleteMultipartUpload(ctx, scope, completion); err == nil {
		t.Fatal("expected final record failure")
	}
	s.registry = reg
	repo := indexRepo(t, s)
	_, state, err := s.indexedObject(ctx, repo, "key")
	if err != nil {
		t.Fatal(err)
	}
	// A root receipt is authoritative even when the mutable upload record still
	// says completing. A rival write cannot erase proof of completion.
	if err := s.deleteIndexedObject(ctx, repo, "key"); err != nil {
		t.Fatal(err)
	}
	recovered, err := newOCIStore(reg, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := recovered.CompleteMultipartUpload(ctx, scope, completion)
	if err != nil || result.ETag != state.Annotations[annotation+"etag"] {
		t.Fatalf("receipt recovery: %v %v", result, err)
	}
	if _, err := recovered.HeadObject(ctx, scope, "bucket", "key"); !errors.Is(err, ErrNoKey) {
		t.Fatal("recovery resurrected rival deletion")
	}
}

func TestIndexRejectsCorruptNodeOrdering(t *testing.T) {
	s := indexFixture(t, ocimem.New())
	putIndexKey(t, s, "a")
	putIndexKey(t, s, "b")
	repo := indexRepo(t, s)
	ctx := context.Background()
	root, err := s.readIndex(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	node, err := s.loadNode(ctx, repo, root.objects)
	if err != nil {
		t.Fatal(err)
	}
	node.entries[0], node.entries[1] = node.entries[1], node.entries[0]
	raw, err := json.Marshal(nodeManifest(node))
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.registry.PushManifest(ctx, repo, raw, oci.MediaTypeImageIndex, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.Annotations = map[string]string{annotation + "height": "0", annotation + "lower": "b"}
	root.objects = d
	if _, err := s.commitIndex(ctx, repo, root); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListObjects(ctx, Scope{Tenant: "local"}, ListRequest{Bucket: "bucket"}); err == nil {
		t.Fatal("corrupt ordering silently listed")
	}
}
