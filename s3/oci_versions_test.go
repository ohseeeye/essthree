package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ohseeeye/oci"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ohseeeye/oci/ocimem"
)

func TestVersionHistoryListingAndReopen(t *testing.T) {
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	reg := &observedIndexRegistry{Registry: ocimem.New()}
	s := indexFixture(t, reg)
	s.indexEntries = 3
	var ids []string
	for i := 0; i < 9; i++ {
		object, err := s.PutObject(ctx, scope, PutRequest{Bucket: "bucket", Key: "a", Size: -1}, strings.NewReader(fmt.Sprint(i)))
		if err != nil {
			t.Fatal(err)
		}
		if object.VersionID == "" {
			t.Fatal("missing version ID")
		}
		ids = append(ids, object.VersionID)
	}
	if err := s.DeleteObject(ctx, scope, "bucket", "a"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a/child", "folder/a", "folder/b", "日本語 +"} {
		putIndexKey(t, s, key)
	}
	s, err := newOCIStore(reg, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reg.denyTags = true
	reg.objectReads.Store(0)
	versions, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket"})
	if err != nil || len(versions) != 14 {
		t.Fatalf("versions: %d %v", len(versions), err)
	}
	if !versions[0].IsLatest || !versions[0].DeleteMarker || versions[0].Key != "a" {
		t.Fatalf("latest: %+v", versions[0])
	}
	for i := 0; i < 9; i++ {
		if versions[i+1].VersionID != ids[8-i] || versions[i+1].IsLatest {
			t.Fatalf("version order: %+v", versions[i+1])
		}
	}
	if reg.objectReads.Load() != 0 {
		t.Fatal("listing read object manifests")
	}
	var paged []ObjectVersion
	p := VersionListRequest{Bucket: "bucket", Limit: 2}
	for {
		page, err := s.ListObjectVersions(ctx, scope, p)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		last := page[len(page)-1]
		p.KeyMarker, p.VersionIDMarker = last.Key, last.VersionID
	}
	if fmt.Sprint(paged) != fmt.Sprint(versions) {
		t.Fatal("marker pagination changed versions")
	}
	grouped, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket", Delimiter: "/", KeyMarker: "a"})
	if err != nil || len(grouped) != 3 || grouped[0].CommonPrefix != "a/" || grouped[1].CommonPrefix != "folder/" {
		t.Fatalf("grouped: %v %v", grouped, err)
	}
	next, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket", Delimiter: "/", KeyMarker: "a/"})
	if err != nil || len(next) != 2 || next[0].CommonPrefix != "folder/" {
		t.Fatalf("group marker: %v %v", next, err)
	}
	keys, err := s.ListObjects(ctx, scope, ListRequest{Bucket: "bucket"})
	if err != nil || len(keys) != 4 || keys[0].Key != "a/child" {
		t.Fatalf("live keys: %v %v", keys, err)
	}
	if _, err := s.HeadObject(ctx, scope, "bucket", "a"); !errors.Is(err, ErrNoKey) {
		t.Fatal("delete marker exposed live key")
	}
	if _, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket", KeyMarker: "a", VersionIDMarker: ids[0] + "bad"}); !errors.Is(err, ErrInvalidVersionMarker) {
		t.Fatal("invalid version marker accepted")
	}
	reg.denyTags = false
	putIndexKey(t, s, "a")
	newest, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket", Prefix: "a", Limit: 2})
	if err != nil || len(newest) != 2 || newest[0].DeleteMarker || !newest[0].IsLatest || !newest[1].DeleteMarker || newest[1].IsLatest {
		t.Fatalf("recreated: %v %v", newest, err)
	}
}

func TestMissingIndexIsNotRecreated(t *testing.T) {
	s := indexFixture(t, ocimem.New())
	repo := indexRepo(t, s)
	if err := s.registry.DeleteTag(context.Background(), repo, bucketIndexTag); err != nil {
		t.Fatal(err)
	}
	if _, err := newOCIStore(s.registry, OCIOptions{}); err == nil {
		t.Fatal("missing root was recreated")
	}
}

type failingVersionRegistry struct {
	oci.Registry
	fail, after bool
}

func (r *failingVersionRegistry) PushManifest(ctx context.Context, repo string, raw []byte, media string, p *oci.PushManifestParameters) (oci.Descriptor, error) {
	var node oci.IndexOrManifest
	if err := json.Unmarshal(raw, &node); err != nil {
		return oci.Descriptor{}, err
	}
	if r.fail && node.Annotations[annotation+"kind"] == "key-index" {
		for _, d := range node.Manifests {
			if d.Annotations[annotation+"version-key"] != "" {
				if r.after {
					if _, err := r.Registry.PushManifest(ctx, repo, raw, media, p); err != nil {
						return oci.Descriptor{}, err
					}
				}
				return oci.Descriptor{}, errors.New("injected version publication failure")
			}
		}
	}
	return r.Registry.PushManifest(ctx, repo, raw, media, p)
}
func TestVersionPublicationFailureIsInvisible(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			ctx, scope := context.Background(), Scope{Tenant: "local"}
			reg := &failingVersionRegistry{Registry: ocimem.New(), after: after}
			s := indexFixture(t, reg)
			putIndexKey(t, s, "key")
			reg.fail = true
			if _, err := s.PutObject(ctx, scope, PutRequest{Bucket: "bucket", Key: "key", Size: -1}, strings.NewReader("uncommitted")); err == nil {
				t.Fatal("expected version failure")
			}
			reg.fail = false
			versions, err := s.ListObjectVersions(ctx, scope, VersionListRequest{Bucket: "bucket"})
			if err != nil || len(versions) != 1 {
				t.Fatalf("uncommitted history visible: %v %v", versions, err)
			}
			object, err := s.HeadObject(ctx, scope, "bucket", "key")
			if err != nil || object.VersionID != versions[0].VersionID {
				t.Fatal("current object changed without history")
			}
		})
	}
}
func TestVersionListQueryValidation(t *testing.T) {
	h, err := NewHandler(ocimem.New(), Options{DevelopmentMode: true})
	if err != nil {
		t.Fatal(err)
	}
	s := h.store.(*OCIStore)
	if err := s.CreateBucket(context.Background(), Scope{Tenant: "local"}, "bucket"); err != nil {
		t.Fatal(err)
	}
	putIndexKey(t, s, "key")
	for _, q := range []string{"versions&versions", "versions&max-keys=-1", "versions&max-keys=x", "versions&encoding-type=other", "versions&version-id-marker=missing", "versions&key-marker=key&version-id-marker=unknown", "versions&prefix=a&prefix=b"} {
		r := httptest.NewRequest(http.MethodGet, "/bucket?"+q, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("%s: %d %s", q, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/bucket?versions&max-keys=0", "/bucket?versions&prefix=absent"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != 200 || strings.Contains(w.Body.String(), "<Version>") || !strings.Contains(w.Body.String(), "<IsTruncated>false</IsTruncated>") {
			t.Fatalf("empty page: %d %s", w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/missing?versions", nil))
	if w.Code != 404 {
		t.Fatal("missing bucket accepted")
	}
}
