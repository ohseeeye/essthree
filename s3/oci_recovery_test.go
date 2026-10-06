package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ohseeeye/oci"
	"github.com/ohseeeye/oci/ocisqlite"
)

type failingRegistry struct {
	oci.Registry
	kind  string
	after bool
	fail  bool
}

func (r *failingRegistry) PushManifest(ctx context.Context, repo string, raw []byte, media string, params *oci.PushManifestParameters) (oci.Descriptor, error) {
	var m oci.IndexOrManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return oci.Descriptor{}, err
	}
	if r.fail && m.Annotations[annotation+"kind"] == r.kind {
		if r.after {
			if _, err := r.Registry.PushManifest(ctx, repo, raw, media, params); err != nil {
				return oci.Descriptor{}, err
			}
		}
		return oci.Descriptor{}, errors.New("injected publication failure")
	}
	return r.Registry.PushManifest(ctx, repo, raw, media, params)
}

func TestCompletionRecovery(t *testing.T) {
	for _, kind := range []string{"completing-upload", "object", "key-index", "bucket-root", "completed-upload"} {
		for _, after := range []bool{false, true} {
			for _, reopen := range []bool{false, true} {
				name := kind
				if after {
					name += "/after"
				} else {
					name += "/before"
				}
				if reopen {
					name += "/reopen"
				} else {
					name += "/same-process"
				}
				t.Run(name, func(t *testing.T) {
					ctx, scope := context.Background(), Scope{Tenant: "local"}
					dir := t.TempDir()
					reg, err := ocisqlite.New(dir, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer reg.Close()
					fault := &failingRegistry{Registry: reg, kind: kind, after: after}
					s, err := newOCIStore(fault, OCIOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if err := s.CreateBucket(ctx, scope, "bucket"); err != nil {
						t.Fatal(err)
					}
					p := PutRequest{Bucket: "bucket", Key: "key", Size: -1}
					if _, err := s.PutObject(ctx, scope, p, strings.NewReader("old")); err != nil {
						t.Fatal(err)
					}
					u, err := s.CreateMultipartUpload(ctx, scope, p)
					if err != nil {
						t.Fatal(err)
					}
					part, err := s.UploadPart(ctx, scope, UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}, strings.NewReader("multipart"))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := s.ListObjects(ctx, scope, ListRequest{Bucket: "bucket"}); err != nil {
						t.Fatal(err)
					}
					completion := CompleteMultipartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Parts: []CompletedPart{{Number: 1, ETag: part.ETag}}, MaxSize: 100}
					fault.fail = true
					if _, err := s.CompleteMultipartUpload(ctx, scope, completion); err == nil {
						t.Fatal("expected injected failure")
					}
					if (kind == "object" || kind == "key-index" || kind == "bucket-root" || kind == "completed-upload") && !after {
						// A frozen upload blocks later writes while reconciliation fails.
						if _, err := s.PutObject(ctx, scope, p, strings.NewReader("blocked")); err == nil {
							t.Fatal("uncertain completion allowed a conflicting write")
						}
					}
					fault.fail = false
					if reopen {
						reg, err = ocisqlite.New(dir, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer reg.Close()
						s, err = newOCIStore(reg, OCIOptions{})
						if err != nil {
							t.Fatal(err)
						}
					}
					result, err := s.CompleteMultipartUpload(ctx, scope, completion)
					if err != nil {
						t.Fatal(err)
					}
					listed, err := s.ListObjects(ctx, scope, ListRequest{Bucket: "bucket"})
					if err != nil || len(listed) != 1 || listed[0].ETag != result.ETag {
						t.Fatalf("stale listing: %v %v", listed, err)
					}
					if _, err := s.PutObject(ctx, scope, p, strings.NewReader("later")); err != nil {
						t.Fatal(err)
					}
					replayed, err := s.CompleteMultipartUpload(ctx, scope, completion)
					if err != nil || replayed.ETag != result.ETag || !replayed.Modified.Equal(result.Modified) {
						t.Fatalf("replay: %v %v", replayed, err)
					}
					got, err := s.GetObject(ctx, scope, "bucket", "key")
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(got.Body)
					got.Body.Close()
					if err != nil || string(data) != "later" {
						t.Fatalf("retry overwrote later write: %q %v", data, err)
					}
					if err := s.DeleteObject(ctx, scope, "bucket", "key"); err != nil {
						t.Fatal(err)
					}
					if _, err := s.CompleteMultipartUpload(ctx, scope, completion); err != nil {
						t.Fatal(err)
					}
					if _, err := s.HeadObject(ctx, scope, "bucket", "key"); !errors.Is(err, ErrNoKey) {
						t.Fatalf("retry resurrected deleted object: %v", err)
					}
					completion.Parts[0].ETag = "wrong"
					if _, err := s.CompleteMultipartUpload(ctx, scope, completion); !errors.Is(err, ErrInvalidPart) {
						t.Fatalf("changed completion: %v", err)
					}
				})
			}
		}
	}
}

type missingPartRegistry struct{ oci.Registry }

func (r missingPartRegistry) ResolveBlob(context.Context, string, oci.Digest) (oci.Descriptor, error) {
	return oci.Descriptor{}, oci.ErrBlobUnknown
}

func TestRecoveryRejectsCorruptionAndLaterGeneration(t *testing.T) {
	for _, problem := range []string{"missing-part", "bad-etag", "later-generation"} {
		t.Run(problem, func(t *testing.T) {
			s, u := multipartFixture(t)
			ctx, scope := context.Background(), Scope{Tenant: "local"}
			part, err := s.UploadPart(ctx, scope, UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}, strings.NewReader("part"))
			if err != nil {
				t.Fatal(err)
			}
			reg := s.registry
			s.registry = &failingRegistry{Registry: reg, kind: "object", fail: true}
			if _, err := s.CompleteMultipartUpload(ctx, scope, CompleteMultipartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Parts: []CompletedPart{{Number: 1, ETag: part.ETag}}, MaxSize: -1}); err == nil {
				t.Fatal("expected failure")
			}
			s.registry = reg
			b, err := s.bucket(ctx, scope, "bucket")
			if err != nil {
				t.Fatal(err)
			}
			repo := b.Annotations[annotation+"repo"]
			state, err := s.read(ctx, repo, "upload-"+hash(u.UploadID))
			if err != nil {
				t.Fatal(err)
			}
			switch problem {
			case "missing-part":
				reg = missingPartRegistry{Registry: reg}
			case "bad-etag":
				state.Annotations[annotation+"etag"] = "corrupt"
				if err := s.publish(ctx, repo, "upload-"+hash(u.UploadID), "completing-upload", state.Annotations, state.Layers); err != nil {
					t.Fatal(err)
				}
			case "later-generation":
				// Simulate a rival commit bypassing this store's recovery guard.
				if err := s.putIndexedObject(ctx, repo, objectAnnotations("key", part.Size, part.ETag, time.Now(), nil, nil), state.Layers); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := newOCIStore(reg, OCIOptions{}); err == nil {
				t.Fatal("startup accepted unsafe completion")
			}
			if _, err := s.HeadObject(ctx, scope, "bucket", "key"); problem != "later-generation" && !errors.Is(err, ErrNoKey) {
				t.Fatalf("recovery published unsafe object: %v", err)
			}
		})
	}
}
