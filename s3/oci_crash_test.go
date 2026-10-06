package s3

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/ohseeeye/oci"
	"github.com/ohseeeye/oci/ocisqlite"
)

type crashRegistry struct {
	oci.Registry
	kind string
}

func (r crashRegistry) PushManifest(ctx context.Context, repo string, raw []byte, media string, params *oci.PushManifestParameters) (oci.Descriptor, error) {
	d, err := r.Registry.PushManifest(ctx, repo, raw, media, params)
	if err != nil {
		return d, err
	}
	var m oci.IndexOrManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return d, err
	}
	if m.Annotations[annotation+"kind"] == r.kind {
		os.Exit(77)
	}
	return d, nil
}

func TestCompletionProcessInterruption(t *testing.T) {
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	if dir := os.Getenv("ESSTHREE_CRASH_TEST_DIR"); dir != "" {
		reg, err := ocisqlite.New(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer reg.Close()
		fault := &crashRegistry{Registry: reg}
		s, err := newOCIStore(fault, OCIOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CreateBucket(ctx, scope, "bucket"); err != nil {
			t.Fatal(err)
		}
		u, err := s.CreateMultipartUpload(ctx, scope, PutRequest{Bucket: "bucket", Key: "key"})
		if err != nil {
			t.Fatal(err)
		}
		part, err := s.UploadPart(ctx, scope, UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}, strings.NewReader("complete bytes"))
		if err != nil {
			t.Fatal(err)
		}
		fault.kind = os.Getenv("ESSTHREE_CRASH_TEST_KIND")
		if _, err := s.CompleteMultipartUpload(ctx, scope, CompleteMultipartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Parts: []CompletedPart{{Number: 1, ETag: part.ETag}}, MaxSize: -1}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("child did not terminate at publication")
	}
	for _, kind := range []string{"completing-upload", "object", "key-index", "bucket-root", "completed-upload"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCompletionProcessInterruption$")
			cmd.Env = append(os.Environ(), "ESSTHREE_CRASH_TEST_DIR="+dir, "ESSTHREE_CRASH_TEST_KIND="+kind)
			raw, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 77 {
				t.Fatalf("child: %v %s", err, raw)
			}
			reg, err := ocisqlite.New(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reg.Close()
			s, err := newOCIStore(reg, OCIOptions{})
			if err != nil {
				t.Fatal(err)
			}
			o, err := s.GetObject(ctx, scope, "bucket", "key")
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(o.Body)
			o.Body.Close()
			if err != nil || string(data) != "complete bytes" {
				t.Fatalf("recovered object: %q %v", data, err)
			}
			b, err := s.bucket(ctx, scope, "bucket")
			if err != nil {
				t.Fatal(err)
			}
			var state oci.IndexOrManifest
			for tag, err := range reg.Tags(ctx, b.Annotations[annotation+"repo"], nil) {
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(tag, "upload-") {
					state, err = s.read(ctx, b.Annotations[annotation+"repo"], tag)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if state.Annotations[annotation+"kind"] != "completed-upload" {
				t.Fatal("startup left an uncertain upload")
			}
			u, err := multipartInfo("bucket", state)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.PutObject(ctx, scope, PutRequest{Bucket: "bucket", Key: "key", Size: -1}, strings.NewReader("later")); err != nil {
				t.Fatal(err)
			}
			result, err := s.CompleteMultipartUpload(ctx, scope, CompleteMultipartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Parts: []CompletedPart{{Number: 1, ETag: u.Parts[0].ETag}}, MaxSize: -1})
			if err != nil || result.ETag != o.ETag {
				t.Fatalf("replay after crash: %v %v", result, err)
			}
			current, err := s.HeadObject(ctx, scope, "bucket", "key")
			if err != nil || current.ETag == result.ETag {
				t.Fatalf("retry replaced later object: %v %v", current, err)
			}
		})
	}
}
