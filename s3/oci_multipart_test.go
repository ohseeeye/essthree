package s3

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ohseeeye/oci"
	"github.com/ohseeeye/oci/ocilayout"
)

func multipartFixture(t *testing.T) (*OCIStore, MultipartUpload) {
	t.Helper()
	r, err := ocilayout.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newOCIStore(r, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	if err := s.CreateBucket(ctx, scope, "bucket"); err != nil {
		t.Fatal(err)
	}
	u, err := s.CreateMultipartUpload(ctx, scope, PutRequest{Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	return s, u
}

func TestConcurrentMultipartPartsAndFailedReplacement(t *testing.T) {
	s, u := multipartFixture(t)
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for n := 1; n <= 16; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := s.UploadPart(ctx, scope, UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: n, Size: -1}, strings.NewReader(fmt.Sprint(n)))
			errs <- err
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	parts, err := s.ListParts(ctx, scope, "bucket", "key", u.UploadID)
	if err != nil || len(parts.Parts) != 16 {
		t.Fatalf("lost concurrent part: %v %v", parts, err)
	}
	old := parts.Parts[0]
	p := UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}
	if _, err := s.UploadPart(ctx, scope, p, brokenReader{}); err == nil {
		t.Fatal("expected reader failure")
	}
	digest := md5.Sum([]byte("other"))
	p.MD5 = digest[:]
	if _, err := s.UploadPart(ctx, scope, p, strings.NewReader("replacement")); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	p.MD5, p.Size = nil, 100
	if _, err := s.UploadPart(ctx, scope, p, strings.NewReader("replacement")); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	p.Size = -1
	reg := s.registry
	s.registry = &failingRegistry{Registry: reg, kind: "multipart-upload", fail: true}
	if _, err := s.UploadPart(ctx, scope, p, strings.NewReader("replacement")); err == nil {
		t.Fatal("expected part-record publication failure")
	}
	s.registry = reg
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.UploadPart(canceled, scope, p, strings.NewReader("replacement")); err == nil {
		t.Fatal("expected cancellation")
	}
	parts, err = s.ListParts(ctx, scope, "bucket", "key", u.UploadID)
	if err != nil || parts.Parts[0] != old {
		t.Fatalf("failed replacement changed part: %v %v", parts, err)
	}
}

type gatedReader struct {
	io.Reader
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *gatedReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started); <-r.release })
	return r.Reader.Read(p)
}

func TestPartRacesWithTermination(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			s, u := multipartFixture(t)
			ctx, scope := context.Background(), Scope{Tenant: "local"}
			p := UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}
			old, err := s.UploadPart(ctx, scope, p, strings.NewReader("original"))
			if err != nil {
				t.Fatal(err)
			}
			reader := &gatedReader{Reader: strings.NewReader("late replacement"), started: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() { _, err := s.UploadPart(ctx, scope, p, reader); done <- err }()
			<-reader.started
			if complete {
				_, err = s.CompleteMultipartUpload(ctx, scope, CompleteMultipartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Parts: []CompletedPart{{Number: 1, ETag: old.ETag}}, MaxSize: -1})
			} else {
				err = s.AbortMultipartUpload(ctx, scope, "bucket", "key", u.UploadID)
			}
			close(reader.release)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, ErrNoUpload) {
				t.Fatalf("late part: %v", err)
			}
			if complete {
				o, err := s.GetObject(ctx, scope, "bucket", "key")
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(o.Body)
				o.Body.Close()
				if err != nil || string(data) != "original" {
					t.Fatalf("completed bytes: %q %v", data, err)
				}
			}
		})
	}
}

func TestListPartsDefaultPageBoundary(t *testing.T) {
	s, u := multipartFixture(t)
	ctx, scope := context.Background(), Scope{Tenant: "local"}
	if _, err := s.UploadPart(ctx, scope, UploadPartRequest{Bucket: "bucket", Key: "key", UploadID: u.UploadID, Number: 1, Size: -1}, strings.NewReader("same")); err != nil {
		t.Fatal(err)
	}
	repo, state, err := s.multipart(ctx, scope, "bucket", "key", u.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	// Seed real persisted descriptors without doing 1,001 redundant transfers.
	layers := make([]oci.Descriptor, 1001)
	for i := range layers {
		layers[i] = state.Layers[0]
		layers[i].Annotations = cloneStrings(layers[i].Annotations)
		layers[i].Annotations[annotation+"part-number"] = strconv.Itoa(i + 1)
	}
	if err := s.publish(ctx, repo, "upload-"+hash(u.UploadID), "multipart-upload", cloneStrings(state.Annotations), layers); err != nil {
		t.Fatal(err)
	}
	h := &Handler{store: s, opts: Options{DevelopmentMode: true}}
	for _, suffix := range []string{"", "&max-parts=10000"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/bucket/key?uploadId="+u.UploadID+suffix, nil))
		var result struct {
			IsTruncated                    bool
			MaxParts, NextPartNumberMarker int
			Parts                          []struct{ PartNumber int } `xml:"Part"`
		}
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || !result.IsTruncated || result.MaxParts != 1000 || result.NextPartNumberMarker != 1000 || len(result.Parts) != 1000 {
			t.Fatalf("default boundary: %+v", result)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/bucket/key?uploadId="+u.UploadID+"&part-number-marker=1000", nil))
	if !strings.Contains(w.Body.String(), "<PartNumber>1001</PartNumber>") || !strings.Contains(w.Body.String(), "<IsTruncated>false</IsTruncated>") {
		t.Fatal(w.Body.String())
	}
}
