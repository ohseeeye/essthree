package s3

import (
	"context"
	"errors"
	"github.com/ohseeeye/oci/ocisqlite"
	"io"
	"strings"
	"testing"
)

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func TestScopesAndInterruptedWrite(t *testing.T) {
	r, err := ocisqlite.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	s, err := newOCIStore(r, OCIOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a, b := Scope{Tenant: "a"}, Scope{Tenant: "b"}
	if err = s.CreateBucket(ctx, a, "bucket"); err != nil {
		t.Fatal(err)
	}
	if err = s.HeadBucket(ctx, b, "bucket"); !errors.Is(err, ErrNoBucket) {
		t.Fatal(err)
	}
	p := PutRequest{Bucket: "bucket", Key: "key", Size: -1}
	if _, err = s.PutObject(ctx, a, p, strings.NewReader("old")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutObject(ctx, a, p, brokenReader{}); err == nil {
		t.Fatal("expected read error")
	}
	o, err := s.GetObject(ctx, a, "bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer o.Body.Close()
	data, err := io.ReadAll(o.Body)
	if err != nil || string(data) != "old" {
		t.Fatalf("%s: %v", data, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.PutObject(canceled, a, p, strings.NewReader("new")); err == nil {
		t.Fatal("expected cancellation")
	}
}
