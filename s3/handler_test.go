package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/ohseeeye/essthree/s3"
	"github.com/ohseeeye/oci/ocilayout"
)

func open(t *testing.T, dir string) *httptest.Server {
	t.Helper()
	r, err := ocilayout.New(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	h, err := s3.NewHandler(r, s3.Options{DevelopmentMode: true, MaxObjectSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}
func request(t *testing.T, s *httptest.Server, method, path, body string, headers map[string]string, status int) (http.Header, string) {
	t.Helper()
	r, err := http.NewRequest(method, s.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, err := s.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != status {
		t.Fatalf("%s %s: status %d want %d: %s", method, path, resp.StatusCode, status, raw)
	}
	return resp.Header, string(raw)
}
func TestRoundTripRestartAndDelete(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	_, body := request(t, s, "GET", "/", "", nil, 200)
	if !strings.Contains(body, "test-bucket") {
		t.Fatal(body)
	}
	keys := []string{"hello.txt", "a", "a/b", "../a", "a//b", "plus+space name", "percent%2F", "日本語/", "empty"}
	for _, key := range keys {
		body := "value:" + key
		if key == "empty" {
			body = ""
		}
		path := "/test-bucket/" + url.PathEscape(key)
		request(t, s, "PUT", path, body, map[string]string{"Content-Type": "text/plain", "x-amz-meta-owner": "test"}, 200)
	}
	s.Close()
	s = open(t, dir)
	for _, key := range keys {
		want := "value:" + key
		if key == "empty" {
			want = ""
		}
		path := "/test-bucket/" + url.PathEscape(key)
		h, got := request(t, s, "GET", path, "", nil, 200)
		if got != want || h.Get("x-amz-meta-owner") != "test" {
			t.Fatalf("%q: %q %v", key, got, h)
		}
		_, got = request(t, s, "HEAD", path, "", nil, 200)
		if got != "" {
			t.Fatal("HEAD body")
		}
	}
	request(t, s, "DELETE", "/test-bucket", "", nil, 409)
	for _, key := range keys {
		request(t, s, "DELETE", "/test-bucket/"+url.PathEscape(key), "", nil, 204)
	}
	request(t, s, "DELETE", "/test-bucket/missing", "", nil, 204)
	request(t, s, "HEAD", "/test-bucket/hello.txt", "", nil, 404)
	request(t, s, "DELETE", "/test-bucket", "", nil, 204)
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	request(t, s, "GET", "/test-bucket/hello.txt", "", nil, 404)
}
func TestFailedOverwriteAndUnsupportedRequests(t *testing.T) {
	s := open(t, t.TempDir())
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	request(t, s, "PUT", "/test-bucket/key", "old", nil, 200)
	wrong := md5.Sum([]byte("different"))
	request(t, s, "PUT", "/test-bucket/key", "new", map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(wrong[:])}, 400)
	request(t, s, "PUT", "/test-bucket/key", "new", map[string]string{"x-amz-checksum-crc32": "AAAAAA=="}, 501)
	request(t, s, "PUT", "/test-bucket/key?acl", "new", nil, 501)
	request(t, s, "PUT", "/test-bucket/key", strings.Repeat("x", 1025), nil, 400)
	_, got := request(t, s, "GET", "/test-bucket/key", "", nil, 200)
	if got != "old" {
		t.Fatal("failed upload replaced old object")
	}
	request(t, s, "POST", "/test-bucket/key?acl", "", nil, 501)
	request(t, s, "GET", "/test-bucket/key", "", map[string]string{"Range": "bytes=0-1"}, 501)
	request(t, s, "GET", "/test-bucket?list-type=2", "", nil, 200)
}

func TestMultipartUploadAndCopyObject(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	request(t, s, "PUT", "/source-bucket", "", nil, 200)
	request(t, s, "PUT", "/destination-bucket", "", nil, 200)

	_, body := request(t, s, "POST", "/source-bucket/multipart.txt?uploads", "", map[string]string{
		"Content-Type":     "text/plain",
		"x-amz-meta-owner": "multipart-test",
	}, 200)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(body), &initiated); err != nil || initiated.UploadID == "" {
		t.Fatalf("initiate multipart: %q %v", body, err)
	}
	uploadPath := "/source-bucket/multipart.txt?uploadId=" + url.QueryEscape(initiated.UploadID)
	part2Headers, _ := request(t, s, "PUT", uploadPath+"&partNumber=2", "second", nil, 200)
	request(t, s, "PUT", uploadPath+"&partNumber=1", "old-", nil, 200)
	part1Headers, _ := request(t, s, "PUT", uploadPath+"&partNumber=1", "first-", nil, 200)

	_, listed := request(t, s, "GET", uploadPath, "", nil, 200)
	var parts struct {
		Parts []struct {
			Number int    `xml:"PartNumber"`
			ETag   string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal([]byte(listed), &parts); err != nil || len(parts.Parts) != 2 || parts.Parts[0].Number != 1 || parts.Parts[1].Number != 2 {
		t.Fatalf("list parts: %q %v", listed, err)
	}

	// Multipart state is stored in OCI and survives a handler restart.
	s.Close()
	s = open(t, dir)
	completion := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + part1Headers.Get("ETag") + "</ETag></Part><Part><PartNumber>2</PartNumber><ETag>" + part2Headers.Get("ETag") + "</ETag></Part></CompleteMultipartUpload>"
	_, completed := request(t, s, "POST", uploadPath, completion, nil, 200)
	var completionResult struct{ ETag string }
	if err := xml.Unmarshal([]byte(completed), &completionResult); err != nil || !strings.HasSuffix(strings.Trim(completionResult.ETag, "\""), "-2") {
		t.Fatalf("multipart ETag missing part count: %s", completed)
	}
	headers, got := request(t, s, "GET", "/source-bucket/multipart.txt", "", nil, 200)
	if got != "first-second" || headers.Get("x-amz-meta-owner") != "multipart-test" || headers.Get("Content-Type") != "text/plain" {
		t.Fatalf("multipart object = %q, headers %v", got, headers)
	}

	copySource := url.PathEscape("source-bucket/multipart.txt")
	request(t, s, "PUT", "/destination-bucket/copied.txt", "", map[string]string{"x-amz-copy-source": copySource}, 200)
	headers, got = request(t, s, "GET", "/destination-bucket/copied.txt", "", nil, 200)
	if got != "first-second" || headers.Get("x-amz-meta-owner") != "multipart-test" {
		t.Fatalf("copied object = %q, headers %v", got, headers)
	}
	request(t, s, "PUT", "/destination-bucket/replaced.txt", "", map[string]string{
		"x-amz-copy-source":        copySource,
		"x-amz-metadata-directive": "REPLACE",
		"Content-Type":             "application/test",
		"x-amz-meta-owner":         "copy-test",
	}, 200)
	headers, got = request(t, s, "GET", "/destination-bucket/replaced.txt", "", nil, 200)
	if got != "first-second" || headers.Get("x-amz-meta-owner") != "copy-test" || headers.Get("Content-Type") != "application/test" {
		t.Fatalf("metadata replacement = %q, headers %v", got, headers)
	}

	_, body = request(t, s, "POST", "/source-bucket/aborted?uploads", "", nil, 200)
	if err := xml.Unmarshal([]byte(body), &initiated); err != nil {
		t.Fatal(err)
	}
	abortPath := "/source-bucket/aborted?uploadId=" + url.QueryEscape(initiated.UploadID)
	request(t, s, "DELETE", abortPath, "", nil, 204)
	request(t, s, "PUT", abortPath+"&partNumber=1", "late", nil, 404)
}

type listResponse struct {
	Contents              []struct{ Key string }    `xml:"Contents"`
	Prefixes              []struct{ Prefix string } `xml:"CommonPrefixes"`
	IsTruncated           bool                      `xml:"IsTruncated"`
	NextMarker            string                    `xml:"NextMarker"`
	NextContinuationToken string                    `xml:"NextContinuationToken"`
	KeyCount              int                       `xml:"KeyCount"`
}

func parseList(t *testing.T, body string) listResponse {
	t.Helper()
	var response listResponse
	if err := xml.Unmarshal([]byte(body), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func listKeys(response listResponse) []string {
	keys := make([]string, 0, len(response.Contents))
	for _, object := range response.Contents {
		keys = append(keys, object.Key)
	}
	return keys
}

func listPrefixes(response listResponse) []string {
	prefixes := make([]string, 0, len(response.Prefixes))
	for _, prefix := range response.Prefixes {
		prefixes = append(prefixes, prefix.Prefix)
	}
	return prefixes
}

func TestListObjectsV1AndV2(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	for _, key := range []string{"a.txt", "dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "space key", "z.txt"} {
		request(t, s, "PUT", "/test-bucket/"+url.PathEscape(key), key, nil, 200)
	}

	_, body := request(t, s, "GET", "/test-bucket", "", nil, 200)
	v1 := parseList(t, body)
	if got, want := strings.Join(listKeys(v1), ","), "a.txt,dir/a.txt,dir/b.txt,dir/sub/c.txt,space key,z.txt"; got != want {
		t.Fatalf("V1 keys = %q, want %q", got, want)
	}
	_, body = request(t, s, "GET", "/test-bucket?max-keys=1", "", nil, 200)
	v1 = parseList(t, body)
	if !v1.IsTruncated || v1.NextMarker != "a.txt" {
		t.Fatalf("first V1 page = %+v", v1)
	}
	_, body = request(t, s, "GET", "/test-bucket?marker="+url.QueryEscape(v1.NextMarker), "", nil, 200)
	v1 = parseList(t, body)
	if got, want := strings.Join(listKeys(v1), ","), "dir/a.txt,dir/b.txt,dir/sub/c.txt,space key,z.txt"; got != want {
		t.Fatalf("second V1 page = %q, want %q", got, want)
	}

	_, body = request(t, s, "GET", "/test-bucket?prefix=dir%2F&delimiter=%2F", "", nil, 200)
	v1 = parseList(t, body)
	if got, want := strings.Join(listKeys(v1), ","), "dir/a.txt,dir/b.txt"; got != want {
		t.Fatalf("V1 contents = %q, want %q", got, want)
	}
	if got, want := strings.Join(listPrefixes(v1), ","), "dir/sub/"; got != want {
		t.Fatalf("V1 prefixes = %q, want %q", got, want)
	}

	_, body = request(t, s, "GET", "/test-bucket?list-type=2&max-keys=2", "", nil, 200)
	v2 := parseList(t, body)
	if !v2.IsTruncated || v2.NextContinuationToken == "" || v2.KeyCount != 2 {
		t.Fatalf("first V2 page = %+v", v2)
	}
	if got, want := strings.Join(listKeys(v2), ","), "a.txt,dir/a.txt"; got != want {
		t.Fatalf("first V2 keys = %q, want %q", got, want)
	}
	_, body = request(t, s, "GET", "/test-bucket?list-type=2&continuation-token="+url.QueryEscape(v2.NextContinuationToken), "", nil, 200)
	v2 = parseList(t, body)
	if got, want := strings.Join(listKeys(v2), ","), "dir/b.txt,dir/sub/c.txt,space key,z.txt"; got != want || v2.IsTruncated {
		t.Fatalf("second V2 page = %+v", v2)
	}

	_, body = request(t, s, "GET", "/test-bucket?list-type=2&encoding-type=url&prefix=space", "", nil, 200)
	v2 = parseList(t, body)
	if got, want := strings.Join(listKeys(v2), ","), "space%20key"; got != want {
		t.Fatalf("encoded V2 keys = %q, want %q", got, want)
	}

	s.Close()
	s = open(t, dir)
	_, body = request(t, s, "GET", "/test-bucket?list-type=2&start-after=dir%2Fa.txt", "", nil, 200)
	v2 = parseList(t, body)
	if got, want := strings.Join(listKeys(v2), ","), "dir/b.txt,dir/sub/c.txt,space key,z.txt"; got != want {
		t.Fatalf("rebuilt V2 cache = %q, want %q", got, want)
	}

	request(t, s, "DELETE", "/test-bucket/dir/b.txt", "", nil, 204)
	_, body = request(t, s, "GET", "/test-bucket?list-type=2&prefix=dir%2F", "", nil, 200)
	v2 = parseList(t, body)
	if got, want := strings.Join(listKeys(v2), ","), "dir/a.txt,dir/sub/c.txt"; got != want {
		t.Fatalf("invalidated V2 cache = %q, want %q", got, want)
	}
	request(t, s, "PUT", "/test-bucket/dir/new.txt", "new", nil, 200)
	_, body = request(t, s, "GET", "/test-bucket?list-type=2&prefix=dir%2F", "", nil, 200)
	v2 = parseList(t, body)
	if got, want := strings.Join(listKeys(v2), ","), "dir/a.txt,dir/new.txt,dir/sub/c.txt"; got != want {
		t.Fatalf("PUT cache invalidation = %q, want %q", got, want)
	}
}
func TestConcurrentOverwrites(t *testing.T) {
	s := open(t, t.TempDir())
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := bytes.Repeat([]byte{byte('a' + i)}, 512)
			req, _ := http.NewRequest("PUT", s.URL+"/test-bucket/key", bytes.NewReader(data))
			resp, err := s.Client().Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("PUT: %d", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()
	_, got := request(t, s, "GET", "/test-bucket/key", "", nil, 200)
	if len(got) != 512 || strings.Trim(got, string(got[0])) != "" {
		t.Fatal("torn object")
	}
}
