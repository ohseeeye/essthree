package s3_test

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestMultipartDiscoveryAndPagination(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	create := func(key string) string {
		_, body := request(t, s, "POST", "/test-bucket/"+url.PathEscape(key)+"?uploads", "", nil, 200)
		var result struct {
			ID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal([]byte(body), &result); err != nil || result.ID == "" {
			t.Fatalf("create: %q %v", body, err)
		}
		return result.ID
	}
	ids := []string{create("same"), create("same"), create("dir/a"), create("dir/b"), create("space +%")}
	aborted := create("aborted")
	request(t, s, "DELETE", "/test-bucket/aborted?uploadId="+aborted, "", nil, 204)
	s.Close()
	s = open(t, dir)
	type result struct {
		IsTruncated   bool
		NextKeyMarker string
		NextID        string `xml:"NextUploadIdMarker"`
		Uploads       []struct {
			Key string
			ID  string `xml:"UploadId"`
		} `xml:"Upload"`
		Prefixes []struct{ Prefix string } `xml:"CommonPrefixes"`
	}
	var seen []string
	marker, idMarker := "", ""
	for page := 0; page < 8; page++ {
		_, raw := request(t, s, "GET", "/test-bucket?uploads&max-uploads=1&key-marker="+url.QueryEscape(marker)+"&upload-id-marker="+url.QueryEscape(idMarker), "", nil, 200)
		var out result
		if err := xml.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Uploads) != 1 {
			t.Fatalf("page: %s", raw)
		}
		seen = append(seen, out.Uploads[0].ID)
		if !out.IsTruncated {
			break
		}
		if marker == out.NextKeyMarker && idMarker == out.NextID {
			t.Fatal("marker did not advance")
		}
		marker, idMarker = out.NextKeyMarker, out.NextID
	}
	want := []string{ids[2], ids[3], ids[0], ids[1], ids[4]}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("uploads %v want %v", seen, want)
	}
	// The previous page's upload may be terminated between listing requests.
	request(t, s, "DELETE", "/test-bucket/same?uploadId="+ids[0], "", nil, 204)
	_, afterAbort := request(t, s, "GET", "/test-bucket?uploads&key-marker=same&upload-id-marker="+ids[0], "", nil, 200)
	var remaining result
	if err := xml.Unmarshal([]byte(afterAbort), &remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining.Uploads) != 2 || remaining.Uploads[0].ID != ids[1] {
		t.Fatalf("terminated marker skipped next upload: %s", afterAbort)
	}
	_, raw := request(t, s, "GET", "/test-bucket?uploads&delimiter=%2F&max-uploads=1", "", nil, 200)
	var grouped result
	if err := xml.Unmarshal([]byte(raw), &grouped); err != nil {
		t.Fatal(err)
	}
	if len(grouped.Prefixes) != 1 || grouped.Prefixes[0].Prefix != "dir/" || !grouped.IsTruncated || grouped.NextKeyMarker != "dir/" {
		t.Fatalf("grouped: %s", raw)
	}
	_, raw = request(t, s, "GET", "/test-bucket?uploads&delimiter=%2F&key-marker=dir%2F", "", nil, 200)
	if !strings.Contains(raw, "<Key>same</Key>") || strings.Contains(raw, "<CommonPrefixes>") {
		t.Fatal(raw)
	}
	_, raw = request(t, s, "GET", "/test-bucket?uploads&prefix=space&encoding-type=url", "", nil, 200)
	if !strings.Contains(raw, "space%20%2B%25") {
		t.Fatal(raw)
	}
	request(t, s, "DELETE", "/test-bucket", "", nil, 409)
	for _, query := range []string{"uploads&max-uploads=0", "uploads&max-uploads=-1", "uploads&prefix=a&prefix=b", "uploads&encoding-type=bad", "uploads&unknown=1"} {
		request(t, s, "GET", "/test-bucket?"+query, "", nil, 400)
	}
	request(t, s, "GET", "/missing-bucket?uploads", "", nil, 404)
}

func TestListPartsPaginationAndCompletionValidation(t *testing.T) {
	s := open(t, t.TempDir())
	request(t, s, "PUT", "/test-bucket", "", nil, 200)
	_, raw := request(t, s, "POST", "/test-bucket/key?uploads", "", nil, 200)
	var initiated struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(raw), &initiated); err != nil {
		t.Fatal(err)
	}
	path := "/test-bucket/key?uploadId=" + initiated.ID
	etags := map[int]string{}
	for _, n := range []int{5, 1, 3} {
		h, _ := request(t, s, "PUT", path+fmt.Sprintf("&partNumber=%d", n), "identical", nil, 200)
		etags[n] = h.Get("ETag")
	}
	var page struct {
		IsTruncated                                      bool
		MaxParts, PartNumberMarker, NextPartNumberMarker int
		Parts                                            []struct {
			Number int `xml:"PartNumber"`
		} `xml:"Part"`
	}
	_, raw = request(t, s, "GET", path+"&max-parts=2", "", nil, 200)
	if err := xml.Unmarshal([]byte(raw), &page); err != nil {
		t.Fatal(err)
	}
	if !page.IsTruncated || page.MaxParts != 2 || page.NextPartNumberMarker != 3 || len(page.Parts) != 2 || page.Parts[0].Number != 1 {
		t.Fatal(raw)
	}
	page.Parts = nil
	_, raw = request(t, s, "GET", path+"&max-parts=2&part-number-marker=3", "", nil, 200)
	if err := xml.Unmarshal([]byte(raw), &page); err != nil {
		t.Fatal(err)
	}
	if page.IsTruncated || page.PartNumberMarker != 3 || len(page.Parts) != 1 || page.Parts[0].Number != 5 {
		t.Fatal(raw)
	}
	request(t, s, "GET", path+"&max-parts=0", "", nil, 200)
	for _, suffix := range []string{"&max-parts=-1", "&part-number-marker=bad", "&part-number-marker=10001", "&max-parts=1&max-parts=2", "&unknown=1"} {
		request(t, s, "GET", path+suffix, "", nil, 400)
	}
	partXML := func(n int, etag string) string {
		return fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n, etag)
	}
	completion := "<CompleteMultipartUpload>" + partXML(1, etags[1]) + partXML(3, etags[3]) + partXML(5, etags[5]) + "</CompleteMultipartUpload>"
	for _, body := range []string{
		strings.ReplaceAll(completion, "CompleteMultipartUpload", "WrongRoot"),
		completion + "<extra/>", completion + "trailing", completion + strings.Repeat(" ", 1<<20),
		"<CompleteMultipartUpload>",
		"<CompleteMultipartUpload>" + partXML(1, "bad") + "</CompleteMultipartUpload>",
		"<CompleteMultipartUpload>" + partXML(3, etags[3]) + partXML(1, etags[1]) + "</CompleteMultipartUpload>",
		"<CompleteMultipartUpload>" + partXML(1, etags[1]) + partXML(1, etags[1]) + "</CompleteMultipartUpload>",
	} {
		request(t, s, "POST", path, body, nil, 400)
	}
	request(t, s, "PUT", path+"&partNumber=1&partNumber=2", "bad", nil, 400)
	request(t, s, "POST", path+"&uploadId=duplicate", completion, nil, 400)
	request(t, s, "POST", path, completion, nil, 200)
	_, data := request(t, s, "GET", "/test-bucket/key", "", nil, 200)
	if data != strings.Repeat("identical", 3) {
		t.Fatalf("duplicate descriptors lost bytes: %q", data)
	}
	request(t, s, "GET", path, "", nil, 404)
	request(t, s, "PUT", "/test-bucket/key", "later", nil, 200)
	request(t, s, "POST", path, completion, nil, 200)
	_, data = request(t, s, "GET", "/test-bucket/key", "", nil, 200)
	if data != "later" {
		t.Fatal("retry overwrote later object")
	}
	_, raw = request(t, s, "GET", "/test-bucket?uploads", "", nil, 200)
	if strings.Contains(raw, "<Upload>") {
		t.Fatal("completed upload remains active")
	}
}
