package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func validXMLDocument(raw []byte) bool {
	d := xml.NewDecoder(bytes.NewReader(raw))
	depth, roots := 0, 0
	for {
		token, err := d.Token()
		if err == io.EOF {
			return depth == 0 && roots == 1
		}
		if err != nil {
			return false
		}
		switch token := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
			}
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(token)) != "" {
				return false
			}
		}
	}
}

func multipartQuery(q url.Values, allowed ...string) (url.Values, error) {
	wanted := map[string]bool{"x-id": true}
	for _, name := range allowed {
		wanted[name] = true
	}
	for name := range q {
		if !wanted[name] {
			return nil, &listError{400, "InvalidArgument", "Invalid multipart query parameter: " + name}
		}
		if _, err := queryValue(q, name); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func pageLimit(raw, name string, allowZero bool) (int, error) {
	if raw == "" {
		return 1000, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || (!allowZero && n == 0) {
		return 0, &listError{400, "InvalidArgument", "Invalid " + name + "."}
	}
	return min(n, 1000), nil
}

func (h *Handler) multipartQueryError(w http.ResponseWriter, r *http.Request, err error) {
	e := err.(*listError)
	h.fail(w, r, e.status, e.code, e.message)
}

type uploadResult struct {
	Key          string
	UploadID     string `xml:"UploadId"`
	Initiated    string
	StorageClass string
	Initiator    uploadOwner
	Owner        uploadOwner
}

type uploadOwner struct{ ID, DisplayName string }

func (h *Handler) listMultipartUploads(w http.ResponseWriter, r *http.Request, scope Scope, bucket string, q url.Values) {
	values, err := multipartQuery(q, "uploads", "prefix", "delimiter", "key-marker", "upload-id-marker", "max-uploads", "encoding-type")
	if err != nil {
		h.multipartQueryError(w, r, err)
		return
	}
	limit, err := pageLimit(values.Get("max-uploads"), "max-uploads", false)
	if err != nil {
		h.multipartQueryError(w, r, err)
		return
	}
	encoding := values.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		h.fail(w, r, 400, "InvalidArgument", "encoding-type must be url.")
		return
	}
	uploads, err := h.store.ListMultipartUploads(r.Context(), scope, MultipartListRequest{Bucket: bucket, KeyMarker: values.Get("key-marker"), UploadIDMarker: values.Get("upload-id-marker")})
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	prefix, delimiter := values.Get("prefix"), values.Get("delimiter")
	keyMarker, idMarker := values.Get("key-marker"), values.Get("upload-id-marker")
	type entry struct {
		key, id string
		upload  *MultipartUpload
	}
	var entries []entry
	seen := map[string]bool{}
	for i := range uploads {
		u := &uploads[i]
		if !strings.HasPrefix(u.Key, prefix) {
			continue
		}
		if delimiter != "" {
			if index := strings.Index(strings.TrimPrefix(u.Key, prefix), delimiter); index >= 0 {
				name := prefix + strings.TrimPrefix(u.Key, prefix)[:index+len(delimiter)]
				if name > keyMarker && !seen[name] {
					seen[name] = true
					entries = append(entries, entry{key: name})
				}
				continue
			}
		}
		entries = append(entries, entry{key: u.Key, id: u.UploadID, upload: u})
	}
	result := struct {
		XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
		XMLNS              string   `xml:"xmlns,attr"`
		Bucket             string
		KeyMarker          string
		UploadIDMarker     string `xml:"UploadIdMarker"`
		NextKeyMarker      string `xml:"NextKeyMarker,omitempty"`
		NextUploadIDMarker string `xml:"NextUploadIdMarker,omitempty"`
		Prefix             string
		Delimiter          string `xml:"Delimiter,omitempty"`
		EncodingType       string `xml:"EncodingType,omitempty"`
		MaxUploads         int
		IsTruncated        bool
		Uploads            []uploadResult `xml:"Upload"`
		Prefixes           []commonPrefix `xml:"CommonPrefixes"`
	}{XMLNS: xmlns, Bucket: bucket, KeyMarker: encodeListKey(keyMarker, encoding), UploadIDMarker: idMarker, Prefix: encodeListKey(prefix, encoding), Delimiter: encodeListKey(delimiter, encoding), EncodingType: encoding, MaxUploads: limit}
	if len(entries) > limit {
		result.IsTruncated = true
		entries = entries[:limit]
		last := entries[len(entries)-1]
		result.NextKeyMarker, result.NextUploadIDMarker = encodeListKey(last.key, encoding), last.id
	}
	for _, e := range entries {
		if e.upload == nil {
			result.Prefixes = append(result.Prefixes, commonPrefix{Prefix: encodeListKey(e.key, encoding)})
			continue
		}
		owner := uploadOwner{ID: "local", DisplayName: "local"}
		result.Uploads = append(result.Uploads, uploadResult{Key: encodeListKey(e.key, encoding), UploadID: e.id, Initiated: e.upload.Created.Format(time.RFC3339Nano), StorageClass: "STANDARD", Initiator: owner, Owner: owner})
	}
	writeXML(w, result)
}
