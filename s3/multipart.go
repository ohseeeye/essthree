package s3

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var errMetadataTooLarge = errors.New("user metadata exceeds limit")

func requestProperties(r *http.Request, bucket, key string) (PutRequest, error) {
	p := PutRequest{Bucket: bucket, Key: key, Size: r.ContentLength, Headers: map[string]string{}, Metadata: map[string]string{}}
	for _, header := range []string{"Content-Type", "Content-Encoding", "Content-Disposition", "Cache-Control", "Content-Language", "Expires"} {
		if value := r.Header.Get(header); value != "" {
			p.Headers[header] = value
		}
	}
	if p.Headers["Content-Type"] == "" {
		p.Headers["Content-Type"] = "application/octet-stream"
	}
	metadataSize := 0
	for header, values := range r.Header {
		if strings.HasPrefix(strings.ToLower(header), "x-amz-meta-") {
			name := strings.TrimPrefix(strings.ToLower(header), "x-amz-meta-")
			value := strings.Join(values, ",")
			metadataSize += len(name) + len(value)
			p.Metadata[name] = value
		}
	}
	if metadataSize > 2048 {
		return PutRequest{}, errMetadataTooLarge
	}
	return p, nil
}

func payloadDigests(r *http.Request) (md5Digest, sha256Digest []byte, err error) {
	if value := r.Header.Get("Content-MD5"); value != "" {
		md5Digest, err = base64.StdEncoding.DecodeString(value)
		if err != nil || len(md5Digest) != 16 {
			return nil, nil, errors.New("invalid Content-MD5")
		}
	}
	if value := r.Header.Get("x-amz-content-sha256"); value != "" && value != "UNSIGNED-PAYLOAD" {
		sha256Digest, err = hex.DecodeString(value)
		if err != nil || len(sha256Digest) != 32 {
			return nil, nil, errors.New("invalid payload SHA-256")
		}
	}
	return md5Digest, sha256Digest, nil
}

func queryIs(q url.Values, names ...string) bool {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
		if _, ok := q[name]; !ok {
			return false
		}
	}
	for name := range q {
		if name != "x-id" && !wanted[name] {
			return false
		}
	}
	return true
}

func (h *Handler) objectSubresource(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key string, q url.Values) bool {
	if _, uploads := q["uploads"]; uploads || q.Has("uploadId") || q.Has("partNumber") {
		for name, values := range q {
			if len(values) != 1 {
				h.fail(w, r, 400, "InvalidArgument", "Duplicate "+name+" parameters are not supported.")
				return true
			}
		}
		if q.Has("uploadId") && q.Get("uploadId") == "" {
			h.fail(w, r, 400, "InvalidArgument", "uploadId must not be empty.")
			return true
		}
	}
	if r.Method == http.MethodPost && queryIs(q, "uploads") {
		h.createMultipartUpload(w, r, scope, bucket, key)
		return true
	}
	if uploadID := q.Get("uploadId"); uploadID != "" {
		switch {
		case r.Method == http.MethodPut && queryIs(q, "uploadId", "partNumber"):
			h.uploadPart(w, r, scope, bucket, key, uploadID, q.Get("partNumber"))
		case r.Method == http.MethodGet:
			h.listParts(w, r, scope, bucket, key, uploadID, q)
		case r.Method == http.MethodPost && queryIs(q, "uploadId"):
			h.completeMultipartUpload(w, r, scope, bucket, key, uploadID)
		case r.Method == http.MethodDelete && queryIs(q, "uploadId"):
			h.abortMultipartUpload(w, r, scope, bucket, key, uploadID)
		default:
			h.fail(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid multipart upload request.")
		}
		return true
	}
	if r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "" && !hasOperationQuery(q) {
		h.copyObject(w, r, scope, bucket, key)
		return true
	}
	return false
}

func (h *Handler) createMultipartUpload(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key string) {
	p, err := requestProperties(r, bucket, key)
	if errors.Is(err, errMetadataTooLarge) {
		h.fail(w, r, http.StatusBadRequest, "MetadataTooLarge", "User metadata exceeds 2 KiB.")
		return
	}
	upload, err := h.store.CreateMultipartUpload(r.Context(), scope, p)
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	writeXML(w, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		XMLNS    string   `xml:"xmlns,attr"`
		Bucket   string
		Key      string
		UploadID string `xml:"UploadId"`
	}{XMLNS: xmlns, Bucket: bucket, Key: key, UploadID: upload.UploadID})
}

func (h *Handler) uploadPart(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key, uploadID, rawPartNumber string) {
	partNumber, err := strconv.Atoi(rawPartNumber)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		h.fail(w, r, http.StatusBadRequest, "InvalidArgument", "Part number must be between 1 and 10000.")
		return
	}
	if r.ContentLength > h.opts.MaxObjectSize {
		h.fail(w, r, http.StatusBadRequest, "EntityTooLarge", "The part exceeds the configured limit.")
		return
	}
	md5Digest, sha256Digest, err := payloadDigests(r)
	if err != nil {
		h.fail(w, r, http.StatusBadRequest, "InvalidDigest", "Invalid payload digest.")
		return
	}
	part, err := h.store.UploadPart(r.Context(), scope, UploadPartRequest{
		Bucket: bucket, Key: key, UploadID: uploadID, Number: partNumber, Size: r.ContentLength,
		MD5: md5Digest, SHA256: sha256Digest,
	}, http.MaxBytesReader(w, r.Body, h.opts.MaxObjectSize))
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("%q", part.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listParts(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key, uploadID string, q url.Values) {
	values, err := multipartQuery(q, "uploadId", "max-parts", "part-number-marker")
	if err != nil {
		h.multipartQueryError(w, r, err)
		return
	}
	maxParts, err := pageLimit(values.Get("max-parts"), "max-parts", true)
	if err != nil {
		h.multipartQueryError(w, r, err)
		return
	}
	marker := 0
	if raw := values.Get("part-number-marker"); raw != "" {
		marker, err = strconv.Atoi(raw)
		if err != nil || marker < 0 || marker > 10000 {
			h.fail(w, r, 400, "InvalidArgument", "Invalid part-number-marker.")
			return
		}
	}
	upload, err := h.store.ListParts(r.Context(), scope, bucket, key, uploadID)
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	type partResult struct {
		PartNumber   int
		LastModified string
		ETag         string
		Size         int64
	}
	result := struct {
		XMLName              xml.Name `xml:"ListPartsResult"`
		XMLNS                string   `xml:"xmlns,attr"`
		Bucket               string
		Key                  string
		UploadID             string `xml:"UploadId"`
		IsTruncated          bool
		PartNumberMarker     int
		NextPartNumberMarker int `xml:"NextPartNumberMarker,omitempty"`
		MaxParts             int
		StorageClass         string
		Initiator            uploadOwner
		Owner                uploadOwner
		Parts                []partResult `xml:"Part"`
	}{XMLNS: xmlns, Bucket: bucket, Key: key, UploadID: uploadID, PartNumberMarker: marker, MaxParts: maxParts, StorageClass: "STANDARD", Initiator: uploadOwner{ID: "local", DisplayName: "local"}, Owner: uploadOwner{ID: "local", DisplayName: "local"}}
	for _, part := range upload.Parts {
		if part.Number <= marker {
			continue
		}
		if len(result.Parts) == maxParts {
			result.IsTruncated = maxParts > 0
			if result.IsTruncated {
				result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
			}
			break
		}
		result.Parts = append(result.Parts, partResult{PartNumber: part.Number, LastModified: part.Modified.Format(time.RFC3339Nano), ETag: fmt.Sprintf("%q", part.ETag), Size: part.Size})
	}
	writeXML(w, result)
}

func (h *Handler) completeMultipartUpload(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key, uploadID string) {
	var body struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int
			ETag       string
		} `xml:"Part"`
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || xml.Unmarshal(raw, &body) != nil || !validXMLDocument(raw) {
		h.fail(w, r, http.StatusBadRequest, "MalformedXML", "Invalid multipart completion document.")
		return
	}
	parts := make([]CompletedPart, 0, len(body.Parts))
	for _, part := range body.Parts {
		parts = append(parts, CompletedPart{Number: part.PartNumber, ETag: part.ETag})
	}
	object, err := h.store.CompleteMultipartUpload(r.Context(), scope, CompleteMultipartRequest{
		Bucket: bucket, Key: key, UploadID: uploadID, Parts: parts, MaxSize: h.opts.MaxObjectSize,
	})
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	writeXML(w, struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		XMLNS    string   `xml:"xmlns,attr"`
		Location string
		Bucket   string
		Key      string
		ETag     string
	}{XMLNS: xmlns, Location: "/" + bucket + "/" + key, Bucket: bucket, Key: key, ETag: fmt.Sprintf("%q", object.ETag)})
}

func (h *Handler) abortMultipartUpload(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key, uploadID string) {
	if err := h.store.AbortMultipartUpload(r.Context(), scope, bucket, key, uploadID); err != nil {
		h.storageError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseCopySource(value string) (bucket, key string, err error) {
	value = strings.TrimPrefix(value, "/")
	if strings.Contains(value, "?") {
		return "", "", errors.New("copy source versions are not supported")
	}
	decoded, err := url.PathUnescape(value)
	if err != nil {
		return "", "", err
	}
	bucket, key, ok := strings.Cut(decoded, "/")
	if !ok || !validBucket(bucket) || key == "" {
		return "", "", errors.New("invalid copy source")
	}
	return bucket, key, nil
}

func (h *Handler) copyObject(w http.ResponseWriter, r *http.Request, scope Scope, bucket, key string) {
	sourceBucket, sourceKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if err != nil {
		h.fail(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid copy source.")
		return
	}
	directive := strings.ToUpper(r.Header.Get("x-amz-metadata-directive"))
	if directive == "" {
		directive = "COPY"
	}
	if directive != "COPY" && directive != "REPLACE" {
		h.fail(w, r, http.StatusBadRequest, "InvalidArgument", "Metadata directive must be COPY or REPLACE.")
		return
	}
	destination, err := requestProperties(r, bucket, key)
	if errors.Is(err, errMetadataTooLarge) {
		h.fail(w, r, http.StatusBadRequest, "MetadataTooLarge", "User metadata exceeds 2 KiB.")
		return
	}
	object, err := h.store.CopyObject(r.Context(), scope, CopyRequest{
		SourceBucket: sourceBucket, SourceKey: sourceKey, Destination: destination, ReplaceMetadata: directive == "REPLACE",
	})
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	writeXML(w, struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		LastModified string
		ETag         string
	}{LastModified: object.Modified.Format(time.RFC3339Nano), ETag: fmt.Sprintf("%q", object.ETag)})
}
