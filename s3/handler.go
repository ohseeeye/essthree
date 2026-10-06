// Package s3 exposes an embeddable path-style S3 HTTP handler.
package s3

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/ohseeeye/oci"
)

const xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

type Options struct {
	// DevelopmentMode explicitly accepts requests without verifying credentials.
	DevelopmentMode bool
	Logger          *slog.Logger
	MaxObjectSize   int64 // defaults to 5 GiB for this single-PUT prototype
}
type Handler struct {
	store Backend
	opts  Options
}

func NewHandler(registry oci.Registry, opts Options) (*Handler, error) {
	if registry == nil {
		return nil, errors.New("nil OCI registry")
	}
	if !opts.DevelopmentMode {
		return nil, errors.New("SigV4 verification is not implemented; explicitly enable development mode")
	}
	if opts.MaxObjectSize == 0 {
		opts.MaxObjectSize = 5 << 30
	}
	if opts.MaxObjectSize < 0 {
		return nil, errors.New("negative object limit")
	}
	store, err := newOCIStore(registry, OCIOptions{})
	if err != nil {
		return nil, err
	}
	return &Handler{store: store, opts: opts}, nil
}

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func validBucket(s string) bool {
	return bucketPattern.MatchString(s) && !strings.Contains(s, "..") && !strings.Contains(s, ".-") && !strings.Contains(s, "-.") && net.ParseIP(s) == nil
}
func (h *Handler) fail(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName                            xml.Name `xml:"Error"`
		Code, Message, Resource, RequestID string
	}{Code: code, Message: message, Resource: r.URL.Path, RequestID: w.Header().Get("x-amz-request-id")})
}
func (h *Handler) storageError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, msg := 500, "InternalError", "An internal error occurred."
	switch {
	case errors.Is(err, ErrInvalidVersionMarker):
		status, code, msg = 400, "InvalidArgument", "Invalid version listing marker."
	case errors.Is(err, ErrNoBucket):
		status, code, msg = 404, "NoSuchBucket", "The specified bucket does not exist."
	case errors.Is(err, ErrNoKey):
		status, code, msg = 404, "NoSuchKey", "The specified key does not exist."
	case errors.Is(err, ErrNoUpload):
		status, code, msg = 404, "NoSuchUpload", "The specified multipart upload does not exist."
	case errors.Is(err, ErrBucketExists):
		status, code, msg = 409, "BucketAlreadyOwnedByYou", "The bucket already exists."
	case errors.Is(err, ErrNotEmpty):
		status, code, msg = 409, "BucketNotEmpty", "The bucket is not empty."
	case errors.Is(err, ErrIntegrity):
		status, code, msg = 400, "BadDigest", "The payload does not match its expected length or digest."
	case errors.Is(err, ErrInvalidPart):
		status, code, msg = 400, "InvalidPart", "One or more of the specified parts could not be found or did not match the supplied ETag."
	case errors.Is(err, ErrPartOrder):
		status, code, msg = 400, "InvalidPartOrder", "The list of parts was not in ascending order."
	case errors.Is(err, ErrTooLarge):
		status, code, msg = 400, "EntityTooLarge", "The object exceeds the configured limit."
	default:
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			status, code, msg = 400, "EntityTooLarge", "The object exceeds the configured limit."
		} else if h.opts.Logger != nil {
			h.opts.Logger.ErrorContext(r.Context(), "storage operation failed", "error", err)
		}
	}
	h.fail(w, r, status, code, msg)
}
func writeXML(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(v)
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", rand.Text())
	scope := Scope{Tenant: "local"}
	// URL.Path is already decoded once by net/http. Never clean or redirect it.
	if !strings.HasPrefix(r.URL.Path, "/") {
		h.fail(w, r, 400, "InvalidURI", "Invalid path.")
		return
	}
	bucket, key, hasKey := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q, err := parseQuery(r)
	if err != nil {
		h.fail(w, r, 400, "InvalidArgument", "Malformed query.")
		return
	}
	for k := range r.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-") && !allowedAmz(lower) || strings.HasPrefix(lower, "if-") || lower == "range" {
			h.fail(w, r, 501, "NotImplemented", "This request feature is not implemented.")
			return
		}
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") || len(r.Trailer) > 0 {
		h.fail(w, r, 501, "NotImplemented", "S3 streaming payload encoding is not implemented.")
		return
	}
	if bucket == "" {
		if r.URL.Path != "/" || r.Method != http.MethodGet || hasOperationQuery(q) {
			h.fail(w, r, 405, "MethodNotAllowed", "Unsupported service operation.")
			return
		}
		buckets, err := h.store.ListBuckets(r.Context(), scope)
		if err != nil {
			h.storageError(w, r, err)
			return
		}
		type entry struct {
			Name         string
			CreationDate string
		}
		result := struct {
			XMLName xml.Name `xml:"ListAllMyBucketsResult"`
			XMLNS   string   `xml:"xmlns,attr"`
			Owner   struct{ ID, DisplayName string }
			Buckets []entry `xml:"Buckets>Bucket"`
		}{XMLNS: xmlns}
		result.Owner.ID = "local"
		result.Owner.DisplayName = "local"
		for _, b := range buckets {
			result.Buckets = append(result.Buckets, entry{b.Name, b.Created.UTC().Format("2006-01-02T15:04:05.000Z")})
		}
		writeXML(w, result)
		return
	}
	if !validBucket(bucket) {
		h.fail(w, r, 400, "InvalidBucketName", "Invalid bucket name.")
		return
	}
	if !hasKey || key == "" {
		switch r.Method {
		case http.MethodPut:
			if hasOperationQuery(q) {
				h.fail(w, r, 501, "NotImplemented", "This query operation is not implemented.")
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
			if err != nil {
				h.fail(w, r, 400, "MalformedXML", "Cannot read request body.")
				return
			}
			if len(body) > 4096 {
				h.fail(w, r, 400, "MalformedXML", "Bucket configuration is too large.")
				return
			}
			if len(strings.TrimSpace(string(body))) > 0 {
				var config struct {
					XMLName  xml.Name `xml:"CreateBucketConfiguration"`
					Location string   `xml:"LocationConstraint"`
				}
				if xml.Unmarshal(body, &config) != nil {
					h.fail(w, r, 400, "MalformedXML", "Invalid bucket configuration.")
					return
				}
				if config.Location != "" && config.Location != "us-east-1" {
					h.fail(w, r, 400, "InvalidLocationConstraint", "Only us-east-1 is supported.")
					return
				}
			}
			if err := h.store.CreateBucket(r.Context(), scope, bucket); err != nil {
				h.storageError(w, r, err)
				return
			}
			w.Header().Set("Location", "/"+bucket)
			w.WriteHeader(200)
		case http.MethodHead:
			if hasOperationQuery(q) {
				h.fail(w, r, 501, "NotImplemented", "This query operation is not implemented.")
				return
			}
			if err := h.store.HeadBucket(r.Context(), scope, bucket); err != nil {
				h.storageError(w, r, err)
				return
			}
			w.WriteHeader(200)
		case http.MethodDelete:
			if hasOperationQuery(q) {
				h.fail(w, r, 501, "NotImplemented", "This query operation is not implemented.")
				return
			}
			if err := h.store.DeleteBucket(r.Context(), scope, bucket); err != nil {
				h.storageError(w, r, err)
				return
			}
			w.WriteHeader(204)
		case http.MethodGet:
			if _, ok := q["versions"]; ok {
				h.listObjectVersions(w, r, scope, bucket, q)
				return
			}
			if _, ok := q["uploads"]; ok {
				h.listMultipartUploads(w, r, scope, bucket, q)
				return
			}
			h.listObjects(w, r, scope, bucket, q)
		default:
			h.fail(w, r, 501, "NotImplemented", "This bucket operation is not implemented.")
		}
		return
	}
	if !utf8.ValidString(key) || len(key) > 1024 {
		h.fail(w, r, 400, "KeyTooLongError", "Keys must be valid UTF-8 and at most 1024 bytes.")
		return
	}
	if h.objectSubresource(w, r, scope, bucket, key, q) {
		return
	}
	if hasOperationQuery(q) {
		h.fail(w, r, 501, "NotImplemented", "This query operation is not implemented.")
		return
	}
	switch r.Method {
	case http.MethodPut:
		p := PutRequest{Bucket: bucket, Key: key, Size: r.ContentLength, Headers: map[string]string{}, Metadata: map[string]string{}}
		if p.Size > h.opts.MaxObjectSize {
			h.fail(w, r, 400, "EntityTooLarge", "The object exceeds the configured limit.")
			return
		}
		for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Disposition", "Cache-Control", "Content-Language", "Expires"} {
			if v := r.Header.Get(k); v != "" {
				p.Headers[k] = v
			}
		}
		if p.Headers["Content-Type"] == "" {
			p.Headers["Content-Type"] = "application/octet-stream"
		}
		metadataSize := 0
		for k, values := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
				name := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
				v := strings.Join(values, ",")
				metadataSize += len(name) + len(v)
				p.Metadata[name] = v
			}
		}
		if metadataSize > 2048 {
			h.fail(w, r, 400, "MetadataTooLarge", "User metadata exceeds 2 KiB.")
			return
		}
		if v := r.Header.Get("Content-MD5"); v != "" {
			p.MD5, err = base64.StdEncoding.DecodeString(v)
			if err != nil || len(p.MD5) != 16 {
				h.fail(w, r, 400, "InvalidDigest", "Invalid Content-MD5.")
				return
			}
		}
		if v := r.Header.Get("x-amz-content-sha256"); v != "" && v != "UNSIGNED-PAYLOAD" {
			p.SHA256, err = hex.DecodeString(v)
			if err != nil || len(p.SHA256) != 32 {
				h.fail(w, r, 501, "NotImplemented", "Unsupported payload hash or streaming encoding.")
				return
			}
		}
		o, err := h.store.PutObject(r.Context(), scope, p, http.MaxBytesReader(w, r.Body, h.opts.MaxObjectSize))
		if err != nil {
			h.storageError(w, r, err)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("%q", o.ETag))
		if o.VersionID != "" {
			w.Header().Set("x-amz-version-id", o.VersionID)
		}
		w.WriteHeader(200)
	case http.MethodGet:
		o, err := h.store.GetObject(r.Context(), scope, bucket, key)
		if err != nil {
			h.storageError(w, r, err)
			return
		}
		defer o.Body.Close()
		objectHeaders(w, o.Object)
		if _, err := io.Copy(w, o.Body); err != nil {
			if h.opts.Logger != nil {
				h.opts.Logger.ErrorContext(r.Context(), "response stream failed", "error", err)
			}
			panic(http.ErrAbortHandler)
		}
	case http.MethodHead:
		o, err := h.store.HeadObject(r.Context(), scope, bucket, key)
		if err != nil {
			h.storageError(w, r, err)
			return
		}
		objectHeaders(w, o)
		w.WriteHeader(200)
	case http.MethodDelete:
		if err := h.store.DeleteObject(r.Context(), scope, bucket, key); err != nil {
			h.storageError(w, r, err)
			return
		}
		w.WriteHeader(204)
	default:
		h.fail(w, r, 501, "NotImplemented", "Operation is not implemented.")
	}
}
func allowedAmz(k string) bool {
	return strings.HasPrefix(k, "x-amz-meta-") || k == "x-amz-date" || k == "x-amz-content-sha256" || k == "x-amz-security-token" || k == "x-amz-user-agent" || k == "x-amz-copy-source" || k == "x-amz-metadata-directive"
}

func hasOperationQuery(q map[string][]string) bool {
	for key := range q {
		if key != "x-id" {
			return true
		}
	}
	return false
}
func objectHeaders(w http.ResponseWriter, o Object) {
	if o.VersionID != "" {
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	for k, v := range o.Headers {
		w.Header().Set(k, v)
	}
	for k, v := range o.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.Header().Set("ETag", fmt.Sprintf("%q", o.ETag))
	if o.VersionID != "" {
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	w.Header().Set("Content-Length", fmt.Sprint(o.Size))
	w.Header().Set("Last-Modified", o.Modified.UTC().Format(http.TimeFormat))
}
