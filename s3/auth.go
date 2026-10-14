package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Middleware wraps an HTTP handler. Authentication middleware must reject
// unauthenticated requests before invoking the next handler.
type Middleware func(http.Handler) http.Handler

// AuthOptions configures header-based AWS Signature Version 4 for one local owner.
// All authenticated requests have access to the same local storage scope.
type AuthOptions struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string        // defaults to us-east-1
	MaxClockSkew    time.Duration // defaults to 15 minutes; must be positive
	MaxBodySize     int64         // defaults to 5 GiB; must be positive
	TempDir         string        // defaults to the system temporary directory
	// RequireSignedPayload rejects UNSIGNED-PAYLOAD when true.
	RequireSignedPayload bool
}

// NewAuthMiddleware verifies S3 SigV4 Authorization headers against a configured
// key pair. Presigned URLs, session tokens, and aws-chunked bodies are unsupported.
// Signed bodies are verified before invoking next: up to 1 MiB is buffered in
// memory and larger bodies use a temporary file, removed when next returns.
// Configure this middleware before any URL, header, or body transformations.
func NewAuthMiddleware(opts AuthOptions) (Middleware, error) {
	if opts.AccessKeyID == "" || strings.ContainsAny(opts.AccessKeyID, "/, \t\r\n") || opts.SecretAccessKey == "" {
		return nil, errors.New("authentication requires a valid access key ID and a nonempty secret access key")
	}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if strings.ContainsAny(opts.Region, "/, \t\r\n") {
		return nil, errors.New("invalid authentication region")
	}
	if opts.MaxClockSkew == 0 {
		opts.MaxClockSkew = 15 * time.Minute
	}
	if opts.MaxBodySize == 0 {
		opts.MaxBodySize = 5 << 30
	}
	if opts.MaxClockSkew < 0 || opts.MaxBodySize < 0 {
		return nil, errors.New("authentication clock skew and body limit must be positive")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if w.Header().Get("x-amz-request-id") == "" {
				w.Header().Set("x-amz-request-id", rand.Text())
			}
			if failure := verifySignature(r, opts, time.Now()); failure != nil {
				failure.write(w, r)
				return
			}
			if r.ContentLength > opts.MaxBodySize {
				authFailure{400, "EntityTooLarge", "The request body exceeds the configured authentication limit."}.write(w, r)
				return
			}
			if r.Body == nil {
				r.Body = http.NoBody
			}
			body := http.MaxBytesReader(w, r.Body, opts.MaxBodySize)
			defer body.Close()
			if r.Header.Get("X-Amz-Content-Sha256") == "UNSIGNED-PAYLOAD" {
				r.Body = body
			} else {
				verified, cleanup, err := verifyBody(body, r.Header.Get("X-Amz-Content-Sha256"), opts.TempDir)
				if err != nil {
					failure := authFailure{400, "XAmzContentSHA256Mismatch", "The payload does not match x-amz-content-sha256."}
					var large *http.MaxBytesError
					switch {
					case errors.As(err, &large):
						failure = authFailure{400, "EntityTooLarge", "The request body exceeds the configured authentication limit."}
					case !errors.Is(err, ErrIntegrity):
						failure = authFailure{500, "InternalError", "Unable to verify the request body."}
					}
					failure.write(w, r)
					return
				}
				defer cleanup()
				r.Body = verified
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

type authFailure struct {
	status        int
	code, message string
}

func (f authFailure) write(w http.ResponseWriter, r *http.Request) {
	writeS3Error(w, r, f.status, f.code, f.message)
}

func verifySignature(r *http.Request, opts AuthOptions, now time.Time) *authFailure {
	malformed := &authFailure{400, "AuthorizationHeaderMalformed", "Malformed SigV4 authorization header."}
	denied := &authFailure{403, "AccessDenied", "Header-based SigV4 authentication is required."}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &authFailure{400, "InvalidArgument", "Malformed query."}
	}
	for name := range q {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") || name == "AWSAccessKeyId" || name == "Signature" {
			return denied // Do not mix header and query authentication.
		}
	}
	if len(r.Header.Values("Authorization")) != 1 {
		return denied
	}
	value, ok := strings.CutPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ")
	if !ok {
		return denied
	}
	fields := make(map[string]string)
	for _, field := range strings.Split(value, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok || value == "" || fields[name] != "" {
			return malformed
		}
		fields[name] = value
	}
	if len(fields) != 3 || fields["Credential"] == "" || fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return malformed
	}
	credential := strings.Split(fields["Credential"], "/")
	if len(credential) != 5 || credential[2] != opts.Region || credential[3] != "s3" || credential[4] != "aws4_request" {
		return malformed
	}
	if credential[0] != opts.AccessKeyID {
		return &authFailure{403, "InvalidAccessKeyId", "The access key ID does not exist."}
	}
	if len(r.Header.Values("X-Amz-Date")) != 1 || len(r.Header.Values("X-Amz-Content-Sha256")) != 1 {
		return malformed
	}
	date := r.Header.Get("X-Amz-Date")
	stamp, err := time.Parse("20060102T150405Z", date)
	if err != nil || credential[1] != stamp.Format("20060102") {
		return malformed
	}
	if stamp.Before(now.Add(-opts.MaxClockSkew)) || stamp.After(now.Add(opts.MaxClockSkew)) {
		return &authFailure{403, "RequestTimeTooSkewed", "The request timestamp is outside the allowed clock skew."}
	}
	if len(r.Header.Values("X-Amz-Security-Token")) != 0 {
		return &authFailure{403, "InvalidToken", "Session credentials are not supported."}
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if strings.HasPrefix(payload, "STREAMING-") || strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") || len(r.Trailer) != 0 {
		return &authFailure{501, "NotImplemented", "S3 streaming payload encoding is not implemented."}
	}
	if payload == "UNSIGNED-PAYLOAD" {
		if opts.RequireSignedPayload {
			return &authFailure{403, "AccessDenied", "A signed payload is required."}
		}
	} else if !validSHA256(payload) {
		return malformed
	}
	canonicalHeaders, err := signedHeaders(r, fields["SignedHeaders"])
	if err != nil {
		return malformed
	}
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{r.Method, path, canonicalQuery(q), canonicalHeaders, fields["SignedHeaders"], payload}, "\n")
	scope := strings.Join(credential[1:], "/")
	toSign := "AWS4-HMAC-SHA256\n" + date + "\n" + scope + "\n" + hashHex([]byte(canonical))
	key := signHMAC([]byte("AWS4"+opts.SecretAccessKey), credential[1])
	for _, value := range credential[2:] {
		key = signHMAC(key, value)
	}
	want := signHMAC(key, toSign)
	got, err := hex.DecodeString(fields["Signature"])
	if err != nil || !validSHA256(fields["Signature"]) || !hmac.Equal(want, got) {
		return &authFailure{403, "SignatureDoesNotMatch", "The request signature does not match."}
	}
	return nil
}

func signedHeaders(r *http.Request, list string) (string, error) {
	names := strings.Split(list, ";")
	seen := make(map[string]bool, len(names))
	var canonical strings.Builder
	for i, name := range names {
		if name == "" || name != strings.ToLower(name) || i > 0 && names[i-1] >= name || name == "authorization" {
			return "", errors.New("invalid signed headers")
		}
		seen[name] = true
		values := r.Header.Values(name)
		switch name {
		case "host":
			values = []string{r.Host}
		case "content-length":
			if r.ContentLength < 0 {
				return "", errors.New("unknown signed content length")
			}
			values = []string{strconv.FormatInt(r.ContentLength, 10)}
		}
		if len(values) == 0 {
			return "", errors.New("missing signed header")
		}
		clean := make([]string, len(values))
		for i, value := range values {
			clean[i] = strings.Join(strings.Fields(value), " ")
		}
		canonical.WriteString(name + ":" + strings.Join(clean, ",") + "\n")
	}
	if !seen["host"] || !seen["x-amz-date"] {
		return "", errors.New("host and x-amz-date must be signed")
	}
	for name := range r.Header {
		name = strings.ToLower(name)
		// The SDK excludes x-amz-user-agent, which carries only client telemetry.
		if (strings.HasPrefix(name, "x-amz-") && name != "x-amz-content-sha256" && name != "x-amz-user-agent" || name == "content-md5") && !seen[name] {
			return "", errors.New("unsigned S3 header")
		}
	}
	return canonical.String(), nil
}

func canonicalQuery(q url.Values) string {
	type pair struct{ name, value string }
	var pairs []pair
	for name, values := range q {
		for _, value := range values {
			pairs = append(pairs, pair{awsEncode(name), awsEncode(value)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].name == pairs[j].name {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].name < pairs[j].name
	})
	encoded := make([]string, len(pairs))
	for i, pair := range pairs {
		encoded[i] = pair.name + "=" + pair.value
	}
	return strings.Join(encoded, "&")
}

func awsEncode(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func hashHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func signHMAC(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func verifyBody(body io.Reader, expected, tempDir string) (io.ReadCloser, func(), error) {
	const memoryLimit = 1 << 20
	prefix, err := io.ReadAll(io.LimitReader(body, memoryLimit+1))
	if err != nil {
		return nil, nil, err
	}
	if len(prefix) <= memoryLimit {
		if hashHex(prefix) != expected {
			return nil, nil, ErrIntegrity
		}
		return io.NopCloser(bytes.NewReader(prefix)), func() {}, nil
	}
	f, err := os.CreateTemp(tempDir, "essthree-auth-*")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = f.Close(); _ = os.Remove(f.Name()) }
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, h), io.MultiReader(bytes.NewReader(prefix), body))
	if err == nil && hex.EncodeToString(h.Sum(nil)) != expected {
		err = ErrIntegrity
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("verify payload: %w", err)
	}
	return f, cleanup, nil
}
