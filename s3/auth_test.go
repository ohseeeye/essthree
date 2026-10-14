package s3

import (
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ohseeeye/oci/ocimem"
)

// Fixed vectors from AWS, independent of our canonicalization/signing helpers:
// https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html
func TestAWSAuthVectors(t *testing.T) {
	opts := AuthOptions{AccessKeyID: "AKIAIOSFODNN7EXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", Region: "us-east-1", MaxClockSkew: 15 * time.Minute}
	now, _ := time.Parse("20060102T150405Z", "20130524T000000Z")
	for _, tt := range []struct {
		name, method, path, body, headers, signature string
		extra                                        map[string]string
	}{
		{"get", "GET", "/test.txt", "", "host;range;x-amz-content-sha256;x-amz-date", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", map[string]string{"Range": "bytes=0-9"}},
		{"put", "PUT", "/test%24file.text", "Welcome to Amazon S3.", "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class", "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd", map[string]string{"Date": "Fri, 24 May 2013 00:00:00 GMT", "X-Amz-Storage-Class": "REDUCED_REDUNDANCY"}},
		{"subresource", "GET", "/?lifecycle", "", "host;x-amz-content-sha256;x-amz-date", "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543", nil},
		{"query", "GET", "/?max-keys=2&prefix=J", "", "host;x-amz-content-sha256;x-amz-date", "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, "https://examplebucket.s3.amazonaws.com"+tt.path, strings.NewReader(tt.body))
			r.Header.Set("X-Amz-Date", "20130524T000000Z")
			r.Header.Set("X-Amz-Content-Sha256", hashHex([]byte(tt.body)))
			for k, v := range tt.extra {
				r.Header.Set(k, v)
			}
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+opts.AccessKeyID+"/20130524/us-east-1/s3/aws4_request,SignedHeaders="+tt.headers+",Signature="+tt.signature)
			if err := verifySignature(r, opts, now); err != nil {
				t.Fatalf("AWS vector rejected: %+v", err)
			}
		})
	}
}

func authRequest(method, body string) *http.Request {
	r := httptest.NewRequest(method, "http://localhost/test-bucket/key?uploadId=a%2Bb&partNumber=1", strings.NewReader(body))
	r.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	r.Header.Set("X-Amz-Content-Sha256", hashHex([]byte(body)))
	resign(r, "test", "secret", "us-east-1", "s3", "host;x-amz-content-sha256;x-amz-date")
	return r
}

func resign(r *http.Request, access, secret, region, service, headers string) {
	date := r.Header.Get("X-Amz-Date")
	scope := date[:8] + "/" + region + "/" + service + "/aws4_request"
	canonicalHeaders, _ := signedHeaders(r, headers)
	q, _ := parseQuery(r)
	canonical := strings.Join([]string{r.Method, r.URL.EscapedPath(), canonicalQuery(q), canonicalHeaders, headers, r.Header.Get("X-Amz-Content-Sha256")}, "\n")
	key := []byte("AWS4" + secret)
	for _, value := range strings.Split(scope, "/") {
		key = signHMAC(key, value)
	}
	signature := signHMAC(key, "AWS4-HMAC-SHA256\n"+date+"\n"+scope+"\n"+hashHex([]byte(canonical)))
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+access+"/"+scope+", SignedHeaders="+headers+", Signature="+hex.EncodeToString(signature))
}

func TestAuthRejectsBeforeNext(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		mutate     func(*http.Request)
	}{
		{"missing", "AccessDenied", func(r *http.Request) { r.Header.Del("Authorization") }},
		{"basic", "AccessDenied", func(r *http.Request) { r.SetBasicAuth("test", "secret") }},
		{"duplicate authorization", "AccessDenied", func(r *http.Request) { r.Header.Add("Authorization", r.Header.Get("Authorization")) }},
		{"malformed", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test") }},
		{"duplicate field", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Set("Authorization", r.Header.Get("Authorization")+", Signature=abc") }},
		{"key", "InvalidAccessKeyId", func(r *http.Request) {
			resign(r, "unknown", "secret", "us-east-1", "s3", "host;x-amz-content-sha256;x-amz-date")
		}},
		{"secret", "SignatureDoesNotMatch", func(r *http.Request) {
			resign(r, "test", "wrong", "us-east-1", "s3", "host;x-amz-content-sha256;x-amz-date")
		}},
		{"region", "AuthorizationHeaderMalformed", func(r *http.Request) {
			resign(r, "test", "secret", "us-west-2", "s3", "host;x-amz-content-sha256;x-amz-date")
		}},
		{"service", "AuthorizationHeaderMalformed", func(r *http.Request) {
			resign(r, "test", "secret", "us-east-1", "sts", "host;x-amz-content-sha256;x-amz-date")
		}},
		{"expired", "RequestTimeTooSkewed", func(r *http.Request) {
			r.Header.Set("X-Amz-Date", time.Now().Add(-16*time.Minute).UTC().Format("20060102T150405Z"))
		}},
		{"future", "RequestTimeTooSkewed", func(r *http.Request) {
			r.Header.Set("X-Amz-Date", time.Now().Add(16*time.Minute).UTC().Format("20060102T150405Z"))
		}},
		{"bad date", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Set("X-Amz-Date", "invalid") }},
		{"duplicate date", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Add("X-Amz-Date", r.Header.Get("X-Amz-Date")) }},
		{"token", "InvalidToken", func(r *http.Request) { r.Header.Set("X-Amz-Security-Token", "token") }},
		{"unsigned metadata", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Set("X-Amz-Meta-Owner", "intruder") }},
		{"unsigned copy source", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Set("X-Amz-Copy-Source", "bucket/key") }},
		{"method", "SignatureDoesNotMatch", func(r *http.Request) { r.Method = "DELETE" }},
		{"path", "SignatureDoesNotMatch", func(r *http.Request) { r.URL.Path += "changed" }},
		{"query", "SignatureDoesNotMatch", func(r *http.Request) { r.URL.RawQuery += "&extra=true" }},
		{"host", "SignatureDoesNotMatch", func(r *http.Request) { r.Host = "otherhost" }},
		{"body", "XAmzContentSHA256Mismatch", func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader("tampered")) }},
		{"payload header", "SignatureDoesNotMatch", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", hashHex([]byte("tampered"))) }},
		{"streaming", "NotImplemented", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") }},
		{"presigned", "AccessDenied", func(r *http.Request) { r.URL.RawQuery += "&X-Amz-Signature=abc" }},
		{"bad query", "InvalidArgument", func(r *http.Request) { r.URL.RawQuery = "bad=%zz" }},
		{"duplicate hash", "AuthorizationHeaderMalformed", func(r *http.Request) { r.Header.Add("X-Amz-Content-Sha256", r.Header.Get("X-Amz-Content-Sha256")) }},
		{"missing signed header", "AuthorizationHeaderMalformed", func(r *http.Request) {
			r.Header.Set("Authorization", strings.ReplaceAll(r.Header.Get("Authorization"), "SignedHeaders=host;", "SignedHeaders=content-type;host;"))
		}},
		{"unsorted headers", "AuthorizationHeaderMalformed", func(r *http.Request) {
			r.Header.Set("Authorization", strings.ReplaceAll(r.Header.Get("Authorization"), "host;x-amz-content-sha256", "x-amz-content-sha256;host"))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			auth, err := NewAuthMiddleware(AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("rejected request reached next handler") })
			r := authRequest("PUT", "original")
			tt.mutate(r)
			w := httptest.NewRecorder()
			auth(next).ServeHTTP(w, r)
			if w.Code < 400 || !strings.Contains(w.Body.String(), "<Code>"+tt.code+"</Code>") || w.Header().Get("x-amz-request-id") == "" {
				t.Fatalf("unexpected rejection: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestAuthBodyVerificationAndCleanup(t *testing.T) {
	for _, size := range []int{0, 128, 1 << 20, 1<<20 + 1, 2 << 20} {
		for _, tamper := range []bool{false, true} {
			t.Run(strconv.Itoa(size)+"/tamper="+strconv.FormatBool(tamper), func(t *testing.T) {
				dir := t.TempDir()
				body := strings.Repeat("x", size)
				r := authRequest("POST", body)
				if tamper {
					r.Body = io.NopCloser(strings.NewReader(body + "y"))
				}
				auth, err := NewAuthMiddleware(AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret", TempDir: dir})
				if err != nil {
					t.Fatal(err)
				}
				called := false
				next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					data, err := io.ReadAll(r.Body)
					if err != nil || string(data) != body {
						t.Fatalf("verified body: len=%d err=%v", len(data), err)
					}
					w.WriteHeader(204)
				})
				w := httptest.NewRecorder()
				auth(next).ServeHTTP(w, r)
				if called == tamper || !tamper && w.Code != 204 || tamper && w.Code != 400 {
					t.Fatalf("called=%v status=%d", called, w.Code)
				}
				files, err := os.ReadDir(dir)
				if err != nil || len(files) != 0 {
					t.Fatalf("temporary files left behind: %v %v", files, err)
				}
			})
		}
	}
}

func TestAuthBodyLimitsAndUnsignedPayload(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		for _, require := range []bool{false, true} {
			for _, unknownLength := range []bool{false, true} {
				opts := AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret", MaxBodySize: 5, RequireSignedPayload: require}
				auth, err := NewAuthMiddleware(opts)
				if err != nil {
					t.Fatal(err)
				}
				r := authRequest("PUT", "123456")
				if unsigned {
					r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
				}
				if unknownLength {
					r.ContentLength = -1
				}
				resign(r, "test", "secret", "us-east-1", "s3", "host;x-amz-content-sha256;x-amz-date")
				next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if _, err := io.ReadAll(r.Body); err == nil {
						t.Error("oversized unsigned body accepted")
					}
					w.WriteHeader(400)
				})
				w := httptest.NewRecorder()
				auth(next).ServeHTTP(w, r)
				want := 400
				if unsigned && require {
					want = 403
				}
				if w.Code != want {
					t.Fatalf("unsigned=%v require=%v unknown=%v: %d", unsigned, require, unknownLength, w.Code)
				}
			}
		}
	}
	// An unsigned payload is accepted when explicitly permitted, without buffering.
	auth, _ := NewAuthMiddleware(AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret"})
	r := authRequest("PUT", "unsigned")
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	resign(r, "test", "secret", "us-east-1", "s3", "host;x-amz-content-sha256;x-amz-date")
	w := httptest.NewRecorder()
	auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })).ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatalf("unsigned: %d %s", w.Code, w.Body.String())
	}
}

func TestHandlerAuthConfiguration(t *testing.T) {
	auth, _ := NewAuthMiddleware(AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret"})
	for _, opts := range []Options{{}, {DevelopmentMode: true, Authentication: auth}, {Authentication: func(http.Handler) http.Handler { return nil }}} {
		if _, err := NewHandler(ocimem.New(), opts); err == nil {
			t.Fatal("invalid authentication configuration accepted")
		}
	}
	h, err := NewHandler(ocimem.New(), Options{Authentication: auth})
	if err != nil {
		t.Fatal(err)
	}
	// Authentication precedes even malformed routing and HEAD never has an error body.
	for _, method := range []string{"PUT", "HEAD"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/invalid_bucket", nil))
		if w.Code != 403 || w.Header().Get("x-amz-request-id") == "" || method == "HEAD" && w.Body.Len() != 0 {
			t.Fatalf("%s: %d %s", method, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	r := authRequest("GET", "")
	r.URL.Path, r.URL.RawQuery = "/", ""
	resign(r, "test", "secret", "us-east-1", "s3", "host;x-amz-content-sha256;x-amz-date")
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("authenticated handler: %d %s", w.Code, w.Body.String())
	}
}

func TestInvalidAuthOptions(t *testing.T) {
	for _, opts := range []AuthOptions{
		{}, {AccessKeyID: "test"}, {SecretAccessKey: "secret"},
		{AccessKeyID: "bad/key", SecretAccessKey: "secret"},
		{AccessKeyID: "test", SecretAccessKey: "secret", Region: "bad/region"},
		{AccessKeyID: "test", SecretAccessKey: "secret", MaxClockSkew: -time.Second},
		{AccessKeyID: "test", SecretAccessKey: "secret", MaxBodySize: -1},
	} {
		if _, err := NewAuthMiddleware(opts); err == nil {
			t.Fatal("invalid auth options accepted")
		}
	}
}

func TestAuthCanonicalQueryEncoding(t *testing.T) {
	q := url.Values{"a": {"z", " ", "+"}, "a!": {""}, "a~": {"日本語"}, "uploads": {""}}
	want := "a=%20&a=%2B&a=z&a%21=&a~=%E6%97%A5%E6%9C%AC%E8%AA%9E&uploads="
	if got := canonicalQuery(q); got != want {
		t.Fatalf("canonical query: %s", got)
	}
}

type authReadFailure struct{}

func (authReadFailure) Read([]byte) (int, error) { return 0, errors.New("body read failed") }

func TestAuthSpoolFailures(t *testing.T) {
	for _, size := range []int{128, 2 << 20} {
		dir := t.TempDir()
		body := io.MultiReader(strings.NewReader(strings.Repeat("x", size)), authReadFailure{})
		if _, _, err := verifyBody(body, hashHex(nil), dir); err == nil {
			t.Fatal("body read failure ignored")
		}
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 {
			t.Fatalf("temporary files after read failure: %v %v", files, err)
		}
	}
	auth, _ := NewAuthMiddleware(AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret", TempDir: t.TempDir() + "/missing"})
	w := httptest.NewRecorder()
	auth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("failed spool reached next") })).ServeHTTP(w, authRequest("PUT", strings.Repeat("x", 2<<20)))
	if w.Code != 500 || !strings.Contains(w.Body.String(), "<Code>InternalError</Code>") {
		t.Fatalf("spool failure: %d %s", w.Code, w.Body.String())
	}
}
