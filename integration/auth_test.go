package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/ohseeeye/essthree/s3"
)

func TestSigV4SignerCompatibility(t *testing.T) {
	for _, path := range []string{
		"/bucket/ordinary", "/bucket/space%20and+plus", "/bucket/percent%252F",
		"/bucket/%E6%97%A5%E6%9C%AC%E8%AA%9E/", "/bucket/a//b/../c/",
		"/bucket/encoded%2Fslash", "/bucket/key?uploads", "/bucket/key?uploadId=a%2Bb%2Fc%3D&partNumber=2",
		"/bucket?list-type=2&prefix=space+and%2Bplus&delimiter=%2F",
		"/bucket?prefix=z&prefix=a&prefix=%2B&prefix=+&max-keys=10",
	} {
		t.Run(path, func(t *testing.T) {
			for _, unsigned := range []bool{false, true} {
				body := "sdk signed body"
				r, err := http.NewRequest("PUT", "http://localhost:9000"+path, strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256([]byte(body))
				payload := hex.EncodeToString(digest[:])
				if unsigned {
					payload = "UNSIGNED-PAYLOAD"
				}
				r.Header.Set("X-Amz-Content-Sha256", payload)
				r.Header["X-Amz-Meta-Labels"] = []string{"  alpha   beta  ", "gamma"}
				err = v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test", SecretAccessKey: "secret"}, r, payload, "s3", "us-east-1", time.Now(), func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
				if err != nil {
					t.Fatal(err)
				}
				auth, err := s3.NewAuthMiddleware(s3.AuthOptions{AccessKeyID: "test", SecretAccessKey: "secret"})
				if err != nil {
					t.Fatal(err)
				}
				next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					data, err := io.ReadAll(r.Body)
					if err != nil || string(data) != body {
						t.Fatalf("body=%s err=%v", data, err)
					}
					w.WriteHeader(204)
				})
				w := httptest.NewRecorder()
				auth(next).ServeHTTP(w, r)
				if w.Code != 204 {
					t.Fatalf("unsigned=%v: %d %s", unsigned, w.Code, w.Body.String())
				}
			}
		})
	}
}
