package integration_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ohseeeye/essthree/s3"
	"github.com/ohseeeye/oci/ociclient"
	"github.com/ohseeeye/oci/ocimem"
	"github.com/ohseeeye/oci/ociserver"
)

func TestIndexThroughOCIHTTP(t *testing.T) {
	for _, ignoreConditions := range []bool{false, true} {
		name := "conditional"
		if ignoreConditions {
			name = "ignores-condition"
		}
		t.Run(name, func(t *testing.T) {
			registry, err := ociserver.New(ocimem.New(), nil)
			if err != nil {
				t.Fatal(err)
			}
			var conditions atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" && r.Header.Get("If-Match") != "" {
					conditions.Add(1)
					if ignoreConditions {
						r.Header.Del("If-Match")
					}
				}
				registry.ServeHTTP(w, r)
			}))
			defer endpoint.Close()
			client, err := ociclient.New(strings.TrimPrefix(endpoint.URL, "http://"), &ociclient.Options{Insecure: true})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := s3.NewHandler(client, s3.Options{DevelopmentMode: true})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			request := func(method, path, body string, status int) string {
				t.Helper()
				r, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				response, err := server.Client().Do(r)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(response.Body)
				if err != nil || response.StatusCode != status {
					t.Fatalf("%s %s: %d %s %v", method, path, response.StatusCode, raw, err)
				}
				return string(raw)
			}
			if ignoreConditions {
				request("PUT", "/remote-bucket", "", 500)
				request("HEAD", "/remote-bucket", "", 404)
				return
			}
			request("PUT", "/remote-bucket", "", 200)
			request("PUT", "/remote-bucket/a", "remote payload", 200)
			if got := request("GET", "/remote-bucket/a", "", 200); got != "remote payload" {
				t.Fatal(got)
			}
			if got := request("GET", "/remote-bucket?list-type=2", "", 200); !strings.Contains(got, "<Key>a</Key>") {
				t.Fatal(got)
			}
			request("DELETE", "/remote-bucket/a", "", 204)
			request("GET", "/remote-bucket/a", "", 404)
			if conditions.Load() < 3 || !client.SupportsIfMatch() {
				t.Fatal("conditional root writes were not discovered and sent")
			}
		})
	}
}
