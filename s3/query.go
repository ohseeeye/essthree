package s3

import (
	"net/http"
	"net/url"
)

func parseQuery(r *http.Request) (url.Values, error) { return url.ParseQuery(r.URL.RawQuery) }
