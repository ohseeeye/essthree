package s3

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const defaultMaxKeys = 1000

type listError struct {
	status  int
	code    string
	message string
}

func (e *listError) Error() string { return e.message }

type listOptions struct {
	v2         bool
	prefix     string
	delimiter  string
	after      string
	maxKeys    int
	encoding   string
	token      string
	startAfter string
}

type listCursor struct {
	Version   int    `json:"v"`
	Prefix    string `json:"p"`
	Delimiter string `json:"d"`
	Last      string `json:"l"`
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, scope Scope, bucket string, q url.Values) {
	opts, err := parseListOptions(q)
	if err != nil {
		e := err.(*listError)
		h.fail(w, r, e.status, e.code, e.message)
		return
	}
	objects, err := h.store.ListObjects(r.Context(), scope, ListRequest{Bucket: bucket, Prefix: opts.prefix, Delimiter: opts.delimiter, After: opts.after, Limit: opts.maxKeys + 1})
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	entries := listEntries(objects, opts.prefix, opts.delimiter, opts.after)
	truncated := opts.maxKeys > 0 && len(entries) > opts.maxKeys
	if opts.maxKeys == 0 {
		entries = nil
		truncated = false
	} else if truncated {
		entries = entries[:opts.maxKeys]
	}

	if opts.v2 {
		h.writeListV2(w, bucket, opts, entries, truncated)
		return
	}
	h.writeListV1(w, bucket, opts, entries, truncated)
}

func parseListOptions(q url.Values) (listOptions, error) {
	allowed := map[string]bool{
		"x-id": true, "list-type": true, "prefix": true, "delimiter": true,
		"max-keys": true, "marker": true, "continuation-token": true,
		"start-after": true, "encoding-type": true, "fetch-owner": true,
	}
	for key := range q {
		if !allowed[key] {
			return listOptions{}, &listError{http.StatusNotImplemented, "NotImplemented", "This query operation is not implemented."}
		}
	}

	listType, err := queryValue(q, "list-type")
	if err != nil {
		return listOptions{}, err
	}
	opts := listOptions{v2: listType != "", maxKeys: defaultMaxKeys}
	if listType != "" && listType != "2" {
		return listOptions{}, &listError{http.StatusNotImplemented, "NotImplemented", "Only ListObjectsV2 is supported for list-type requests."}
	}
	for _, field := range []struct {
		name string
		to   *string
	}{
		{"prefix", &opts.prefix},
		{"delimiter", &opts.delimiter},
		{"encoding-type", &opts.encoding},
	} {
		value, err := queryValue(q, field.name)
		if err != nil {
			return listOptions{}, err
		}
		*field.to = value
	}
	if opts.encoding != "" && opts.encoding != "url" {
		return listOptions{}, &listError{http.StatusBadRequest, "InvalidArgument", "encoding-type must be url."}
	}
	maxKeys, err := queryValue(q, "max-keys")
	if err != nil {
		return listOptions{}, err
	}
	if maxKeys != "" {
		n, err := strconv.Atoi(maxKeys)
		if err != nil || n < 0 {
			return listOptions{}, &listError{http.StatusBadRequest, "InvalidArgument", "max-keys must be a non-negative integer."}
		}
		opts.maxKeys = min(n, defaultMaxKeys)
	}
	if opts.v2 {
		if _, ok := q["marker"]; ok {
			return listOptions{}, &listError{http.StatusBadRequest, "InvalidArgument", "marker is not valid for ListObjectsV2."}
		}
		if opts.token, err = queryValue(q, "continuation-token"); err != nil {
			return listOptions{}, err
		}
		if opts.startAfter, err = queryValue(q, "start-after"); err != nil {
			return listOptions{}, err
		}
		if opts.token != "" && opts.startAfter != "" {
			return listOptions{}, &listError{http.StatusBadRequest, "InvalidArgument", "continuation-token and start-after cannot be used together."}
		}
		if opts.token != "" {
			cursor, err := decodeCursor(opts.token)
			if err != nil || cursor.Prefix != opts.prefix || cursor.Delimiter != opts.delimiter {
				return listOptions{}, &listError{http.StatusBadRequest, "InvalidToken", "The continuation token is invalid for this listing."}
			}
			opts.after = cursor.Last
		} else {
			opts.after = opts.startAfter
		}
	} else {
		if _, ok := q["continuation-token"]; ok {
			return listOptions{}, &listError{http.StatusBadRequest, "InvalidArgument", "continuation-token is not valid for ListObjects."}
		}
		if _, ok := q["start-after"]; ok {
			return listOptions{}, &listError{http.StatusBadRequest, "InvalidArgument", "start-after is not valid for ListObjects."}
		}
		if opts.after, err = queryValue(q, "marker"); err != nil {
			return listOptions{}, err
		}
	}
	return opts, nil
}

func queryValue(q url.Values, name string) (string, error) {
	values, ok := q[name]
	if !ok {
		return "", nil
	}
	if len(values) != 1 {
		return "", &listError{http.StatusBadRequest, "InvalidArgument", "Duplicate " + name + " parameters are not supported."}
	}
	return values[0], nil
}

func decodeCursor(token string) (listCursor, error) {
	var cursor listCursor
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor, err
	}
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.Version != 1 || cursor.Last == "" {
		return listCursor{}, &listError{}
	}
	return cursor, nil
}

func encodeCursor(prefix, delimiter, last string) string {
	data, _ := json.Marshal(listCursor{Version: 1, Prefix: prefix, Delimiter: delimiter, Last: last})
	return base64.RawURLEncoding.EncodeToString(data)
}

type listEntry struct {
	name   string
	object *Object
}

func listEntries(objects []Object, prefix, delimiter, after string) []listEntry {
	entries := make([]listEntry, 0, len(objects))
	prefixes := make(map[string]struct{})
	for i := range objects {
		object := &objects[i]
		if !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		name := object.Key
		if delimiter != "" {
			rest := strings.TrimPrefix(object.Key, prefix)
			if index := strings.Index(rest, delimiter); index >= 0 {
				name = prefix + rest[:index+len(delimiter)]
				if _, seen := prefixes[name]; seen {
					continue
				}
				prefixes[name] = struct{}{}
				object = nil
			}
		}
		if name > after {
			entries = append(entries, listEntry{name: name, object: object})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	return entries
}

type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func listContentFor(object Object, encoding string) listContent {
	return listContent{
		Key:          encodeListKey(object.Key, encoding),
		LastModified: object.Modified.UTC().Format("2006-01-02T15:04:05.000Z"),
		ETag:         `"` + object.ETag + `"`,
		Size:         object.Size,
		StorageClass: "STANDARD",
	}
}

func encodeListKey(key, encoding string) string {
	if encoding != "url" {
		return key
	}
	return strings.ReplaceAll(url.QueryEscape(key), "+", "%20")
}

func prefixesAndContents(entries []listEntry, encoding string) ([]commonPrefix, []listContent) {
	prefixes := make([]commonPrefix, 0)
	contents := make([]listContent, 0)
	for _, entry := range entries {
		if entry.object == nil {
			prefixes = append(prefixes, commonPrefix{Prefix: encodeListKey(entry.name, encoding)})
			continue
		}
		contents = append(contents, listContentFor(*entry.object, encoding))
	}
	return prefixes, contents
}

func (h *Handler) writeListV1(w http.ResponseWriter, bucket string, opts listOptions, entries []listEntry, truncated bool) {
	prefixes, contents := prefixesAndContents(entries, opts.encoding)
	result := struct {
		XMLName      xml.Name       `xml:"ListBucketResult"`
		XMLNS        string         `xml:"xmlns,attr"`
		Name         string         `xml:"Name"`
		Prefix       string         `xml:"Prefix"`
		Marker       string         `xml:"Marker"`
		NextMarker   string         `xml:"NextMarker,omitempty"`
		MaxKeys      int            `xml:"MaxKeys"`
		Delimiter    string         `xml:"Delimiter,omitempty"`
		EncodingType string         `xml:"EncodingType,omitempty"`
		IsTruncated  bool           `xml:"IsTruncated"`
		Contents     []listContent  `xml:"Contents"`
		Prefixes     []commonPrefix `xml:"CommonPrefixes"`
	}{
		XMLNS: xmlns, Name: bucket, Prefix: encodeListKey(opts.prefix, opts.encoding),
		Marker: encodeListKey(opts.after, opts.encoding), MaxKeys: opts.maxKeys,
		Delimiter: encodeListKey(opts.delimiter, opts.encoding), EncodingType: opts.encoding,
		IsTruncated: truncated, Contents: contents, Prefixes: prefixes,
	}
	if truncated {
		result.NextMarker = encodeListKey(entries[len(entries)-1].name, opts.encoding)
	}
	writeXML(w, result)
}

func (h *Handler) writeListV2(w http.ResponseWriter, bucket string, opts listOptions, entries []listEntry, truncated bool) {
	prefixes, contents := prefixesAndContents(entries, opts.encoding)
	result := struct {
		XMLName               xml.Name       `xml:"ListBucketResult"`
		XMLNS                 string         `xml:"xmlns,attr"`
		Name                  string         `xml:"Name"`
		Prefix                string         `xml:"Prefix"`
		KeyCount              int            `xml:"KeyCount"`
		MaxKeys               int            `xml:"MaxKeys"`
		Delimiter             string         `xml:"Delimiter,omitempty"`
		IsTruncated           bool           `xml:"IsTruncated"`
		ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
		StartAfter            string         `xml:"StartAfter,omitempty"`
		EncodingType          string         `xml:"EncodingType,omitempty"`
		Contents              []listContent  `xml:"Contents"`
		Prefixes              []commonPrefix `xml:"CommonPrefixes"`
	}{
		XMLNS: xmlns, Name: bucket, Prefix: encodeListKey(opts.prefix, opts.encoding),
		KeyCount: len(entries), MaxKeys: opts.maxKeys,
		Delimiter: encodeListKey(opts.delimiter, opts.encoding), IsTruncated: truncated,
		ContinuationToken: opts.token, StartAfter: encodeListKey(opts.startAfter, opts.encoding),
		EncodingType: opts.encoding, Contents: contents, Prefixes: prefixes,
	}
	if truncated {
		result.NextContinuationToken = encodeCursor(opts.prefix, opts.delimiter, entries[len(entries)-1].name)
	}
	writeXML(w, result)
}
