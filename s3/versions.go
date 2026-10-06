package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
)

type versionXML struct {
	XMLName      xml.Name
	Key          string                           `xml:"Key"`
	VersionID    string                           `xml:"VersionId"`
	IsLatest     bool                             `xml:"IsLatest"`
	LastModified string                           `xml:"LastModified"`
	Owner        struct{ ID, DisplayName string } `xml:"Owner"`
	ETag         string                           `xml:"ETag,omitempty"`
	Size         *int64                           `xml:"Size,omitempty"`
	StorageClass string                           `xml:"StorageClass,omitempty"`
}

func (h *Handler) listObjectVersions(w http.ResponseWriter, r *http.Request, scope Scope, bucket string, q url.Values) {
	allowed := map[string]bool{"versions": true, "x-id": true, "prefix": true, "delimiter": true, "encoding-type": true, "max-keys": true, "key-marker": true, "version-id-marker": true}
	for key := range q {
		if !allowed[key] {
			h.fail(w, r, 501, "NotImplemented", "This query operation is not implemented.")
			return
		}
		if _, err := queryValue(q, key); err != nil {
			h.fail(w, r, 400, "InvalidArgument", err.Error())
			return
		}
	}
	p := VersionListRequest{Bucket: bucket, Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), KeyMarker: q.Get("key-marker"), VersionIDMarker: q.Get("version-id-marker")}
	encoding := q.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		h.fail(w, r, 400, "InvalidArgument", "encoding-type must be url.")
		return
	}
	if p.VersionIDMarker != "" && p.KeyMarker == "" {
		h.fail(w, r, 400, "InvalidArgument", "version-id-marker requires key-marker.")
		return
	}
	maxKeys := defaultMaxKeys
	if raw := q.Get("max-keys"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			h.fail(w, r, 400, "InvalidArgument", "max-keys must be a non-negative integer.")
			return
		}
		maxKeys = min(n, defaultMaxKeys)
	}
	p.Limit = maxKeys + 1
	entries, err := h.store.ListObjectVersions(r.Context(), scope, p)
	if err != nil {
		h.storageError(w, r, err)
		return
	}
	truncated := maxKeys > 0 && len(entries) > maxKeys
	if maxKeys == 0 {
		entries = nil
	} else if truncated {
		entries = entries[:maxKeys]
	}
	result := struct {
		XMLName             xml.Name `xml:"ListVersionsResult"`
		XMLNS               string   `xml:"xmlns,attr"`
		Name                string   `xml:"Name"`
		Prefix              string   `xml:"Prefix"`
		Delimiter           string   `xml:"Delimiter,omitempty"`
		KeyMarker           string   `xml:"KeyMarker"`
		VersionIDMarker     string   `xml:"VersionIdMarker"`
		NextKeyMarker       string   `xml:"NextKeyMarker,omitempty"`
		NextVersionIDMarker string   `xml:"NextVersionIdMarker,omitempty"`
		MaxKeys             int      `xml:"MaxKeys"`
		IsTruncated         bool     `xml:"IsTruncated"`
		EncodingType        string   `xml:"EncodingType,omitempty"`
		Entries             []versionXML
		Prefixes            []commonPrefix `xml:"CommonPrefixes"`
	}{XMLNS: xmlns, Name: bucket, Prefix: encodeListKey(p.Prefix, encoding), Delimiter: encodeListKey(p.Delimiter, encoding), KeyMarker: encodeListKey(p.KeyMarker, encoding), VersionIDMarker: p.VersionIDMarker, MaxKeys: maxKeys, IsTruncated: truncated, EncodingType: encoding}
	for _, entry := range entries {
		if entry.CommonPrefix != "" {
			result.Prefixes = append(result.Prefixes, commonPrefix{Prefix: encodeListKey(entry.CommonPrefix, encoding)})
			continue
		}
		v := versionXML{XMLName: xml.Name{Local: "Version"}, Key: encodeListKey(entry.Key, encoding), VersionID: entry.VersionID, IsLatest: entry.IsLatest, LastModified: entry.Modified.UTC().Format("2006-01-02T15:04:05.000Z")}
		v.Owner.ID, v.Owner.DisplayName = "local", "local"
		if entry.DeleteMarker {
			v.XMLName.Local = "DeleteMarker"
		} else {
			v.ETag = strconv.Quote(entry.ETag)
			size := entry.Size
			v.Size = &size
			v.StorageClass = "STANDARD"
		}
		result.Entries = append(result.Entries, v)
	}
	if truncated {
		last := entries[len(entries)-1]
		if last.CommonPrefix != "" {
			result.NextKeyMarker = encodeListKey(last.CommonPrefix, encoding)
		} else {
			result.NextKeyMarker = encodeListKey(last.Key, encoding)
			result.NextVersionIDMarker = last.VersionID
		}
	}
	writeXML(w, result)
}
