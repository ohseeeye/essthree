package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/ohseeeye/oci"
)

var ErrInvalidVersionMarker = errors.New("invalid version listing marker")

// Hex preserves UTF-8 byte order, while '/' sorts before a hex digit, preserving
// ordering between a key and a longer key with the same prefix.
func versionPrefix(key string) string { return hex.EncodeToString([]byte(key)) + "/" }
func versionSequence(version string) (int64, error) {
	if len(version) < 22 || version[20] != '-' {
		return 0, ErrInvalidVersionMarker
	}
	n, err := strconv.ParseInt(version[:20], 10, 64)
	if err != nil || n < 1 || n == math.MaxInt64 {
		return 0, ErrInvalidVersionMarker
	}
	return n, nil
}
func versionIndexKey(key, version string) (string, error) {
	n, err := versionSequence(version)
	if err != nil {
		return "", err
	}
	return versionPrefix(key) + fmt.Sprintf("%020d", math.MaxInt64-n) + "/" + version[21:], nil
}
func (s *OCIStore) prepareVersion(ctx context.Context, repo string, root bucketIndex, a map[string]string) error {
	cursor := indexCursor{store: s, ctx: ctx, repo: repo, root: root.versions}
	prefix := versionPrefix(a[annotation+"key"])
	if err := cursor.seek(prefix); err != nil {
		return err
	}
	d, ok, err := cursor.next()
	if err != nil {
		return err
	}
	sequence := int64(1)
	if ok && strings.HasPrefix(indexKey(d, 0), prefix) {
		n, err := versionSequence(d.Annotations[annotation+"version-id"])
		if err != nil {
			return err
		}
		if n >= math.MaxInt64-1 {
			return errors.New("object version sequence exhausted")
		}
		sequence = n + 1
	}
	a[annotation+"version-id"] = fmt.Sprintf("%020d-%s", sequence, id())
	return nil
}
func (s *OCIStore) appendVersion(ctx context.Context, repo string, root bucketIndex, d oci.Descriptor) (oci.Descriptor, error) {
	key := d.Annotations[annotation+"key"]
	encoded, err := versionIndexKey(key, d.Annotations[annotation+"version-id"])
	if err != nil {
		return oci.Descriptor{}, err
	}
	d.Annotations = cloneStrings(d.Annotations)
	d.Annotations[annotation+"version-key"] = key
	d.Annotations[annotation+"key"] = encoded
	return s.changeTree(ctx, repo, root.versions, encoded, &d)
}

func (s *OCIStore) ListObjectVersions(ctx context.Context, scope Scope, p VersionListRequest) ([]ObjectVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(ctx, scope, p.Bucket)
	if err != nil {
		return nil, err
	}
	repo := b.Annotations[annotation+"repo"]
	root, err := s.readIndex(ctx, repo)
	if err != nil {
		return nil, err
	}
	seek := hex.EncodeToString([]byte(p.Prefix))
	if p.KeyMarker != "" {
		marker := versionPrefix(p.KeyMarker)
		if p.VersionIDMarker != "" {
			marker, err = versionIndexKey(p.KeyMarker, p.VersionIDMarker)
			if err != nil {
				return nil, err
			}
			d, err := s.findEntry(ctx, repo, root.versions, marker)
			if err != nil || d.Annotations[annotation+"version-id"] != p.VersionIDMarker {
				return nil, ErrInvalidVersionMarker
			}
		} else {
			// Skip only this exact key, retaining longer keys with the same prefix.
			marker = hex.EncodeToString([]byte(p.KeyMarker)) + "0"
		}
		seek = max(seek, marker)
	} else if p.VersionIDMarker != "" {
		return nil, ErrInvalidVersionMarker
	}
	cursor := indexCursor{store: s, ctx: ctx, repo: repo, root: root.versions}
	if err := cursor.seek(seek); err != nil {
		return nil, err
	}
	var entries []ObjectVersion
	latestKey, latestID := "", ""
	for {
		d, ok, err := cursor.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		key, version := d.Annotations[annotation+"version-key"], d.Annotations[annotation+"version-id"]
		expected, err := versionIndexKey(key, version)
		if err != nil || key == "" || expected != indexKey(d, 0) {
			return nil, errors.New("invalid version descriptor")
		}
		if !strings.HasPrefix(key, p.Prefix) {
			break
		}
		if key == p.KeyMarker && version == p.VersionIDMarker {
			continue
		}
		if p.Delimiter != "" {
			if i := strings.Index(strings.TrimPrefix(key, p.Prefix), p.Delimiter); i >= 0 {
				group := p.Prefix + strings.TrimPrefix(key, p.Prefix)[:i+len(p.Delimiter)]
				if group > p.KeyMarker {
					entries = append(entries, ObjectVersion{CommonPrefix: group})
				}
				if p.Limit > 0 && len(entries) >= p.Limit {
					break
				}
				end, ok := prefixEnd(hex.EncodeToString([]byte(group)))
				if !ok {
					break
				}
				if err := cursor.seek(end); err != nil {
					return nil, err
				}
				continue
			}
		}
		if latestKey != key {
			first := indexCursor{store: s, ctx: ctx, repo: repo, root: root.versions}
			if err := first.seek(versionPrefix(key)); err != nil {
				return nil, err
			}
			head, ok, err := first.next()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errors.New("missing version head")
			}
			latestKey, latestID = key, head.Annotations[annotation+"version-id"]
		}
		object, err := objectInfo(oci.IndexOrManifest{Annotations: d.Annotations})
		if err != nil {
			return nil, err
		}
		object.Key = key
		entries = append(entries, ObjectVersion{Object: object, IsLatest: version == latestID, DeleteMarker: d.Annotations[annotation+"deleted"] == "true"})
		if p.Limit > 0 && len(entries) >= p.Limit {
			break
		}
	}
	return entries, nil
}
