package integration_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ohseeeye/essthree/s3"
	"github.com/ohseeeye/oci/ocisqlite"
)

func TestSDKKeyAndVersionPagination(t *testing.T) {
	dir := t.TempDir()
	reg, err := ocisqlite.New(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if reg != nil {
			_ = reg.Close()
		}
	}()
	handler, err := s3.NewHandler(reg, s3.Options{DevelopmentMode: true})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	clientFor := func(endpoint string) *awss3.Client {
		return awss3.New(awss3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	}
	client := clientFor(srv.URL)
	ctx := context.Background()
	bucket := aws.String("versions-test")
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, body := range []string{"first", "second", "third"} {
		out, err := client.PutObject(ctx, &awss3.PutObjectInput{Bucket: bucket, Key: aws.String("a"), Body: strings.NewReader(body)})
		if err != nil || aws.ToString(out.VersionId) == "" {
			t.Fatalf("put: %v %v", out, err)
		}
		ids = append(ids, aws.ToString(out.VersionId))
	}
	if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: bucket, Key: aws.String("a")}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"b +", "folder/a", "folder/b", "日本語"} {
		if _, err := client.PutObject(ctx, &awss3.PutObjectInput{Bucket: bucket, Key: aws.String(key), Body: strings.NewReader(key)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.CopyObject(ctx, &awss3.CopyObjectInput{Bucket: bucket, Key: aws.String("b +"), CopySource: aws.String("versions-test/folder/a")}); err != nil {
		t.Fatal(err)
	}
	u, err := client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("b +")})
	if err != nil {
		t.Fatal(err)
	}
	part, err := client.UploadPart(ctx, &awss3.UploadPartInput{Bucket: bucket, Key: aws.String("b +"), UploadId: u.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("multipart")})
	if err != nil {
		t.Fatal(err)
	}
	complete := &awss3.CompleteMultipartUploadInput{Bucket: bucket, Key: aws.String("b +"), UploadId: u.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}}}
	if _, err := client.CompleteMultipartUpload(ctx, complete); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteMultipartUpload(ctx, complete); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	if err := reg.Close(); err != nil {
		t.Fatal(err)
	}
	reg, err = ocisqlite.New(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = s3.NewHandler(reg, s3.Options{DevelopmentMode: true})
	if err != nil {
		t.Fatal(err)
	}
	srv = httptest.NewServer(handler)
	defer srv.Close()
	client = clientFor(srv.URL)
	for _, v2 := range []bool{false, true} {
		var keys []string
		if v2 {
			pager := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{Bucket: bucket, MaxKeys: aws.Int32(1)})
			for pager.HasMorePages() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, o := range page.Contents {
					keys = append(keys, aws.ToString(o.Key))
				}
			}
		} else {
			marker := ""
			for {
				page, err := client.ListObjects(ctx, &awss3.ListObjectsInput{Bucket: bucket, MaxKeys: aws.Int32(1), Marker: aws.String(marker)})
				if err != nil {
					t.Fatal(err)
				}
				for _, o := range page.Contents {
					keys = append(keys, aws.ToString(o.Key))
				}
				if !aws.ToBool(page.IsTruncated) {
					break
				}
				next := aws.ToString(page.NextMarker)
				if next == "" || next <= marker {
					t.Fatal("invalid V1 next marker")
				}
				marker = next
			}
		}
		if strings.Join(keys, "|") != "b +|folder/a|folder/b|日本語" {
			t.Fatalf("live keys v2=%t: %v", v2, keys)
		}
	}
	var versions []types.ObjectVersion
	var markers []types.DeleteMarkerEntry
	pager := awss3.NewListObjectVersionsPaginator(client, &awss3.ListObjectVersionsInput{Bucket: bucket, MaxKeys: aws.Int32(1)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, page.Versions...)
		markers = append(markers, page.DeleteMarkers...)
	}
	if len(versions) != 9 || len(markers) != 1 || !aws.ToBool(markers[0].IsLatest) {
		t.Fatalf("history: %d versions %v", len(versions), markers)
	}
	for i := 0; i < 3; i++ {
		if aws.ToString(versions[i].VersionId) != ids[2-i] || aws.ToBool(versions[i].IsLatest) {
			t.Fatalf("version order: %+v", versions[i])
		}
	}
	if !aws.ToBool(versions[3].IsLatest) || aws.ToBool(versions[4].IsLatest) || aws.ToBool(versions[5].IsLatest) {
		t.Fatal("copy/multipart latest flags incorrect")
	}
	grouped, err := client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: bucket, Prefix: aws.String("folder/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
	if err != nil || len(grouped.Versions) != 1 || !aws.ToBool(grouped.IsTruncated) {
		t.Fatalf("prefix: %v %v", grouped, err)
	}
	groups, err := client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: bucket, Delimiter: aws.String("/"), KeyMarker: aws.String("b +"), MaxKeys: aws.Int32(1)})
	if err != nil || len(groups.CommonPrefixes) != 1 || aws.ToString(groups.CommonPrefixes[0].Prefix) != "folder/" || aws.ToString(groups.NextKeyMarker) != "folder/" {
		t.Fatalf("groups: %v %v", groups, err)
	}
	encoded, err := client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: bucket, Prefix: aws.String("b +"), EncodingType: types.EncodingTypeUrl})
	if err != nil || len(encoded.Versions) != 3 || aws.ToString(encoded.Versions[0].Key) != "b%20%2B" {
		t.Fatalf("encoding: %v %v", encoded, err)
	}
}
