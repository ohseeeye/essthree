package integration_test

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ohseeeye/essthree/s3"
	"github.com/ohseeeye/oci/ocilayout"
)

func server(t *testing.T) *httptest.Server {
	t.Helper()
	r, err := ocilayout.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	h, err := s3.NewHandler(r, s3.Options{DevelopmentMode: true})
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}
func TestGoSDK(t *testing.T) {
	srv := server(t)
	client := awss3.New(awss3.Options{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	ctx := context.Background()
	bucket := aws.String("sdk-test")
	key := aws.String("nested/space + percent%/hello.txt")
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListBuckets(ctx, &awss3.ListBucketsInput{})
	if err != nil || len(listed.Buckets) != 1 {
		t.Fatalf("list: %v %v", listed, err)
	}
	if _, err = client.PutObject(ctx, &awss3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("hello sdk"), Metadata: map[string]string{"owner": "test"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.PutObject(ctx, &awss3.PutObjectInput{Bucket: bucket, Key: aws.String("nested/second.txt"), Body: strings.NewReader("second")}); err != nil {
		t.Fatal(err)
	}
	v1, err := client.ListObjects(ctx, &awss3.ListObjectsInput{Bucket: bucket, Prefix: aws.String("nested/")})
	if err != nil || len(v1.Contents) != 2 {
		t.Fatalf("ListObjects: %v %v", v1, err)
	}
	v2, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: bucket, Prefix: aws.String("nested/")})
	if err != nil || len(v2.Contents) != 2 {
		t.Fatalf("ListObjectsV2: %v %v", v2, err)
	}
	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: bucket, Key: key})
	if err != nil || head.Metadata["owner"] != "test" {
		t.Fatalf("head: %v %v", head, err)
	}
	got, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: bucket, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(got.Body)
	got.Body.Close()
	if err != nil || string(data) != "hello sdk" {
		t.Fatalf("get: %s %v", data, err)
	}
	createdUpload, err := client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("multipart.bin"), Metadata: map[string]string{"kind": "multipart"}})
	if err != nil {
		t.Fatal(err)
	}
	part2, err := client.UploadPart(ctx, &awss3.UploadPartInput{Bucket: bucket, Key: aws.String("multipart.bin"), UploadId: createdUpload.UploadId, PartNumber: aws.Int32(2), Body: strings.NewReader("second")})
	if err != nil {
		t.Fatal(err)
	}
	part1, err := client.UploadPart(ctx, &awss3.UploadPartInput{Bucket: bucket, Key: aws.String("multipart.bin"), UploadId: createdUpload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("first-")})
	if err != nil {
		t.Fatal(err)
	}
	listedParts, err := client.ListParts(ctx, &awss3.ListPartsInput{Bucket: bucket, Key: aws.String("multipart.bin"), UploadId: createdUpload.UploadId})
	if err != nil || len(listedParts.Parts) != 2 {
		t.Fatalf("ListParts: %v %v", listedParts, err)
	}
	_, err = client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket: bucket, Key: aws.String("multipart.bin"), UploadId: createdUpload.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part1.ETag}, {PartNumber: aws.Int32(2), ETag: part2.ETag}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.CopyObject(ctx, &awss3.CopyObjectInput{Bucket: bucket, Key: aws.String("copied.bin"), CopySource: aws.String("sdk-test/multipart.bin")}); err != nil {
		t.Fatal(err)
	}
	copied, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: bucket, Key: aws.String("copied.bin")})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(copied.Body)
	copied.Body.Close()
	if err != nil || string(data) != "first-second" {
		t.Fatalf("copied multipart object: %s %v", data, err)
	}
	if _, err = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: bucket, Key: key}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: bucket, Key: aws.String("nested/second.txt")}); err != nil {
		t.Fatal(err)
	}
	for _, objectKey := range []string{"multipart.bin", "copied.bin"} {
		if _, err = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: bucket, Key: aws.String(objectKey)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
}
func TestMultipartGoSDKPaginationAndReplay(t *testing.T) {
	srv := server(t)
	client := awss3.New(awss3.Options{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	ctx := context.Background()
	bucket, key := aws.String("multipart-sdk"), aws.String("same key")
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
	var ids []*string
	for i := 0; i < 3; i++ {
		out, err := client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, out.UploadId)
	}
	paginator := awss3.NewListMultipartUploadsPaginator(client, &awss3.ListMultipartUploadsInput{Bucket: bucket, MaxUploads: aws.Int32(1)})
	var discovered []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, upload := range page.Uploads {
			discovered = append(discovered, aws.ToString(upload.UploadId))
		}
	}
	if len(discovered) != 3 {
		t.Fatalf("discovered: %v", discovered)
	}
	for i := range ids {
		if discovered[i] != aws.ToString(ids[i]) {
			t.Fatalf("initiation order: %v", discovered)
		}
	}
	var completed []types.CompletedPart
	for _, n := range []int32{1, 3, 5} {
		part, err := client.UploadPart(ctx, &awss3.UploadPartInput{Bucket: bucket, Key: key, UploadId: ids[0], PartNumber: aws.Int32(n), Body: strings.NewReader(fmt.Sprint(n))})
		if err != nil {
			t.Fatal(err)
		}
		completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(n), ETag: part.ETag})
	}
	parts := awss3.NewListPartsPaginator(client, &awss3.ListPartsInput{Bucket: bucket, Key: key, UploadId: ids[0], MaxParts: aws.Int32(1)})
	var numbers []int32
	for parts.HasMorePages() {
		page, err := parts.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range page.Parts {
			numbers = append(numbers, aws.ToInt32(part.PartNumber))
		}
	}
	if fmt.Sprint(numbers) != "[1 3 5]" {
		t.Fatalf("parts: %v", numbers)
	}
	input := &awss3.CompleteMultipartUploadInput{Bucket: bucket, Key: key, UploadId: ids[0], MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}}
	result, err := client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("later")}); err != nil {
		t.Fatal(err)
	}
	replay, err := client.CompleteMultipartUpload(ctx, input)
	if err != nil || aws.ToString(replay.ETag) != aws.ToString(result.ETag) {
		t.Fatalf("replay: %v %v", replay, err)
	}
	got, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: bucket, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(got.Body)
	got.Body.Close()
	if err != nil || string(data) != "later" {
		t.Fatalf("retry changed object: %q %v", data, err)
	}
	for _, uploadID := range ids[1:] {
		if _, err := client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{Bucket: bucket, Key: key, UploadId: uploadID}); err != nil {
			t.Fatal(err)
		}
	}
	active, err := client.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{Bucket: bucket})
	if err != nil || len(active.Uploads) != 0 {
		t.Fatalf("terminated uploads visible: %v %v", active, err)
	}
}

func TestAWSCLI(t *testing.T) {
	if os.Getenv("ESSTHREE_TEST_CLI") != "1" {
		t.Skip("opt in with ESSTHREE_TEST_CLI=1 inside the CLI test container")
	}
	cli := os.Getenv("ESSTHREE_AWS_CLI")
	if cli == "" {
		var err error
		cli, err = exec.LookPath("aws")
		if err != nil {
			t.Skip("AWS CLI not installed; set ESSTHREE_AWS_CLI to enable")
		}
	}
	srv := server(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	output := filepath.Join(dir, "output.txt")
	if err := os.WriteFile(input, []byte("hello cli"), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(cli, append([]string{"--endpoint-url", srv.URL, "--region", "us-east-1", "s3api"}, args...)...)
		cmd.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID=test", "AWS_SECRET_ACCESS_KEY=test", "AWS_SESSION_TOKEN=", "AWS_CONFIG_FILE="+filepath.Join(dir, "config"), "AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(dir, "credentials"), "AWS_EC2_METADATA_DISABLED=true", "AWS_REQUEST_CHECKSUM_CALCULATION=when_required", "AWS_RESPONSE_CHECKSUM_VALIDATION=when_required", "AWS_PAGER=")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("aws %v: %v\n%s", args, err, out)
		}
	}
	run("create-bucket", "--bucket", "cli-test")
	run("list-buckets")
	run("put-object", "--bucket", "cli-test", "--key", "nested/hello.txt", "--body", input)
	run("head-object", "--bucket", "cli-test", "--key", "nested/hello.txt")
	run("get-object", "--bucket", "cli-test", "--key", "nested/hello.txt", output)
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "hello cli" {
		t.Fatalf("download: %s %v", data, err)
	}
	run("delete-object", "--bucket", "cli-test", "--key", "nested/hello.txt")
	run("delete-bucket", "--bucket", "cli-test")
}
