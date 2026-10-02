# essthree

A small Go S3 HTTP server for local testing, backed by OCI artifacts in
`ohseeeye/oci/ocilayout`. This is the first working slice, not full S3 compatibility.

## Run

Requires Go 1.25 or newer and a POSIX host (the CLI uses a process lock).

```sh
go run . -dev -data ./data
```

Listens on `127.0.0.1:9000`. `-dev` explicitly disables authentication, including
signature verification. Use path-style requests and region `us-east-1`.
Do not expose this development server to an untrusted network.

## Supported features

| S3 operation or feature | Status | Notes |
| --- | --- | --- |
| CreateBucket, ListBuckets, HeadBucket, DeleteBucket | Supported | DeleteBucket requires no live objects or pending uploads. |
| PutObject, GetObject, HeadObject, DeleteObject | Supported | Supports overwrites, zero-byte objects, content headers, and `x-amz-meta-*` metadata. |
| CreateMultipartUpload, UploadPart, ListParts, CompleteMultipartUpload, AbortMultipartUpload | Supported | Parts may arrive out of order or be replaced. State survives restart; completed objects stream directly from their ordered OCI part blobs. ListParts supports max-parts and part-number-marker, with pages capped at 1,000 parts. |
| CopyObject | Supported | Copies ordinary or multipart objects within or across buckets. Supports `COPY` and `REPLACE` metadata directives. |
| ListObjects | Supported | Prefix, delimiter, marker, max-keys, `encoding-type=url`, and pagination via NextMarker. |
| ListObjectsV2 | Supported | Prefix, delimiter, start-after, max-keys, opaque continuation tokens, and `encoding-type=url`. |
| Content-MD5 and `x-amz-content-sha256` | Supported | The server validates supplied MD5 or ordinary SHA-256 payload hashes. |
| ListMultipartUploads | Supported | Active uploads ordered by key and initiation time, with prefix, delimiter, encoding-type=url, and key/upload-ID marker pagination. Pages are capped at 1,000 entries. |
| UploadPartCopy, ranged GET | Not yet supported | Copy parts and byte ranges remain future work. |
| SigV4 verification and presigned URLs | Not yet supported | Development mode accepts requests without authenticating them. |
| AWS chunked payloads and checksum trailers | Not yet supported | Requests using `aws-chunked` payloads or trailers are rejected. |

Object listings are cached inside the OCI storage adapter. A cache miss or server
restart rebuilds a bucket snapshot by scanning current object tags and manifest
annotations; object PUT, DELETE, CopyObject, and multipart completion invalidate that snapshot, including uncertain completion publication results.
Deleted OCI content is retained for later GC.

Multipart parts are stored as separate OCI blobs. Completion publishes an object
manifest that references the selected parts in order, so completion does not copy
the full payload. This local-testing implementation accepts parts smaller than S3's
production minimum size. CopyObject mounts existing blobs into a destination bucket
repository and does not stream payload bytes through the HTTP handler.
Completion persists a frozen selection of parts and a commit ID before publishing
an object. Startup reconciles interrupted completions before serving requests;
mutations also reconcile uncertain completions before changing a bucket. A matching
completion retry returns the original ETag and result without republishing, even
when the key has since been overwritten or deleted. A retry with different parts
is rejected. Completion records and old blobs are retained indefinitely for now;
bucket deletion/recreation starts a new generation and ends access to old records.
Legacy completed records written before result retention return NoSuchUpload.

The completion XML body is limited to 1 MiB; duplicate multipart query parameters
and malformed or trailing XML are rejected. Listings across pages are not snapshots.
Tests cover publication errors before and after commit, abrupt process exit at each
completion commit step, concurrent parts, and races with abort/completion. These
checks establish process-interruption behavior for the pinned local OCI backend;
power-loss durability and distributed writers remain outside this prototype.

For the current SDK slice, set request checksum calculation and response checksum
validation to `when_required`. See [plan.md](plan.md) for the longer-term support matrix.

```sh
curl -X PUT http://localhost:9000/example-bucket
printf hello | curl -X PUT --data-binary @- http://localhost:9000/example-bucket/hello.txt
curl http://localhost:9000/example-bucket/hello.txt
```

## Dependencies and embedding

The root module directly requires only `github.com/ohseeeye/oci`, pinned to v0.0.2.
The `s3` handler and its backend contract use the standard library. OCI imports are
confined to the OCI adapter files, the root executable, and tests; switching implementations is localized.
Upstream test dependencies can appear in `go.sum` without being runtime imports.

Pass an `oci.Registry` directly to `s3.NewHandler` with
`s3.Options{DevelopmentMode: true}`. The handler constructs the OCI-backed storage
internally and works
with `http.Server` or `httptest.NewServer`. The caller owns storage lifetime and
must ensure only one writer targets a layout; the standalone CLI enforces this
with a directory lock. Separate store instances sharing a directory are unsupported.

## Tests

```sh
go test -race ./...
```

Only standard-library test helpers are used by the root module. AWS SDK dependencies
live in the separate `integration` module and are not required by consumers.
Root race tests and the SDK round-trip test have passed both locally and in the Go-only test container. The application image also builds successfully. CLI execution is explicitly opt-in.

Run all current Go tests in a disposable container, without installing Go SDK or
CLI test dependencies on the host:

```sh
docker build --target tests -f integration/Dockerfile -t essthree-tests .
docker run --rm essthree-tests
```

Optional CLI test container (not yet verified):

```sh
docker build --target cli-tests -f integration/Dockerfile -t essthree-cli-tests .
docker run --rm essthree-cli-tests
```

The CLI image pins AWS CLI v1 as a first smoke-test client; AWS CLI v2 compatibility
is not yet verified. Tests start their own HTTP server and use temporary storage;
no ports, credentials, or host data directories need to be mounted.

To run the application itself in a container:

```sh
docker build -t essthree .
docker run --rm -p 127.0.0.1:9000:9000 -v essthree-data:/data essthree
```

The runtime image contains only the binary, with no SDK, Python, or AWS CLI.
