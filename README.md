# essthree

A small S3-compatible HTTP server for local development and application testing.
It stores objects as OCI artifacts using [`ohseeeye/oci`](https://github.com/ohseeeye/oci),
with `ocisqlite` as the default persistent backend.

The goal is a standalone Go binary that makes common S3 workflows easy to test:
create a bucket, upload and download objects, browse keys, and perform multipart
uploads. It targets one process, one data directory, and one local owner, with a
small number of buckets and objects. The current server implements a selected
subset of S3 and requires an explicit development mode with authentication disabled.

## Quick start

Requires Go 1.25 or newer and a POSIX host, such as Linux or macOS.

```sh
git clone https://github.com/ohseeeye/essthree.git
cd essthree
go run . -dev -data ./data
```

The server listens on `http://127.0.0.1:9000` and creates `./data` if needed.
In another terminal, create a bucket and upload, list, and download an object:

```sh
curl --fail -X PUT http://127.0.0.1:9000/example-bucket
printf hello | curl --fail -X PUT --data-binary @- http://127.0.0.1:9000/example-bucket/hello.txt
curl --fail 'http://127.0.0.1:9000/example-bucket?list-type=2'
curl --fail http://127.0.0.1:9000/example-bucket/hello.txt
```

The listing returns S3 XML; the last command prints `hello`. Stop the server with
Ctrl+C. Restart it with the same data directory to keep your buckets, objects,
and pending multipart uploads. The data directory contains `metadata.db`,
`blobs/`, and `uploads/`; back it up as one unit while the server is stopped.

To build and run a standalone binary:

```sh
go build -o essthree .
./essthree -dev -listen 127.0.0.1:9000 -data ./data
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `-dev` | `false` | Required for the current prototype; accepts requests without authenticating credentials or verifying signatures. |
| `-listen` | `127.0.0.1:9000` | HTTP listening address. |
| `-data` | `./data` | Directory for SQLite metadata, shared blobs, and in-progress uploads. |

Run one server per data directory. The binary holds a directory lock to prevent
another writer from opening the same data directory. Keep this unauthenticated development
server on a trusted local network; loopback is the default.
SQLite storage uses a different on-disk format from the former OCI layout
default. Start with a fresh `-data` directory; the server rejects directories
containing a layout `index.json` rather than silently opening an empty database.

### Run with Docker

From the repository root:

```sh
docker build -t essthree .
docker run --rm -p 127.0.0.1:9000:9000 -v essthree-data:/data essthree
```

The named volume keeps the SQLite database and blobs when the container stops. The same curl commands work
against the published port. The runtime image contains only the binary; SDK and
CLI test tools stay in separate test images.

### Connect an S3 client

Configure the client with:

- Endpoint: `http://127.0.0.1:9000`.
- Path-style addressing: enabled (`/bucket/key`).
- Region: `us-east-1`.
- Credentials: dummy values if your client requires them; development mode does
  not verify them.
- Request checksum calculation and response checksum validation: `when_required`.
  Default optional checksums and S3 streaming body formats are still being implemented.

The AWS SDK for Go v2 is tested with these settings. See
[`integration/interop_test.go`](integration/interop_test.go) for a working client
configuration. AWS CLI tests are opt-in, and CLI v2 compatibility is not yet verified.

## How it works

The HTTP layer translates S3 requests into storage operations. An OCI adapter maps
those operations onto an injected `oci.Registry`; the standalone server supplies
`ocisqlite` for local persistence. This keeps the S3 protocol code separate from
storage and lets an application embed the handler with its own registry.

Object bytes live in content-addressed OCI blobs. Each key has an immutable
manifest containing its metadata and ordered blob references. A bucket has one
mutable `bucket-index` tag pointing to the authoritative root of an ordered OCI
index tree. GET, HEAD, and listings all follow that root, so they agree about which
object generation is current. S3 keys remain opaque names, including slashes and
Unicode; they are ordered lexically without interpreting them as filesystem paths.

Leaf indexes hold object-manifest descriptors with the key, size, ETag, creation
time, modification time, and generation in annotations. Parent descriptors carry
the first full key in each child as its lower boundary. Nodes split near the byte
midpoint when they exceed 2,000 entries or 3 MiB of serialized JSON, leaving room
below the 4 MiB manifest limit. Parents split recursively; deletions merge or
redistribute siblings and collapse unnecessary root levels. Full-key boundaries
allow even a large set of keys sharing one prefix to split.

Writes publish the new object manifest and changed index path by digest, then
replace the root with `If-Match` against its previous digest. A conflict retries
against the current root, preserving other committed changes. Listings seek to
the requested prefix or marker and walk only the required leaves; delimiter
listing skips entire common-prefix ranges. They use descriptor metadata without
enumerating object tags or loading every object manifest. Immutable index nodes
have a cache bounded by 8 MiB of serialized content; operations still resolve the
current root.

Each multipart part is an independent blob, so parts can arrive concurrently and
out of order. Completion publishes a manifest referencing the selected parts in
order, and GET streams those parts as one object. CopyObject uses existing OCI
blobs too. The bucket root commits the object tree and a separate completion
receipt and version trees together, allowing interrupted completion to recover
without replacing a subsequent write or resurrecting a deleted key.

Every bucket retains version history automatically. `GET /bucket?versions`
([ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)) returns keys in lexical order and versions newest first,
including delete markers, version IDs, and `IsLatest`. It supports prefix,
delimiter, URL encoding, max-keys, and key/version-ID markers. PUT, copy, and
multipart completion each add one version; retrying a completed multipart upload
does not add another. Ordinary listings show only live keys.

A third ordered tree stores version descriptors and commits in the same root as
current objects and completion receipts. There is no migration from the initial
prototype format. Missing or corrupt roots fail. Old manifests and blobs are
retained until a future garbage collector is implemented. Plan for disk usage to
grow during repeated tests.

Version retention and listing are always enabled in this development server.
Bucket versioning configuration and version-specific GET, HEAD, and DELETE are
not implemented. DeleteBucket still requires no live keys or pending uploads;
deleting a bucket ends access to its retained history.

The adapter requires a registry that enforces conditional manifest updates and
checks that capability when opening or creating a bucket. The standalone server
continues to require one server per data directory: bucket catalog and upload-state
transitions still use process-local coordination. The tree avoids full object
scans in the adapter. SQLite stores repository membership and manifest references
so digest resolution remains indexed as history grows. Applications can still inject
other compatible `oci.Registry` implementations into the handler.

## Supported features

| S3 operation or feature | Status | Notes |
| --- | --- | --- |
| CreateBucket, ListBuckets, HeadBucket, DeleteBucket | Supported | DeleteBucket requires no live objects or pending uploads. |
| PutObject, GetObject, HeadObject, DeleteObject | Supported | Supports overwrites, zero-byte objects, content headers, and `x-amz-meta-*` metadata. |
| CreateMultipartUpload, UploadPart, ListParts, CompleteMultipartUpload, AbortMultipartUpload | Supported | Parts may arrive out of order or be replaced. State survives restart; completed objects stream directly from their ordered OCI part blobs. ListParts supports max-parts and part-number-marker, with pages capped at 1,000 parts. |
| CopyObject | Supported | Copies ordinary or multipart objects within or across buckets. Supports `COPY` and `REPLACE` metadata directives. |
| ListObjects | Supported | Prefix, delimiter, marker, max-keys, `encoding-type=url`, and pagination via NextMarker. |
| ListObjectVersions | Supported | Retained generations and delete markers, newest first within each key; prefix/delimiter grouping, URL encoding, and key/version-ID marker pagination. |
| ListObjectsV2 | Supported | Prefix, delimiter, start-after, max-keys, opaque continuation tokens, and `encoding-type=url`. |
| Content-MD5 and `x-amz-content-sha256` | Supported | The server validates supplied MD5 or ordinary SHA-256 payload hashes. |
| ListMultipartUploads | Supported | Active uploads ordered by key and initiation time, with prefix, delimiter, encoding-type=url, and key/upload-ID marker pagination. Pages are capped at 1,000 entries. |
| UploadPartCopy, ranged GET | Not yet supported | Copy parts and byte ranges remain future work. |
| SigV4 verification and presigned URLs | Not yet supported | Development mode accepts requests without authenticating them. |
| AWS chunked payloads and checksum trailers | Not yet supported | Requests using `aws-chunked` payloads or trailers are rejected. |

## Multipart recovery and limits

This local-testing implementation accepts multipart parts smaller than S3's
production minimum size. The handler's default object-size limit is 5 GiB,
including the total size of a completed multipart object; embedding applications
can configure it through `s3.Options.MaxObjectSize`.

Completion persists a frozen selection of parts and a commit ID before publishing
an object. Startup reconciles interrupted completions before serving requests;
mutations also reconcile uncertain completions before changing a bucket. A matching
completion retry returns the original ETag and result without republishing, even
when the key has since been overwritten or deleted. A retry with different parts
is rejected. Completion records and old blobs are retained indefinitely for now;
bucket deletion/recreation starts a new generation and ends access to old records.


The completion XML body is limited to 1 MiB; duplicate multipart query parameters
and malformed or trailing XML are rejected. Listings across pages are not snapshots.
Tests cover publication errors before and after commit, abrupt process exit at each
completion commit step, concurrent parts, and races with abort/completion. These
checks establish process-interruption behavior for the pinned SQLite backend;
power-loss durability and distributed writers remain outside this prototype.

## Dependencies and embedding

The root module requires `github.com/ohseeeye/oci` v0.0.5 and the separate
`github.com/ohseeeye/oci/ocisqlite` v0.0.2 module. The SQLite backend uses a
pure-Go driver, so the standalone binary does not require CGo. The `s3` handler
accepts any compatible `oci.Registry`; callers choose and close their own backend.

Pass an `oci.Registry` directly to `s3.NewHandler` with
`s3.Options{DevelopmentMode: true}`. The handler constructs the OCI-backed storage
internally and works with `http.Server` or `httptest.NewServer`. The caller owns
storage lifetime. The standalone CLI holds a directory lock and closes SQLite
after HTTP shutdown. Bucket catalog and multipart state still use process-local
coordination, so only one server should own a data directory.

## Tests

```sh
go test -race ./...
(cd integration && go test -race ./...)
```

AWS SDK dependencies live in the separate `integration` module.
Tests cover recursive count/byte splits, merging, sorted seek pagination, conditional-write conflicts, version history and SDK key/version pagination, restart and completion recovery, and OCI client/server interoperability. CLI execution is explicitly opt-in.

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
