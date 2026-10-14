# essthree

A small S3-compatible HTTP server for local development and application testing.
It stores objects as OCI artifacts using [`ohseeeye/oci`](https://github.com/ohseeeye/oci),
with `ocisqlite` as the default persistent backend.

The goal is a standalone Go binary that makes common S3 workflows easy to test:
create a bucket, upload and download objects, browse keys, and perform multipart
uploads. It targets one process, one data directory, and one local owner, with a
small number of buckets and objects. The server implements a selected subset of
S3 with configurable SigV4 authentication or an explicit unauthenticated development mode.

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
| `-dev` | `false` | Accepts requests without authenticating credentials or verifying signatures. Cannot be combined with configured credentials. |
| `-listen` | `127.0.0.1:9000` | HTTP listening address. |
| `-data` | `./data` | Directory for SQLite metadata, shared blobs, and in-progress uploads. |
| `-region` | `us-east-1` | Required SigV4 signing region. Bucket location configuration still supports only `us-east-1`. |
| `-require-signed-payload` | `false` | Rejects `UNSIGNED-PAYLOAD`; requires a SHA-256 payload hash. |

Run one server per data directory. The binary holds a directory lock to prevent
another writer from opening the same data directory. Keep this unauthenticated development
server on a trusted local network; loopback is the default.
SQLite storage uses a different on-disk format from the former OCI layout
default. Start with a fresh `-data` directory; the server rejects directories
containing a layout `index.json` rather than silently opening an empty database.

### Enable authentication

Set one access key and secret, then start without `-dev`:

```sh
export ESSTHREE_ACCESS_KEY_ID=local-test
export ESSTHREE_SECRET_ACCESS_KEY='replace-with-your-local-secret'
go run . -data ./data
```

Configure your S3 client with those credentials and the same signing region.
For curl versions supporting `--aws-sigv4`, create a bucket with:

```sh
curl --fail --aws-sigv4 'aws:amz:us-east-1:s3' \
  --user "$ESSTHREE_ACCESS_KEY_ID:$ESSTHREE_SECRET_ACCESS_KEY" \
  -X PUT http://127.0.0.1:9000/example-bucket
```

Startup fails if credentials are missing or if credentials are combined with
`-dev`. The secret is read from the environment, never from a command-line flag
or written to server logs. This is one local owner with access to all buckets;
there are no IAM policies, accounts, or per-bucket permissions.

The middleware checks header-based SigV4 signatures, signing scope, signed S3
headers, and timestamps within 15 minutes of server time. It preserves object
paths without cleaning repeated slashes or `..`. Signed payloads are hashed and
verified before routing or storage changes. Bodies up to 1 MiB stay in memory;
larger bodies are spooled to a temporary file and removed when the request ends.
This adds temporary disk usage and an extra read/write pass for large signed
uploads. The default authentication body limit is 5 GiB.

`UNSIGNED-PAYLOAD` is accepted by default, as allowed by
[S3 SigV4](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html).
It authenticates the request without checking its body hash. Use
`-require-signed-payload` to require body integrity. Presigned URLs, temporary
session credentials, SigV2/SigV4a, and signed chunked payloads are unsupported.
Use HTTPS through a reverse proxy when credentials or object data need transport
protection, and preserve the original signed host, path, query, and headers.

### Run with Docker

From the repository root:

```sh
docker build -t essthree .
docker run --rm -p 127.0.0.1:9000:9000 -v essthree-data:/data essthree
```

The named volume keeps the SQLite database and blobs when the container stops. The same curl commands work
against the published port. The runtime image contains only the binary; SDK and
CLI test tools stay in separate test images.

The image defaults to `-dev`. To enable auth, pass credentials and replace its
default arguments:

```sh
docker run --rm -p 127.0.0.1:9000:9000 -v essthree-data:/data \
  -e ESSTHREE_ACCESS_KEY_ID -e ESSTHREE_SECRET_ACCESS_KEY \
  essthree -listen 0.0.0.0:9000 -data /data
```

To build both `linux/amd64` and `linux/arm64`, install
[Task](https://taskfile.dev/) and run:

```sh
task docker:build
```

This uses the currently selected Docker Buildx builder and
writes both architectures to `dist/essthree.oci.tar`. Set `IMAGE` to choose the
image tag and `OUTPUT` to choose a Buildx exporter. For example, to build and
push both architectures to a registry you control:

```sh
task docker:build IMAGE=ghcr.io/your-account/essthree:latest OUTPUT=type=registry
```

Registry output requires signing in to that registry first.

### Connect an S3 client

Configure the client with:

- Endpoint: `http://127.0.0.1:9000`.
- Path-style addressing: enabled (`/bucket/key`).
- Region: `us-east-1`.
- Credentials: the configured key and secret in authenticated mode; dummy values
  in development mode, which does not verify them.
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
| Header SigV4 verification | Supported | One configured key pair; signed payload verification, configurable region, clock skew, and body limit. `UNSIGNED-PAYLOAD` is configurable. |
| Presigned URLs and session credentials | Not yet supported | Query authentication and temporary security tokens are rejected in authenticated mode. |
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

Pass an `oci.Registry` directly to `s3.NewHandler` with authentication middleware:

```go
auth, err := s3.NewAuthMiddleware(s3.AuthOptions{
    AccessKeyID:     "local-test",
    SecretAccessKey: secret,
    Region:         "us-east-1",
    // RequireSignedPayload: true,
    // MaxClockSkew: 5 * time.Minute,
    // MaxBodySize: 64 << 20,
    // TempDir: "/path/to/temporary/storage",
})
if err != nil {
    return err
}
handler, err := s3.NewHandler(registry, s3.Options{Authentication: auth})
```

`NewAuthMiddleware` returns a reusable `func(http.Handler) http.Handler` wrapper
and can also protect other HTTP handlers. `Options.Authentication` accepts custom
authentication middleware, whose responsibility is to reject unauthorized
requests before calling the next handler. Run authentication before middleware
that transforms the URL, headers, or body. Each authenticated request uses the
existing `local` storage scope; credentials do not select tenants.

For unauthenticated tests, use `s3.Options{DevelopmentMode: true}` instead.
Construction requires exactly one of authentication or development mode.
The handler constructs the OCI-backed storage
internally and works with `http.Server` or `httptest.NewServer`. The caller owns
storage lifetime. The standalone CLI holds a directory lock and closes SQLite
after HTTP shutdown. Bucket catalog and multipart state still use process-local
coordination, so only one server should own a data directory.

## Tests

```sh
task lint
task test
```

`task lint` checks Go formatting and runs `go vet`. `task test` runs uncached
tests with the race detector. Both cover the root and integration modules,
matching the CI checks.

AWS SDK dependencies live in the separate `integration` module.
Tests cover AWS signature fixtures, tampered and expired requests, payload verification
and temporary-file cleanup, authenticated SDK round trips and multipart workflows,
recursive count/byte splits, merging, sorted seek pagination, conditional-write
conflicts, version history and SDK key/version pagination, restart and completion
recovery, and OCI client/server interoperability. CLI execution is explicitly opt-in.

## CI and container releases

GitHub Actions checks formatting with `gofmt`, runs `go vet`, and runs race tests
for both the root and integration modules on pull requests. Pushes to `main`,
including merged PRs, rerun those checks before publishing the Dockerfile as a
multiarchitecture image for `linux/amd64` and `linux/arm64`.

Releases publish to `ohseeeye/essthree` on Docker Hub. Configure these under
**Settings → Secrets and variables → Actions**:

| Kind | Name | Value |
| --- | --- | --- |
| Repository secret | `DOCKERHUB_USERNAME` | Docker Hub login name. |
| Repository secret | `DOCKERHUB_TOKEN` | Docker Hub access token with push access to that repository. |

Successful releases publish only the `ohseeeye/essthree:latest` tag. PR checks do not
use Docker Hub credentials. After configuring credentials, you can also run
**Release** manually from the Actions tab with `main` selected. The release workflow uses Docker's
[Buildx action](https://github.com/docker/setup-buildx-action) on its temporary
GitHub runner; local `task docker:build` uses your selected builder.

To require checks before merging, configure a branch rule for `main` that requires
`Lint`, `Test (.)`, and `Test (integration)`. Workflow files alone do not enforce
branch protection.

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
