# Build from the repository root. Test tools never enter the runtime image/module.
FROM golang:1.25-bookworm AS tests
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY integration/go.mod integration/go.sum ./integration/
RUN cd integration && go mod download
COPY . .
CMD ["sh", "-c", "go test -race ./... && cd integration && go test -v ./..."]

# Optional CLI profile; the default test image does not install Python or AWS CLI.
FROM tests AS cli-tests
ARG AWSCLI_VERSION=1.42.30
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/awscli \
    && /opt/awscli/bin/pip install --no-cache-dir awscli==${AWSCLI_VERSION}
ENV ESSTHREE_TEST_CLI=1 ESSTHREE_AWS_CLI=/opt/awscli/bin/aws
