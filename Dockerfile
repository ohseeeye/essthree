FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY s3 ./s3
COPY main.go ./main.go
RUN CGO_ENABLED=0 go build -trimpath -o /essthree .

FROM scratch
COPY --from=build /essthree /essthree
EXPOSE 9000
VOLUME /data
ENTRYPOINT ["/essthree"]
CMD ["-dev", "-listen", "0.0.0.0:9000", "-data", "/data"]
