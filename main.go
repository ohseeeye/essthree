package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ohseeeye/essthree/s3"
	"github.com/ohseeeye/oci/ocisqlite"
)

func main() {
	if err := run(); err != nil {
		slog.Error("essthree stopped", "error", err)
		os.Exit(1)
	}
}
func run() (runErr error) {
	addr := flag.String("listen", "127.0.0.1:9000", "HTTP listen address")
	dir := flag.String("data", "./data", "SQLite metadata and blob storage directory")
	dev := flag.Bool("dev", false, "explicitly disable authentication (local testing only)")
	region := flag.String("region", "us-east-1", "SigV4 signing region")
	requireSigned := flag.Bool("require-signed-payload", false, "reject authenticated requests using UNSIGNED-PAYLOAD")
	flag.Parse()
	auth, err := configuredAuth(*dev, *region, *requireSigned)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(*dir, ".essthree.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("data directory is already locked: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if _, err := os.Stat(filepath.Join(*dir, "index.json")); err == nil {
		return fmt.Errorf("data directory %q contains an OCI layout; use a new -data directory for SQLite storage", *dir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect data directory: %w", err)
	}
	registry, err := ocisqlite.New(*dir, nil)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, registry.Close()) }()
	handler, err := s3.NewHandler(registry, s3.Options{DevelopmentMode: *dev, Authentication: auth, Logger: slog.Default()})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	authMode := "sigv4"
	if *dev {
		authMode = "disabled"
	}
	slog.Info("starting S3 server", "address", *addr, "data", *dir, "authentication", authMode, "backend", "ocisqlite")
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		return err
	}
	return nil
}

func configuredAuth(dev bool, region string, requireSigned bool) (s3.Middleware, error) {
	accessKey, secret := os.Getenv("ESSTHREE_ACCESS_KEY_ID"), os.Getenv("ESSTHREE_SECRET_ACCESS_KEY")
	if dev {
		if accessKey != "" || secret != "" || requireSigned {
			return nil, errors.New("-dev cannot be combined with credentials or -require-signed-payload")
		}
		return nil, nil
	}
	if accessKey == "" || secret == "" {
		return nil, errors.New("set ESSTHREE_ACCESS_KEY_ID and ESSTHREE_SECRET_ACCESS_KEY, or pass -dev to disable authentication")
	}
	return s3.NewAuthMiddleware(s3.AuthOptions{AccessKeyID: accessKey, SecretAccessKey: secret, Region: region, RequireSignedPayload: requireSigned})
}
