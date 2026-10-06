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
	flag.Parse()
	if !*dev {
		return errors.New("pass -dev for this prototype; verified authentication is not implemented")
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
	handler, err := s3.NewHandler(registry, s3.Options{DevelopmentMode: true, Logger: slog.Default()})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("starting development S3 server", "address", *addr, "data", *dir, "authentication", "disabled", "backend", "ocisqlite")
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
