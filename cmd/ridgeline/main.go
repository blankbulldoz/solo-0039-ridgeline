package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ridgeline/internal/chunk"
	"ridgeline/internal/engine"
	"ridgeline/internal/wire"
)

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// blockFormat selects the chunk format for newly written blocks. Existing
// blocks of every supported format stay readable regardless of this choice.
func blockFormat() (int, error) {
	switch value := os.Getenv("RIDGE_BLOCK_FORMAT"); value {
	case "":
		return chunk.Latest, nil
	case "1":
		return chunk.FormatV1, nil
	case "2":
		return chunk.FormatV2, nil
	default:
		return 0, fmt.Errorf("RIDGE_BLOCK_FORMAT must be 1 or 2, got %q", value)
	}
}

func run() error {
	format, err := blockFormat()
	if err != nil {
		return err
	}
	e, err := engine.Open(env("RIDGE_DB", "var/ridgeline.db"), format)
	if err != nil {
		return err
	}
	defer e.Close()
	log.Printf("ridgeline block write format %d", format)
	listener, err := net.Listen("tcp", env("RIDGE_ADDR", "127.0.0.1:8399"))
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: wire.Handler(e), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 20 * time.Second, WriteTimeout: 60 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10,
	}
	stopping, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	log.Printf("ridgeline listening %s", listener.Addr())
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-stopping.Done():
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := srv.Shutdown(ctx); err != nil {
			srv.Close()
			<-done
			return err
		}
		<-done
		return nil
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
