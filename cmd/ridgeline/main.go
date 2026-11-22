package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ridgeline/internal/engine"
	"ridgeline/internal/wire"
)

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func run() error {
	dbPath := env("RIDGE_DB", "var/ridgeline.db")
	e, err := engine.Open(dbPath)
	if err != nil {
		return err
	}
	defer e.Close()
	if e.DB.Migrated {
		log.Printf("storage migrated V1 -> V2: namespaces=%d series=%d empty_series=%d head=%d blocks=%d tombstones=%d tomb_targets=%d rollups=%d orphan_blocks=%d",
			e.DB.Migration.Namespaces, e.DB.Migration.Series, e.DB.Migration.EmptySeries,
			e.DB.Migration.HeadPoints, e.DB.Migration.Blocks, e.DB.Migration.Tombstones,
			e.DB.Migration.TombTargets, e.DB.Migration.Rollups, e.DB.Migration.OrphanBlocks)
		log.Printf("pre-migration V1 mirror retained at %s", e.DB.MirrorPath)
	}
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
