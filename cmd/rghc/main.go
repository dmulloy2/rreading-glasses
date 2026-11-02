// Package main runs a metadata server using Hardcover as an upstream.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/alecthomas/kong"
	"github.com/blampe/rreading-glasses/cmd"
	"github.com/blampe/rreading-glasses/internal"
	"github.com/go-chi/chi/v5/middleware"
)

// cli contains our command-line flags.
type cli struct {
	Serve server `cmd:"" help:"Run an HTTP server."`

	Bust cmd.Bust `cmd:"" help:"Bust cache entries."`
}

type server struct {
	cmd.PGConfig
	cmd.LogConfig
	cmd.CloudflareConfig

	Port     int    `default:"8788" env:"PORT" help:"Port to serve traffic on."`
	Proxy    string `default:"" env:"PROXY" help:"HTTP proxy URL to use for upstream requests."`
	Upstream string `default:"api.hardcover.app" env:"UPSTREAM" help:"Upstream host (e.g. www.example.com)."`

	RetryAttempts int    `default:"5" env:"RETRY_ATTEMPTS" help:"Attempts for upstream requests that fail with 429, 502, 503 or 504, including the first (1 disables retries)."`
	BatchSize     int    `default:"10" env:"HARDCOVER_BATCH_SIZE" help:"Most queries sent to Hardcover in one request. Hardcover rejects a request with more top-level fields than the account's burst allowance (10 on the Free tier)."`
	RatePerMinute int    `default:"60" env:"HARDCOVER_RATE_PER_MINUTE" help:"Sustained queries per minute sent to Hardcover (60 on the Free tier), with bursts of up to one batch. 0 disables pacing."`
	IgnoreSeries  string `default:"" env:"IGNORE_SERIES" help:"Case-insensitive regular expression; works in a series whose title matches are left out of author lists (e.g. 'split[- ]volumes?' for translations published in volumes)."`
	InferLanguage bool   `default:"false" env:"INFER_LANGUAGE" help:"Label blank-language editions with a non-English language code when the title clearly isn't English, so downstream language filters can drop them."`

	HardcoverAuth     string `required:"" env:"HARDCOVER_AUTH" xor:"hardcover-auth" help:"Hardcover Authorization header, e.g. 'Bearer ...'"`
	HardcoverAuthFile []byte `required:"" type:"filecontent" xor:"hardcover-auth" env:"HARDCOVER_AUTH_FILE" help:"File containing the Hardcover Authorization header, e.g. 'Bearer ...'"`
}

func (s *server) Run() error {
	_ = s.LogConfig.Run()
	internal.SetInferLanguage(s.InferLanguage)
	if err := internal.SetIgnoreSeries(s.IgnoreSeries); err != nil {
		return err
	}
	reg := internal.NewMetrics()

	cf, err := s.Cache(reg)
	if err != nil {
		return fmt.Errorf("setting up cloudflare: %w", err)
	}

	ctx := context.Background()
	cache, err := internal.NewCache(ctx, s.DSN(), cf, reg)
	if err != nil {
		return fmt.Errorf("setting up cache: %w", err)
	}

	if len(s.HardcoverAuthFile) > 0 {
		s.HardcoverAuth = string(bytes.TrimSpace(s.HardcoverAuthFile))
	}

	hcTransport := internal.ScopedTransport{
		Host: s.Upstream,
		RoundTripper: &internal.HeaderTransport{
			Key:          "Authorization",
			Value:        s.HardcoverAuth,
			RoundTripper: &internal.RetryTransport{Attempts: s.RetryAttempts, RoundTripper: http.DefaultTransport},
		},
	}

	hcClient := &http.Client{Transport: hcTransport}

	if s.BatchSize < 1 {
		return fmt.Errorf("batch size must be at least 1, got %d", s.BatchSize)
	}

	gql, err := internal.NewBatchedGraphQLClient("https://api.hardcover.app/v1/graphql", hcClient, time.Second, s.BatchSize, reg, internal.WithRateLimit(s.RatePerMinute))
	if err != nil {
		return err
	}

	getter, err := internal.NewHardcoverGetter(cache, gql)
	if err != nil {
		return err
	}

	persister, err := internal.NewPersister(ctx, cache, s.DSN())
	if err != nil {
		return err
	}

	ctrl, err := internal.NewController(cache, getter, persister, reg)
	if err != nil {
		return err
	}
	h := internal.NewHandler(ctrl)
	mux := internal.NewMux(h, reg)

	mux = middleware.RequestSize(1024)(mux)  // Limit request bodies.
	mux = internal.Requestlogger{}.Wrap(mux) // Log requests.
	mux = middleware.RequestID(mux)          // Include a request ID header.
	mux = middleware.Recoverer(mux)          // Recover from panics.

	// TODO: The client doesn't send Accept-Encoding and doesn't handle
	// Content-Encoding responses. This would allow us to send compressed bytes
	// directly from the cache.

	addr := fmt.Sprintf(":%d", s.Port)
	server := &http.Server{
		Handler:  mux,
		Addr:     addr,
		ErrorLog: slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
	}

	go func() {
		slog.Info("listening on " + addr)
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			internal.Log(ctx).Error(err.Error())
			os.Exit(1)
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)

	go func() {
		<-shutdown
		os.Exit(0)
		// slog.Info("shutting down http server")
		// _ = server.Shutdown(ctx)
		// slog.Info("waiting for denormalization to finish")
		// ctrl.Shutdown(ctx)
	}()

	ctrl.Run(ctx)

	slog.Info("au revoir!")

	return nil
}

func main() {
	kctx := kong.Parse(&cli{})
	err := kctx.Run()
	if err != nil {
		internal.Log(context.Background()).Error("fatal", "err", err)
		os.Exit(1)
	}
}
