package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/sancar-space/whatsapp-connector/internal/commands"
	"github.com/sancar-space/whatsapp-connector/internal/config"
	"github.com/sancar-space/whatsapp-connector/internal/events"
	"github.com/sancar-space/whatsapp-connector/internal/media"
	waproto "github.com/sancar-space/whatsapp-connector/internal/proto"
	"github.com/sancar-space/whatsapp-connector/internal/registry"
	"github.com/sancar-space/whatsapp-connector/internal/session"
)

func main() {
	cfg := config.Load()

	log, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	log.Info("whatsapp-connector starting",
		zap.String("version", cfg.Version),
		zap.String("instance", cfg.InstanceID),
	)

	// --- Redis ---
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatal("bad REDIS_URL", zap.Error(err))
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("redis unreachable", zap.Error(err))
	}
	cancel()

	keys := waproto.Keys{Prefix: cfg.RedisPrefix}

	// --- Media server ---
	mediaStore := media.NewStore()
	mediaHandler := media.NewHandler(mediaStore, cfg.MediaToken)
	mux := http.NewServeMux()
	mux.Handle("/blob/", mediaHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MediaPort),
		Handler: mux,
	}
	go func() {
		log.Info("media server listening", zap.Int("port", cfg.MediaPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("media server error", zap.Error(err))
		}
	}()

	// --- Event publisher ---
	pub := events.New(rdb, keys, cfg.EventShards, cfg.InstanceID, int(time.Now().Unix()), log)

	// --- Session manager ---
	mgr := session.NewManager(cfg.StoragePath, pub, mediaStore, cfg.AdvertiseURL, log)

	// --- Command router ---
	router := commands.NewRouter(rdb, keys, mgr, log)

	// --- Instance registry ---
	reg := registry.New(rdb, keys, cfg.InstanceID, cfg.Version, cfg.AdvertiseURL, cfg.MediaToken, log)

	// --- Main context ---
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	// Registry heartbeat
	go reg.Run(mainCtx)

	// Command polling loop — a real connector would subscribe to channel publishes
	// or maintain persistent XREADGROUP consumers per session. Here we poll a
	// well-known "control" stream to discover which sessions need processing.
	go pollControl(mainCtx, rdb, keys, router, cfg.InstanceID, log)

	// Shutdown on signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info("shutting down")
	mainCancel()
	mgr.DisconnectAll()
	reg.Deregister(context.Background())
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
	log.Info("bye")
}

// pollControl subscribes to the control stream where Chatwoot announces new
// sessions, then processes each session's command stream.
func pollControl(ctx context.Context, rdb *redis.Client, keys waproto.Keys, router *commands.Router, consumerName string, log *zap.Logger) {
	// Ensure the control group exists.
	controlStream := keys.ControlStream()
	rdb.XGroupCreateMkStream(ctx, controlStream, "connector", "0")

	knownSessions := map[string]bool{}

	for {
		if ctx.Err() != nil {
			return
		}

		// Read from control stream to find new sessions.
		msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "connector",
			Consumer: consumerName,
			Streams:  []string{controlStream, ">"},
			Count:    100,
			Block:    1 * time.Second,
		}).Result()
		if err != nil && err != redis.Nil {
			log.Warn("control stream read", zap.Error(err))
		}

		for _, stream := range msgs {
			for _, msg := range stream.Messages {
				if sid, ok := msg.Values["sid"].(string); ok && sid != "" {
					if !knownSessions[sid] {
						knownSessions[sid] = true
						router.EnsureGroup(ctx, sid)
						log.Info("discovered session", zap.String("sid", sid))
					}
				}
				rdb.XAck(ctx, controlStream, "connector", msg.ID)
			}
		}

		// Process commands for all known sessions.
		for sid := range knownSessions {
			if ctx.Err() != nil {
				return
			}
			if err := router.ProcessStream(ctx, sid, consumerName); err != nil {
				log.Warn("process stream error", zap.String("sid", sid), zap.Error(err))
			}
		}
	}
}
