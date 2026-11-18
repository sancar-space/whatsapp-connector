package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

	// --- Event publisher ---
	pub := events.New(rdb, keys, cfg.EventShards, cfg.InstanceID, int(time.Now().Unix()), cfg.WebhookURL, log)

	// --- Session manager ---
	mgr := session.NewManager(cfg.StoragePath, pub, mediaStore, cfg.AdvertiseURL, log)

	// Register QR endpoint after mgr is ready
	mux.HandleFunc("/qr/", func(w http.ResponseWriter, r *http.Request) {
		sid := strings.TrimPrefix(r.URL.Path, "/qr/")
		if sid == "" {
			http.Error(w, "session id required", http.StatusBadRequest)
			return
		}
		serveQR(w, r, sid, mgr)
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

// serveQR triggers a session connect and renders the QR code as an HTML page.
// Auto-refreshes every 5 s until the session is paired.
func serveQR(w http.ResponseWriter, r *http.Request, sid string, mgr *session.Manager) {
	sess, err := mgr.Connect(r.Context(), sid)
	if err != nil {
		http.Error(w, "connect error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if sess.IsLoggedIn() {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!DOCTYPE html><html><body style="font-family:sans-serif;text-align:center;padding:40px">
<h2>✅ WhatsApp conectado</h2><p>Sessão <code>%s</code> já está autenticada.</p></body></html>`, sid)
		return
	}

	// Poll briefly for QR (up to 10 s) to avoid an extra page load on first visit.
	var qr string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		qr = sess.GetLastQR()
		if qr != "" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if qr == "" {
		fmt.Fprintf(w, `<!DOCTYPE html><html><head>
<meta http-equiv="refresh" content="3">
<title>Aguardando QR — %s</title></head>
<body style="font-family:sans-serif;text-align:center;padding:40px">
<h2>⏳ Aguardando QR Code…</h2>
<p>Sessão: <code>%s</code></p>
<p>Esta página atualiza automaticamente a cada 3 segundos.</p>
</body></html>`, sid, sid)
		return
	}

	fmt.Fprintf(w, `<!DOCTYPE html><html><head>
<meta http-equiv="refresh" content="20">
<title>QR Code — %s</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
</head>
<body style="font-family:sans-serif;text-align:center;padding:40px">
<h2>Escaneie com o WhatsApp</h2>
<p>Sessão: <code>%s</code></p>
<div id="qr" style="display:inline-block;margin:20px"></div>
<p style="color:#666">Configurações → Dispositivos conectados → Conectar dispositivo</p>
<p style="color:#999;font-size:0.85em">O QR expira em ~20 s — a página atualiza automaticamente.</p>
<script>
new QRCode(document.getElementById("qr"), {text: %q, width: 300, height: 300});
</script>
</body></html>`, sid, sid, qr)
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
