package main

import (
	"context"
	"encoding/json"
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

	// /send — HTTP REST endpoint for outbound messages (n8n → connector → WhatsApp)
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if cfg.MediaToken != "" {
			if r.Header.Get("Authorization") != "Bearer "+cfg.MediaToken {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		var payload struct {
			SID  string `json:"sid"`
			To   string `json:"to"`
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if payload.SID == "" || payload.To == "" || payload.Text == "" {
			http.Error(w, "sid, to and text are required", http.StatusBadRequest)
			return
		}
		s := mgr.Get(payload.SID)
		if s == nil {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		if !s.IsConnected() {
			http.Error(w, "session not connected", http.StatusServiceUnavailable)
			return
		}
		msgID, err := s.SendText(r.Context(), payload.To, payload.Text)
		if err != nil {
			http.Error(w, "send error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": msgID})
	})

	// Register QR endpoint after mgr is ready
	mux.HandleFunc("/qr/", func(w http.ResponseWriter, r *http.Request) {
		sid := strings.TrimPrefix(r.URL.Path, "/qr/")
		if sid == "" {
			http.Error(w, "session id required", http.StatusBadRequest)
			return
		}
		serveQR(w, r, sid, mgr)
	})

	// Status endpoint — returns JSON for polling from the QR page
	mux.HandleFunc("/status/", func(w http.ResponseWriter, r *http.Request) {
		sid := strings.TrimPrefix(r.URL.Path, "/status/")
		if sid == "" {
			http.Error(w, "session id required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		sess := mgr.Get(sid)
		if sess == nil {
			fmt.Fprintf(w, `{"connected":false,"logged_in":false}`)
			return
		}
		fmt.Fprintf(w, `{"connected":%v,"logged_in":%v}`, sess.IsConnected(), sess.IsLoggedIn())
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

const qrPageCSS = `
  :root { --brand: #00A884; --brand-dark: #007a60; --bg: #f0f2f5; --card: #fff; --text: #111; --muted: #667781; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         min-height: 100vh; display: flex; align-items: center; justify-content: center; }
  .card { background: var(--card); border-radius: 16px; box-shadow: 0 2px 16px rgba(0,0,0,.08);
          padding: 40px 48px; max-width: 420px; width: 100%; text-align: center; }
  .logo { font-size: 1.5rem; font-weight: 700; color: var(--brand); margin-bottom: 4px; }
  .logo span { color: var(--text); font-weight: 400; font-size: 1rem; display: block; margin-top: 2px; }
  h2 { font-size: 1.1rem; font-weight: 600; margin: 20px 0 6px; color: var(--text); }
  .sub { font-size: .85rem; color: var(--muted); margin-bottom: 20px; }
  #qr-box { display: inline-block; padding: 12px; background: #fff; border-radius: 12px;
             border: 2px solid #e9ecef; margin: 8px 0; }
  #qr-box canvas, #qr-box img { display: block; }
  .timer-row { display: flex; align-items: center; justify-content: center; gap: 8px;
               margin: 14px 0 0; font-size: .82rem; color: var(--muted); }
  .timer-bar-wrap { width: 140px; height: 4px; background: #e9ecef; border-radius: 4px; overflow: hidden; }
  .timer-bar { height: 100%; background: var(--brand); border-radius: 4px;
               transition: width .9s linear, background .3s; }
  .timer-bar.urgent { background: #f7a13b; }
  .timer-bar.expired { background: #e74c3c; }
  #timer-sec { min-width: 20px; text-align: right; font-variant-numeric: tabular-nums; }
  .status-badge { display: inline-flex; align-items: center; gap: 6px; padding: 6px 14px;
                  border-radius: 999px; font-size: .82rem; font-weight: 500; margin-top: 12px; }
  .badge-ok { background: #e8f8f1; color: #00a884; }
  .badge-wait { background: #fff8e1; color: #b7860b; }
  .badge-spin { width: 10px; height: 10px; border: 2px solid currentColor; border-top-color: transparent;
                border-radius: 50%; animation: spin .7s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .connected-wrap { text-align: center; }
  .connected-wrap .check { font-size: 3rem; }
  .connected-wrap h2 { font-size: 1.3rem; color: var(--brand); margin: 12px 0 6px; }
  .connected-wrap p { color: var(--muted); font-size: .9rem; }
`

const qrPageConnected = `<!DOCTYPE html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stella — Conectado</title><style>%s</style></head>
<body><div class="card connected-wrap">
  <div class="logo">Stella <span>Atendimento Inteligente</span></div>
  <div class="check">✅</div>
  <h2>WhatsApp conectado</h2>
  <p>Sessão <code>%s</code> já está autenticada.</p>
</div></body></html>`

const qrPageWaiting = `<!DOCTYPE html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stella — Aguardando QR</title>
<script>setTimeout(()=>location.reload(),4000)</script>
<style>%s</style></head>
<body><div class="card">
  <div class="logo">Stella <span>Atendimento Inteligente</span></div>
  <h2>⏳ Aguardando QR Code…</h2>
  <p class="sub">Sessão: <code>%s</code></p>
  <div class="status-badge badge-wait"><div class="badge-spin"></div>Conectando ao WhatsApp…</div>
  <p style="margin-top:16px;font-size:.8rem;color:#aaa">Esta página atualiza automaticamente.</p>
</div></body></html>`

const qrPageQR = `<!DOCTYPE html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stella — Conectar WhatsApp</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
<style>%s</style>
</head>
<body>
<div class="card">
  <div class="logo">Stella <span>Atendimento Inteligente</span></div>
  <h2>Escaneie com o WhatsApp</h2>
  <p class="sub">Configurações → Dispositivos conectados → Conectar dispositivo</p>
  <div id="qr-box"><div id="qr"></div></div>
  <div class="timer-row">
    <div class="timer-bar-wrap"><div class="timer-bar" id="bar"></div></div>
    <span>QR expira em <span id="timer-sec">20</span>s</span>
  </div>
  <div id="badge" class="status-badge badge-wait"><div class="badge-spin"></div>Aguardando escaneamento…</div>
</div>
<script>
new QRCode(document.getElementById("qr"), {text: %q, width: 256, height: 256, correctLevel: QRCode.CorrectLevel.M});

var total = 20, left = total, bar = document.getElementById("bar"), sec = document.getElementById("timer-sec");
var tick = setInterval(function(){
  left--;
  sec.textContent = left;
  var pct = (left / total) * 100;
  bar.style.width = pct + "%%";
  bar.className = "timer-bar" + (left <= 5 ? " expired" : left <= 8 ? " urgent" : "");
  if (left <= 0) { clearInterval(tick); location.reload(); }
}, 1000);

(function poll(){
  setTimeout(function(){
    fetch("/status/%s").then(function(r){ return r.json(); }).then(function(d){
      if (d.logged_in) {
        clearInterval(tick);
        document.getElementById("badge").innerHTML = "✅ Conectado! Redirecionando…";
        document.getElementById("badge").className = "status-badge badge-ok";
        setTimeout(function(){ location.reload(); }, 1500);
      } else { poll(); }
    }).catch(function(){ poll(); });
  }, 2000);
})();
</script>
</body></html>`

// serveQR triggers a session connect and renders the QR code as an HTML page.
// Shows a live countdown timer and polls /status/<sid> every 2s for auto-redirect on pair.
func serveQR(w http.ResponseWriter, r *http.Request, sid string, mgr *session.Manager) {
	sess, err := mgr.Connect(r.Context(), sid)
	if err != nil {
		http.Error(w, "connect error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if sess.IsLoggedIn() {
		fmt.Fprintf(w, qrPageConnected, qrPageCSS, sid)
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

	if qr == "" {
		fmt.Fprintf(w, qrPageWaiting, qrPageCSS, sid)
		return
	}

	fmt.Fprintf(w, qrPageQR, qrPageCSS, qr, sid)
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
