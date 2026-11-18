package session

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waevents "go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/sancar-space/whatsapp-connector/internal/events"
	"github.com/sancar-space/whatsapp-connector/internal/media"
	waproto "github.com/sancar-space/whatsapp-connector/internal/proto"
	"github.com/sancar-space/whatsapp-connector/internal/store"
)

// Session manages a single WhatsApp session lifecycle.
type Session struct {
	ID           string
	client       *whatsmeow.Client
	pub          *events.Publisher
	mediaStore   *media.Store
	advertiseURL string
	log          *zap.Logger
	mu           sync.Mutex
}

func New(
	id string,
	container *sqlstore.Container,
	pub *events.Publisher,
	mediaStore *media.Store,
	advertiseURL string,
	log *zap.Logger,
) (*Session, error) {
	device, err := store.GetOrCreateDevice(container)
	if err != nil {
		return nil, fmt.Errorf("session %s: get device: %w", id, err)
	}

	waLogger := waLog.Stdout("whatsmeow:"+id, "WARN", true)
	client := whatsmeow.NewClient(device, waLogger)

	s := &Session{
		ID:           id,
		client:       client,
		pub:          pub,
		mediaStore:   mediaStore,
		advertiseURL: advertiseURL,
		log:          log.With(zap.String("sid", id)),
	}
	client.AddEventHandler(s.handleEvent)
	return s, nil
}

// Connect initiates pairing / reconnect. If the device has no JID yet it starts
// pairing (QR or pairing code). If already paired it just reconnects.
func (s *Session) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client.IsConnected() {
		return nil
	}

	if s.client.Store.ID == nil {
		// Not paired yet — caller must observe session.state events for QR/linked.
		if err := s.client.Connect(); err != nil {
			return fmt.Errorf("connect (pairing): %w", err)
		}
	} else {
		if err := s.client.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
	}
	return nil
}

// PairingQR returns a channel that emits the QR code string for pairing.
// Must be called before Connect for new sessions.
func (s *Session) RequestQR(ctx context.Context) (<-chan string, error) {
	ch, err := s.client.GetQRChannel(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan string, 1)
	go func() {
		for evt := range ch {
			if evt.Event == "code" {
				out <- evt.Code
				close(out)
				return
			}
		}
		close(out)
	}()
	return out, nil
}

// SendText sends a plain-text message.
func (s *Session) SendText(ctx context.Context, to, text string) (string, error) {
	jid, err := types.ParseJID(to)
	if err != nil {
		return "", fmt.Errorf("bad jid %q: %w", to, err)
	}
	msg := &waE2E.Message{
		Conversation: proto.String(text),
	}
	resp, err := s.client.SendMessage(ctx, jid, msg)
	if err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Disconnect cleanly disconnects. The session object can reconnect later.
func (s *Session) Disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client.Disconnect()
}

// IsConnected reports whether the underlying client has an active WS connection.
func (s *Session) IsConnected() bool {
	return s.client.IsConnected()
}

// IsLoggedIn reports whether the session is paired with a WhatsApp account.
func (s *Session) IsLoggedIn() bool {
	return s.client.IsLoggedIn()
}

// JID returns the session's WhatsApp JID, or empty string if not paired.
func (s *Session) JID() string {
	if id := s.client.Store.ID; id != nil {
		return id.String()
	}
	return ""
}

// --- internal event handler ---

func (s *Session) handleEvent(rawEvt interface{}) {
	ctx := context.Background()

	switch evt := rawEvt.(type) {
	case *waevents.QR:
		if len(evt.Codes) > 0 {
			_ = s.pub.Publish(ctx, s.ID, waproto.EventPairingQR, map[string]interface{}{
				"qr": evt.Codes[0],
			})
		}

	case *waevents.PairSuccess:
		_ = s.pub.Publish(ctx, s.ID, waproto.EventSessionState, map[string]interface{}{
			"status": "linked",
			"jid":    evt.ID.String(),
		})

	case *waevents.Connected:
		_ = s.pub.Publish(ctx, s.ID, waproto.EventSessionState, map[string]interface{}{
			"status": "connected",
			"jid":    s.JID(),
		})

	case *waevents.Disconnected:
		_ = s.pub.Publish(ctx, s.ID, waproto.EventSessionState, map[string]interface{}{
			"status": "disconnected",
		})

	case *waevents.LoggedOut:
		_ = s.pub.Publish(ctx, s.ID, waproto.EventSessionState, map[string]interface{}{
			"status": "logged_out",
		})

	case *waevents.Message:
		s.handleMessage(ctx, evt)
	}
}

func (s *Session) handleMessage(ctx context.Context, evt *waevents.Message) {
	payload := buildMessagePayload(evt)
	_ = s.pub.Publish(ctx, s.ID, waproto.EventMessageReceived, payload)
}

func buildMessagePayload(evt *waevents.Message) map[string]interface{} {
	p := map[string]interface{}{
		"id":        evt.Info.ID,
		"from":      evt.Info.Sender.String(),
		"chat":      evt.Info.Chat.String(),
		"timestamp": evt.Info.Timestamp.Unix(),
		"is_from_me": evt.Info.IsFromMe,
	}

	msg := evt.Message
	if msg == nil {
		return p
	}

	if text := msg.GetConversation(); text != "" {
		p["type"] = "text"
		p["text"] = text
	} else if ext := msg.GetExtendedTextMessage(); ext != nil {
		p["type"] = "text"
		p["text"] = ext.GetText()
	} else if img := msg.GetImageMessage(); img != nil {
		p["type"] = "image"
		p["caption"] = img.GetCaption()
		p["mime"] = img.GetMimetype()
	} else if doc := msg.GetDocumentMessage(); doc != nil {
		p["type"] = "document"
		p["filename"] = doc.GetFileName()
		p["mime"] = doc.GetMimetype()
	} else if audio := msg.GetAudioMessage(); audio != nil {
		p["type"] = "audio"
		p["ptt"] = audio.GetPtt()
	} else if vid := msg.GetVideoMessage(); vid != nil {
		p["type"] = "video"
		p["caption"] = vid.GetCaption()
	} else {
		raw, _ := json.Marshal(msg)
		p["type"] = "unknown"
		p["raw"] = string(raw)
	}

	return p
}
