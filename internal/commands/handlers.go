package commands

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	waproto "github.com/sancar-space/whatsapp-connector/internal/proto"
	"github.com/sancar-space/whatsapp-connector/internal/session"
)

// Handler is a function that handles a command and returns (result, error).
type Handler func(ctx context.Context, frame *waproto.CommandFrame, mgr *session.Manager, log *zap.Logger) (interface{}, error)

// Registry maps command type strings to handlers.
var Registry = map[string]Handler{
	"session.connect":    handleSessionConnect,
	"session.disconnect": handleSessionDisconnect,
	"session.status":     handleSessionStatus,
	"message.send":       handleMessageSend,
}

// --- session.connect ---

type connectPayload struct {
	Pairing string `json:"pairing"` // "qr" or "code"
}

func handleSessionConnect(ctx context.Context, frame *waproto.CommandFrame, mgr *session.Manager, log *zap.Logger) (interface{}, error) {
	_, err := mgr.Connect(ctx, frame.SID)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return map[string]string{"status": "connecting"}, nil
}

// --- session.disconnect ---

func handleSessionDisconnect(ctx context.Context, frame *waproto.CommandFrame, mgr *session.Manager, log *zap.Logger) (interface{}, error) {
	if s := mgr.Get(frame.SID); s != nil {
		s.Disconnect()
	}
	return map[string]string{"status": "disconnected"}, nil
}

// --- session.status ---

func handleSessionStatus(ctx context.Context, frame *waproto.CommandFrame, mgr *session.Manager, log *zap.Logger) (interface{}, error) {
	s := mgr.Get(frame.SID)
	if s == nil {
		return map[string]interface{}{
			"connected":  false,
			"logged_in":  false,
			"jid":        "",
		}, nil
	}
	return map[string]interface{}{
		"connected": s.IsConnected(),
		"logged_in": s.IsLoggedIn(),
		"jid":       s.JID(),
	}, nil
}

// --- message.send ---

type sendPayload struct {
	To   string `json:"to"`
	Text string `json:"text"`
}

func handleMessageSend(ctx context.Context, frame *waproto.CommandFrame, mgr *session.Manager, log *zap.Logger) (interface{}, error) {
	var p sendPayload
	if err := json.Unmarshal(frame.Payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	if p.To == "" {
		return nil, fmt.Errorf("missing 'to'")
	}

	s := mgr.Get(frame.SID)
	if s == nil {
		return nil, fmt.Errorf("session %s not found — connect first", frame.SID)
	}
	if !s.IsConnected() {
		return nil, fmt.Errorf("session %s not connected", frame.SID)
	}

	msgID, err := s.SendText(ctx, p.To, p.Text)
	if err != nil {
		return nil, err
	}
	return map[string]string{"id": msgID}, nil
}
