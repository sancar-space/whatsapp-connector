package session

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/sancar-space/whatsapp-connector/internal/events"
	"github.com/sancar-space/whatsapp-connector/internal/media"
	"github.com/sancar-space/whatsapp-connector/internal/store"
)

// Manager holds all active sessions for this connector instance.
type Manager struct {
	mu           sync.RWMutex
	sessions     map[string]*Session
	storeDir     string
	pub          *events.Publisher
	mediaStore   *media.Store
	advertiseURL string
	log          *zap.Logger
}

func NewManager(
	storeDir string,
	pub *events.Publisher,
	mediaStore *media.Store,
	advertiseURL string,
	log *zap.Logger,
) *Manager {
	return &Manager{
		sessions:     make(map[string]*Session),
		storeDir:     storeDir,
		pub:          pub,
		mediaStore:   mediaStore,
		advertiseURL: advertiseURL,
		log:          log,
	}
}

// Get returns an existing session or nil.
func (m *Manager) Get(id string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[id]
}

// Adopt creates (or returns) a Session for the given ID, opening its SQLite store.
func (m *Manager) Adopt(id string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.sessions[id]; ok {
		return s, nil
	}

	ss, err := store.New(m.storeDir)
	if err != nil {
		return nil, err
	}
	container, err := ss.Container(id)
	if err != nil {
		return nil, fmt.Errorf("manager: container for %s: %w", id, err)
	}

	sess, err := New(id, container, m.pub, m.mediaStore, m.advertiseURL, m.log)
	if err != nil {
		return nil, err
	}
	m.sessions[id] = sess
	return sess, nil
}

// Connect ensures a session is adopted and connected.
func (m *Manager) Connect(ctx context.Context, id string) (*Session, error) {
	sess, err := m.Adopt(id)
	if err != nil {
		return nil, err
	}
	return sess, sess.Connect(ctx)
}

// DisconnectAll disconnects every active session (for shutdown).
func (m *Manager) DisconnectAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		s.Disconnect()
	}
}
