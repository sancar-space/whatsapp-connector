package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

// SessionStore manages per-session whatsmeow SQL stores backed by SQLite.
type SessionStore struct {
	dir string
}

func New(dir string) (*SessionStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: create dir %s: %w", dir, err)
	}
	return &SessionStore{dir: dir}, nil
}

// Container returns (or creates) the sqlstore.Container for a given session.
func (s *SessionStore) Container(sessionID string) (*sqlstore.Container, error) {
	dir := filepath.Join(s.dir, sessionID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: create session dir: %w", err)
	}
	dbPath := filepath.Join(dir, "whatsapp.db")
	db, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite %s: %w", dbPath, err)
	}
	container, err := sqlstore.NewWithDB(db, "sqlite3", waLog.Stdout("whatsmeow", "WARN", true))
	if err != nil {
		return nil, fmt.Errorf("store: init sqlstore: %w", err)
	}
	return container, nil
}

// GetOrCreateDevice returns the first device in the store (or creates a new one).
func GetOrCreateDevice(container *sqlstore.Container) (*wastore.Device, error) {
	devices, err := container.GetAllDevices()
	if err != nil {
		return nil, err
	}
	if len(devices) > 0 {
		return devices[0], nil
	}
	return container.NewDevice(), nil
}
