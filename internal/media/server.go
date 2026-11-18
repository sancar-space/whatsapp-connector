package media

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
	"time"
)

// Blob holds a downloaded media file in memory.
type Blob struct {
	Data    []byte
	MIME    string
	Name    string
	Expires time.Time
}

// Store is an in-memory blob cache with TTL eviction.
type Store struct {
	mu    sync.Mutex
	blobs map[string]*Blob
}

func NewStore() *Store {
	s := &Store{blobs: make(map[string]*Blob)}
	go s.evict()
	return s
}

// Put stores a blob and returns its token.
func (s *Store) Put(data []byte, mime, name string, ttl time.Duration) string {
	token := randomToken()
	s.mu.Lock()
	s.blobs[token] = &Blob{Data: data, MIME: mime, Name: name, Expires: time.Now().Add(ttl)}
	s.mu.Unlock()
	return token
}

func (s *Store) Get(token string) *Blob {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.blobs[token]
	if b == nil || time.Now().After(b.Expires) {
		delete(s.blobs, token)
		return nil
	}
	return b
}

func (s *Store) evict() {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		now := time.Now()
		for k, v := range s.blobs {
			if now.After(v.Expires) {
				delete(s.blobs, k)
			}
		}
		s.mu.Unlock()
	}
}

// Handler serves blobs at /blob/{token}.
// Requires Authorization: Bearer <bearer> header.
type Handler struct {
	store  *Store
	bearer string
}

func NewHandler(store *Store, bearer string) *Handler {
	return &Handler{store: store, bearer: bearer}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Auth check
	if h.bearer != "" {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+h.bearer {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	token := r.URL.Path[len("/blob/"):]
	if token == "" {
		http.NotFound(w, r)
		return
	}

	blob := h.store.Get(token)
	if blob == nil {
		http.NotFound(w, r)
		return
	}

	if blob.MIME != "" {
		w.Header().Set("Content-Type", blob.MIME)
	}
	if blob.Name != "" {
		w.Header().Set("Content-Disposition", "inline; filename=\""+blob.Name+"\"")
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(blob.Data))
}

func randomToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
