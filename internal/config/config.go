package config

import (
	"os"
	"strconv"
)

type Config struct {
	RedisURL    string
	RedisPrefix string

	EventShards int

	StoragePath string // SQLite databases per session

	AdvertiseURL string // public URL for media blob server
	MediaPort    int    // HTTP port for media server
	MediaToken   string // bearer token for media endpoint

	InstanceID string // unique ID for this process
	Version    string
}

func Load() *Config {
	cfg := &Config{
		RedisURL:    getenv("REDIS_URL", "redis://localhost:6379"),
		RedisPrefix: getenv("WHATSAPP_CONNECTOR_REDIS_PREFIX", "wa:"),
		EventShards: getenvInt("WHATSAPP_CONNECTOR_EVENT_SHARDS", 16),
		StoragePath: getenv("WHATSAPP_CONNECTOR_STORAGE", "/app/data"),
		AdvertiseURL: getenv("WHATSAPP_CONNECTOR_ADVERTISE_URL", ""),
		MediaPort:   getenvInt("WHATSAPP_CONNECTOR_MEDIA_PORT", 8085),
		MediaToken:  getenv("WHATSAPP_CONNECTOR_MEDIA_TOKEN", ""),
		InstanceID:  getenv("WHATSAPP_CONNECTOR_INSTANCE_ID", ""),
		Version:     getenv("WHATSAPP_CONNECTOR_VERSION", "1.0.0-sancar"),
	}
	return cfg
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
