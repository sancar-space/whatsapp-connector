package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/sancar-space/whatsapp-connector/internal/proto"
)

const ttl = 60 * time.Second
const heartbeatInterval = 20 * time.Second

// Registry keeps the connector's entry alive in Redis.
type Registry struct {
	rdb        *redis.Client
	keys       proto.Keys
	instanceID string
	version    string
	advertiseURL string
	mediaToken string
	log        *zap.Logger
}

func New(
	rdb *redis.Client,
	keys proto.Keys,
	instanceID, version, advertiseURL, mediaToken string,
	log *zap.Logger,
) *Registry {
	return &Registry{
		rdb:          rdb,
		keys:         keys,
		instanceID:   instanceID,
		version:      version,
		advertiseURL: advertiseURL,
		mediaToken:   mediaToken,
		log:          log,
	}
}

// Register writes the instance record and sets its TTL. Called once at startup
// and then periodically by Run.
func (r *Registry) Register(ctx context.Context) error {
	pipe := r.rdb.Pipeline()
	hash := r.keys.InstanceHash(r.instanceID)
	pipe.HSet(ctx, hash,
		"version", r.version,
		"protocol_min", fmt.Sprintf("%d", proto.ProtocolMin),
		"protocol_max", fmt.Sprintf("%d", proto.ProtocolVersion),
		"advertise_url", r.advertiseURL,
		"media_token", r.mediaToken,
	)
	pipe.Expire(ctx, hash, ttl)
	pipe.SAdd(ctx, r.keys.InstanceSet(), r.instanceID)
	_, err := pipe.Exec(ctx)
	return err
}

// Run renews the registration on a ticker until ctx is cancelled.
func (r *Registry) Run(ctx context.Context) {
	if err := r.Register(ctx); err != nil {
		r.log.Error("registry: initial register failed", zap.Error(err))
	}

	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Register(ctx); err != nil {
				r.log.Warn("registry: heartbeat failed", zap.Error(err))
			}
		}
	}
}

// Deregister removes the instance record. Called on graceful shutdown.
func (r *Registry) Deregister(ctx context.Context) {
	r.rdb.SRem(ctx, r.keys.InstanceSet(), r.instanceID)
	r.rdb.Del(ctx, r.keys.InstanceHash(r.instanceID))
}
