package events

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/sancar-space/whatsapp-connector/internal/proto"
)

const streamMaxLen = 10_000

// Publisher writes events to Redis Streams for Chatwoot's consumer.
type Publisher struct {
	rdb        *redis.Client
	keys       proto.Keys
	numShards  int
	instanceID string
	log        *zap.Logger
	seqs       sync.Map // sessionID → *atomic.Int64
	epoch      int
}

func New(rdb *redis.Client, keys proto.Keys, numShards int, instanceID string, epoch int, log *zap.Logger) *Publisher {
	return &Publisher{
		rdb:        rdb,
		keys:       keys,
		numShards:  numShards,
		instanceID: instanceID,
		log:        log,
		epoch:      epoch,
	}
}

// Publish sends an event to the appropriate shard stream.
func (p *Publisher) Publish(ctx context.Context, sessionID, eventType string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	seq := p.nextSeq(sessionID)
	frame := &proto.EventFrame{
		V:       proto.ProtocolVersion,
		ID:      uuid.New().String(),
		Type:    eventType,
		SID:     sessionID,
		Epoch:   p.epoch,
		Seq:     seq,
		TS:      time.Now().UnixMilli(),
		Inst:    p.instanceID,
		Payload: json.RawMessage(b),
	}

	shard := proto.ShardFor(sessionID, p.numShards)
	stream := p.keys.EventStream(shard)

	_, err = p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: frame.ToStreamFields(),
	}).Result()
	if err != nil {
		p.log.Error("failed to publish event", zap.String("type", eventType), zap.String("sid", sessionID), zap.Error(err))
		return err
	}

	p.log.Debug("published event", zap.String("type", eventType), zap.String("sid", sessionID), zap.Int("shard", shard))
	return nil
}

func (p *Publisher) nextSeq(sessionID string) int {
	v, _ := p.seqs.LoadOrStore(sessionID, new(atomic.Int64))
	counter := v.(*atomic.Int64)
	return int(counter.Add(1))
}
