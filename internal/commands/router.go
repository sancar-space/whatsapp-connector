package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	waproto "github.com/sancar-space/whatsapp-connector/internal/proto"
	"github.com/sancar-space/whatsapp-connector/internal/session"
)

const consumerGroup = "chatwoot"
const rpcTimeout = 30 * time.Second

// Router reads commands from per-session Redis streams and dispatches them.
type Router struct {
	rdb  *redis.Client
	keys waproto.Keys
	mgr  *session.Manager
	log  *zap.Logger
}

func NewRouter(rdb *redis.Client, keys waproto.Keys, mgr *session.Manager, log *zap.Logger) *Router {
	return &Router{rdb: rdb, keys: keys, mgr: mgr, log: log}
}

// EnsureGroup creates the consumer group for a session's command stream if needed.
func (r *Router) EnsureGroup(ctx context.Context, sessionID string) error {
	stream := r.keys.CmdStream(sessionID)
	err := r.rdb.XGroupCreateMkStream(ctx, stream, consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("xgroupcreate %s: %w", stream, err)
	}
	return nil
}

// ProcessStream reads one batch of commands from a session stream and handles them.
// consumerName identifies this process (e.g. instanceID).
func (r *Router) ProcessStream(ctx context.Context, sessionID, consumerName string) error {
	stream := r.keys.CmdStream(sessionID)

	msgs, err := r.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: consumerName,
		Streams:  []string{stream, ">"},
		Count:    10,
		Block:    2 * time.Second,
	}).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}

	for _, stream := range msgs {
		for _, msg := range stream.Messages {
			r.dispatch(ctx, sessionID, msg)
			r.rdb.XAck(ctx, stream.Stream, consumerGroup, msg.ID)
		}
	}
	return nil
}

func (r *Router) dispatch(ctx context.Context, sessionID string, msg redis.XMessage) {
	fields := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		if s, ok := v.(string); ok {
			fields[k] = s
		}
	}

	frame, err := waproto.DecodeCommandFields(fields)
	if err != nil {
		r.log.Warn("bad command frame", zap.String("stream_id", msg.ID), zap.Error(err))
		return
	}

	if frame.Expired() {
		r.log.Debug("command expired, skipping", zap.String("type", frame.Type), zap.String("sid", frame.SID))
		return
	}

	log := r.log.With(zap.String("cmd", frame.Type), zap.String("sid", frame.SID), zap.String("id", frame.ID))

	handler, ok := Registry[frame.Type]
	if !ok {
		log.Warn("unknown command type")
		r.replyError(ctx, frame, "unknown_command", fmt.Sprintf("unknown command %q", frame.Type))
		return
	}

	cmdCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	result, err := handler(cmdCtx, frame, r.mgr, log)
	if err != nil {
		log.Warn("command failed", zap.Error(err))
		r.replyError(ctx, frame, "handler_error", err.Error())
		return
	}

	if frame.ReplyTo != "" {
		payload, encErr := waproto.OKReply(result)
		if encErr != nil {
			log.Error("encode reply", zap.Error(encErr))
			return
		}
		r.rdb.LPush(ctx, r.keys.ReplyKey(frame.ReplyTo), payload)
		r.rdb.Expire(ctx, r.keys.ReplyKey(frame.ReplyTo), 60*time.Second)
	}
}

func (r *Router) replyError(ctx context.Context, frame *waproto.CommandFrame, code, msg string) {
	if frame.ReplyTo == "" {
		return
	}
	payload, _ := waproto.ErrorReply(code, msg)
	r.rdb.LPush(ctx, r.keys.ReplyKey(frame.ReplyTo), payload)
	r.rdb.Expire(ctx, r.keys.ReplyKey(frame.ReplyTo), 60*time.Second)
}
