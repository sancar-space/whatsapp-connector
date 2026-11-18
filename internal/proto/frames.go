package proto

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"
)

const ProtocolVersion = 1
const ProtocolMin = 1

// Event type constants used by the connector.
const (
	EventSessionState    = "session.state"
	EventPairingQR       = "pairing.qr"
	EventPairingCode     = "pairing.code"
	EventMessageReceived = "message.received"
	EventCommandFailed   = "command.failed"
	EventCommandOK       = "command.ok"
)

// CommandFrame is the envelope for commands read from Redis Streams.
// All stream fields are strings; numeric fields were JSON-encoded by the Ruby side
// (e.g. ts → "1234567890123", payload → "{...}").
type CommandFrame struct {
	V              int
	ID             string
	Type           string
	SID            string
	TS             int64
	ReplyTo        string
	Deadline       int64 // ms since epoch; 0 = no deadline
	MaxRuntimeMS   int64 // 0 = no ceiling
	IdempotencyKey string
	Payload        json.RawMessage
}

func (f *CommandFrame) Expired() bool {
	if f.Deadline == 0 {
		return false
	}
	return time.Now().UnixMilli() > f.Deadline
}

// DecodeCommandFields decodes a Redis stream entry map into a CommandFrame.
func DecodeCommandFields(fields map[string]string) (*CommandFrame, error) {
	f := &CommandFrame{}
	for k, v := range fields {
		switch k {
		case "v":
			n, _ := strconv.Atoi(v)
			f.V = n
		case "id":
			f.ID = v
		case "type":
			f.Type = v
		case "sid":
			f.SID = v
		case "ts":
			f.TS, _ = strconv.ParseInt(v, 10, 64)
		case "reply_to":
			f.ReplyTo = v
		case "deadline":
			f.Deadline, _ = strconv.ParseInt(v, 10, 64)
		case "max_runtime_ms":
			f.MaxRuntimeMS, _ = strconv.ParseInt(v, 10, 64)
		case "idempotency_key":
			f.IdempotencyKey = v
		case "payload":
			if v == "" {
				v = "{}"
			}
			f.Payload = json.RawMessage(v)
		}
	}
	if f.Type == "" {
		return nil, fmt.Errorf("command frame missing type")
	}
	if f.V != ProtocolVersion {
		return nil, fmt.Errorf("command frame on protocol %d, this build reads %d..%d", f.V, ProtocolMin, ProtocolVersion)
	}
	return f, nil
}

// EventFrame is the envelope for events we publish to Redis Streams.
type EventFrame struct {
	V       int             `json:"v"`
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	SID     string          `json:"sid"`
	Epoch   int             `json:"epoch"`
	Seq     int             `json:"seq"`
	TS      int64           `json:"ts,omitempty"`
	Inst    string          `json:"inst,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// ToStreamFields converts an EventFrame to the string map Redis XADD expects.
// Non-string values are stored as their JSON representation (integers as "1234", etc.).
func (e *EventFrame) ToStreamFields() map[string]interface{} {
	payload := string(e.Payload)
	if payload == "" {
		payload = "{}"
	}
	fields := map[string]interface{}{
		"v":       strconv.Itoa(e.V),
		"id":      e.ID,
		"type":    e.Type,
		"sid":     e.SID,
		"epoch":   strconv.Itoa(e.Epoch),
		"seq":     strconv.Itoa(e.Seq),
		"payload": payload,
	}
	if e.TS > 0 {
		fields["ts"] = strconv.FormatInt(e.TS, 10)
	}
	if e.Inst != "" {
		fields["inst"] = e.Inst
	}
	return fields
}

// RPCReply is the JSON pushed to the reply list for RPC commands.
type RPCReply struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func OKReply(result interface{}) (string, error) {
	b, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	r := RPCReply{OK: true, Result: b}
	out, err := json.Marshal(r)
	return string(out), err
}

func ErrorReply(code, message string) (string, error) {
	r := RPCReply{OK: false, Error: &RPCError{Code: code, Message: message}}
	out, err := json.Marshal(r)
	return string(out), err
}

// ShardFor returns the event shard index for a session ID.
// Matches the Go connector convention: fnv32(sessionID) % numShards.
func ShardFor(sessionID string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(sessionID))
	return int(h.Sum32()) % numShards
}
