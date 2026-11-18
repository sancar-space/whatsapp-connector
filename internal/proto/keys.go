package proto

import "fmt"

// Keys produces Redis keys for the connector protocol.
// All keys share a configurable prefix (default "wa:").
type Keys struct {
	Prefix string
}

func (k Keys) key(parts ...string) string {
	s := k.Prefix
	for i, p := range parts {
		if i > 0 {
			s += ":"
		}
		s += p
	}
	return s
}

func (k Keys) CmdStream(sessionID string) string    { return k.key("cmd", sessionID) }
func (k Keys) ControlStream() string                { return k.key("control") }
func (k Keys) EventStream(shard int) string         { return k.key("events", fmt.Sprintf("%d", shard)) }
func (k Keys) ReplyKey(commandID string) string     { return k.key("reply", commandID) }
func (k Keys) InstanceSet() string                  { return k.key("instances") }
func (k Keys) InstanceHash(id string) string        { return k.key("instance", id) }
func (k Keys) MetaHash() string                     { return k.key("meta") }
func (k Keys) DLQ() string                          { return k.key("dlq", "events") }
