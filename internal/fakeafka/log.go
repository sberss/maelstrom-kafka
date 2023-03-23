package fakeafka

import "sync"

// Message is a tuple in the form [<offset>, <value>]
type Message [2]int

type Log struct {
	id                string
	messages          []Message
	messagesMu        sync.RWMutex
	committedOffset   int
	committedOffsetMu sync.RWMutex
}

// NewLog creates a new log.
func NewLog(id string) *Log {
	return &Log{
		id:                id,
		messages:          make([]Message, 0),
		messagesMu:        sync.RWMutex{},
		committedOffset:   0,
		committedOffsetMu: sync.RWMutex{},
	}
}

// append appends a message to the log and returns the offset of the message.
func (l *Log) append(msg int) int {
	l.messagesMu.Lock()
	defer l.messagesMu.Unlock()

	l.messages = append(l.messages, Message{len(l.messages), msg})

	return len(l.messages) - 1
}

// poll returns a slice of messages starting at offset and up to limit.
func (l *Log) poll(offset int, limit int) []Message {
	l.messagesMu.RLock()
	defer l.messagesMu.RUnlock()

	if len(l.messages[offset:]) > limit {
		return l.messages[offset : offset+limit]
	}
	return l.messages[offset:]
}

// commitOffset updates the committed offset for the log.
func (l *Log) commitOffset(offset int) {
	l.committedOffsetMu.Lock()
	defer l.committedOffsetMu.Unlock()

	if offset > l.committedOffset {
		l.committedOffset = offset
	}
}

// getCommittedOffset returns the committed offset for the log.
func (l *Log) getCommittedOffset() int {
	l.committedOffsetMu.RLock()
	defer l.committedOffsetMu.RUnlock()

	return l.committedOffset
}
