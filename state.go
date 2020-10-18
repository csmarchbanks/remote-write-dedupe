package main

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/hashicorp/memberlist"
)

type state struct {
	ActiveMember         string
	HighestSentTimestamp int64
	RequestLastReceived  time.Time

	localVersion uint64
}

func (s state) Invalidates(other memberlist.Broadcast) bool {
	if other, ok := other.(state); ok {
		return other.localVersion < s.localVersion
	}
	return false
}

func (s state) Message() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		panic("unable to encode")
	}
	return buf.Bytes()
}

func (s state) Finished() {
}

func parseState(data []byte) (state, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	s := state{}
	err := dec.Decode(&s)
	return s, err
}
