package raft

import (
	"time"
)

type SyncCommand struct {
	Time time.Time `json:"time"`
}

// The name of the Sync command in the log
func (c SyncCommand) CommandName() string {
	return "raft:sync"
}

func (c SyncCommand) Apply(s Server) (interface{}, error) {
	f := s.SyncFunc()

	if f != nil {
		f(c.Time)
	}
	return nil, nil
}
