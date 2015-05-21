package raft

import (
	"encoding/binary"
	"io"
)

// StateMachine is the interface for allowing the host application to save and
// recovery the state machine. This makes it possible to make snapshots
// and compact the log.

type StateMachine interface {
}
type StateMachineBytes interface {
	StateMachine
	Save() ([]byte, error)
	Recovery([]byte) error
}

type StateMachineIo interface {
	StateMachine
	WriteSnapshot(w io.Writer) (int, error)
	RecoverSnapshot(r io.Reader) error
}

type DefaultStateMachine struct {
	b []byte
}

func (d *DefaultStateMachine) Save() ([]byte, error) {
	return d.b, nil
}

func (d *DefaultStateMachine) Recovery(b []byte) error {
	d.b = b
	return nil
}

// wraps a byte state machine
type StateMachineIoWrapper struct {
	s StateMachineBytes
}

func (s *StateMachineIoWrapper) WriteSnapshot(w io.Writer) (int, error) {
	b, err := s.s.Save()
	if err != nil {
		return 0, err
	}
	length := len(b)
	sizeBuff := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(sizeBuff, uint64(length))
	written := 0
	for written < 8 {
		n, err := w.Write(sizeBuff[written:])
		if err != nil {
			return 0, err
		}
		written += n
	}
	written = 0
	for written < length {
		n, err := w.Write(b[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
	}
	return written + 8, nil
}

func (s *StateMachineIoWrapper) RecoverSnapshot(r io.Reader) error {
	sizeBuff := make([]byte, 8, 8)
	nRead := 0
	for nRead < 8 {
		n, err := r.Read(sizeBuff[nRead:])
		if n > 0 {
			nRead += n
		}
		if err != nil {
			return err
		}
	}
	length := int(binary.LittleEndian.Uint64(sizeBuff))
	snapshot := make([]byte, length, length)
	nRead = 0
	for nRead < length {
		n, err := r.Read(snapshot[nRead:])
		if err != nil {
			return err
		}
		nRead += n
	}
	return s.s.Recovery(snapshot)
}
