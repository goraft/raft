package raft

import (
	"testing"
	"time"
)

func TestHeartbeatDuration(t *testing.T) {
	heartbeatInterval := time.Duration(20)
	p := newPeer(nil, "testpeer", "testpeer", heartbeatInterval)
	if p.failedHeartbeats != 0 {
		t.Error("Failed heartbeat counter's default is not 0.")
	}

	if p.heartbeatDuration() != heartbeatInterval {
		t.Errorf("Unexpected heartbeat duration. Expected %d, got %d", heartbeatInterval, p.heartbeatDuration())
	}

	p.backoffHeartbeat()

	if p.failedHeartbeats != 1 {
		t.Errorf("Failed heartbeat counter did not increment. Expected %d, got %d", 1, p.failedHeartbeats)
	}

	firstBackoff := time.Duration(heartbeatInterval * 2)
	if p.heartbeatDuration() != firstBackoff {
		t.Errorf("Unexpected heartbeat duration. Expected %d, got %d", firstBackoff, p.heartbeatDuration())
	}

	p.backoffHeartbeat()
	secondBackoff := time.Duration(heartbeatInterval * 4)
	if p.heartbeatDuration() != secondBackoff {
		t.Errorf("Unexpected heartbeat duration. Expected %d, got %d", secondBackoff, p.heartbeatDuration())
	}

	p.resetHeartbeatInterval()
	if p.heartbeatDuration() != heartbeatInterval {
		t.Errorf("Unexpected heartbeat interval. Expected %d, got %d", heartbeatInterval, p.heartbeatDuration())
	}
}
