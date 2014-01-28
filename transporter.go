package raft

import (
	"github.com/goraft/raft/data"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// Transporter is the interface for allowing the host application to transport
// requests to other nodes.
type Transporter interface {
	SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse
	SendAppendEntriesRequest(server Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse
	SendSnapshotRequest(server Server, peer *Peer, req *data.SnapshotRequest) *data.SnapshotResponse
	SendSnapshotRecoveryRequest(server Server, peer *Peer, req *data.SnapshotRecoveryRequest) *data.SnapshotRecoveryResponse
}
