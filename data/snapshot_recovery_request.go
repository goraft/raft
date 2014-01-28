package data

import (
	"io"
	"io/ioutil"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/goraft/raft/protobuf"
)

// The request sent to a server to start from the snapshot.
type SnapshotRecoveryRequest struct {
	LeaderName string
	LastIndex  uint64
	LastTerm   uint64
	PeerNames  []string
	PeerConns  []string
	State      []byte
}

// Creates a new Snapshot request.
func NewSnapshotRecoveryRequest(leaderName string, snapshot *Snapshot) *SnapshotRecoveryRequest {
	return &SnapshotRecoveryRequest{
		LeaderName: leaderName,
		LastIndex:  snapshot.LastIndex,
		LastTerm:   snapshot.LastTerm,
		PeerNames:  snapshot.PeerNames,
		PeerConns:  snapshot.PeerConns,
		State:      snapshot.State,
	}
}

// Encodes the SnapshotRecoveryRequest to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (req *SnapshotRecoveryRequest) Encode(w io.Writer) (int, error) {

	protoPeers := make([]*protobuf.SnapshotRecoveryRequest_Peer, len(req.PeerNames))

	for i := range protoPeers {
		protoPeers[i] = &protobuf.SnapshotRecoveryRequest_Peer{
			Name:             proto.String(req.PeerNames[i]),
			ConnectionString: proto.String(req.PeerConns[i]),
		}
	}

	pb := &protobuf.SnapshotRecoveryRequest{
		LeaderName: proto.String(req.LeaderName),
		LastIndex:  proto.Uint64(req.LastIndex),
		LastTerm:   proto.Uint64(req.LastTerm),
		Peers:      protoPeers,
		State:      req.State,
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

// Decodes the SnapshotRecoveryRequest from a buffer. Returns the number of bytes read and
// any error that occurs.
func (req *SnapshotRecoveryRequest) Decode(r io.Reader) (int, error) {
	data, err := ioutil.ReadAll(r)

	if err != nil {
		return 0, err
	}

	totalBytes := len(data)

	pb := &protobuf.SnapshotRecoveryRequest{}
	if err = proto.Unmarshal(data, pb); err != nil {
		return -1, err
	}

	req.LeaderName = pb.GetLeaderName()
	req.LastIndex = pb.GetLastIndex()
	req.LastTerm = pb.GetLastTerm()
	req.State = pb.GetState()

	req.PeerNames = make([]string, len(pb.Peers))
	req.PeerConns = make([]string, len(pb.Peers))

	for i, peer := range pb.Peers {
		req.PeerNames[i] = peer.GetName()
		req.PeerConns[i] = peer.GetConnectionString()
	}

	return totalBytes, nil
}
