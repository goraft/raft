package raft

import (
	"bufio"
	"code.google.com/p/gogoprotobuf/proto"
	"encoding/json"
	"fmt"
	"github.com/goraft/raft/protobuf"
	"hash"
	"hash/crc32"
	"io"
	"os"
)

// Snapshot represents an in-memory representation of the current state of the system.
type Snapshot struct {
	LastIndex uint64 `json:"lastIndex"`
	LastTerm  uint64 `json:"lastTerm"`

	// Cluster configuration.
	Peers []*Peer `json:"peers"`
	Path  string  `json:"path"`
}

// The request sent to a server to start from the snapshot.
type SnapshotRecoveryRequestHeader struct {
	LeaderName string
	LastIndex  uint64
	LastTerm   uint64
	Peers      []*Peer
	// bytes of state stream here, see StateMachineIo.WriteSnapshot, StateMachineIo.RecoverSnapshot
}

// The response returned from a server appending entries to the log.
type SnapshotRecoveryResponse struct {
	Term        uint64
	Success     bool
	CommitIndex uint64
}

// The request sent to a server to start from the snapshot.
type SnapshotRequest struct {
	LeaderName string
	LastIndex  uint64
	LastTerm   uint64
}

// The response returned if the follower entered snapshot state
type SnapshotResponse struct {
	Success bool `json:"success"`
}

// Creates a new Snapshot request.

func newSnapshotRecoveryRequestHeader(leaderName string, snapshot *Snapshot) *SnapshotRecoveryRequestHeader {
	return &SnapshotRecoveryRequestHeader{
		LeaderName: leaderName,
		LastIndex:  snapshot.LastIndex,
		LastTerm:   snapshot.LastTerm,
		Peers:      snapshot.Peers,
	}
}

var empty = make([]byte, 0, 0)

func (req *SnapshotRecoveryRequestHeader) Marshal() ([]byte, error) {
	protoPeers := make([]*protobuf.SnapshotRecoveryRequest_Peer, len(req.Peers))
	for i, peer := range req.Peers {
		protoPeers[i] = &protobuf.SnapshotRecoveryRequest_Peer{
			Name:             proto.String(peer.Name),
			ConnectionString: proto.String(peer.ConnectionString),
		}
	}
	pb := &protobuf.SnapshotRecoveryRequest{
		LeaderName: proto.String(req.LeaderName),
		LastIndex:  proto.Uint64(req.LastIndex),
		LastTerm:   proto.Uint64(req.LastTerm),
		Peers:      protoPeers,
		State:      empty,
	}
	return pb.Marshal()
}

// Encodes the SnapshotRecoveryRequest to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (req *SnapshotRecoveryRequestHeader) Encode(w io.Writer) (int, error) {
	return encode(req, w)
}

func (req *SnapshotRecoveryRequestHeader) Unmarshal(data []byte) error {
	pb := &protobuf.SnapshotRecoveryRequest{}
	if err := pb.Unmarshal(data); err != nil {
		return err
	}
	req.LeaderName = pb.GetLeaderName()
	req.LastIndex = pb.GetLastIndex()
	req.LastTerm = pb.GetLastTerm()
	req.Peers = make([]*Peer, len(pb.Peers))
	for i, peer := range pb.Peers {
		req.Peers[i] = &Peer{
			Name:             peer.GetName(),
			ConnectionString: peer.GetConnectionString(),
		}
	}
	return nil
}

// Decodes the SnapshotRecoveryRequest from a buffer. Returns the number of bytes read and
// any error that occurs.
func (req *SnapshotRecoveryRequestHeader) Decode(r io.Reader) (int, error) {
	return decode(req, r)
}

// Creates a new Snapshot response.
func newSnapshotRecoveryResponse(term uint64, success bool, commitIndex uint64) *SnapshotRecoveryResponse {
	return &SnapshotRecoveryResponse{
		Term:        term,
		Success:     success,
		CommitIndex: commitIndex,
	}
}

func (req *SnapshotRecoveryResponse) Marshal() ([]byte, error) {
	pb := &protobuf.SnapshotRecoveryResponse{
		Term:        proto.Uint64(req.Term),
		Success:     proto.Bool(req.Success),
		CommitIndex: proto.Uint64(req.CommitIndex),
	}
	return proto.Marshal(pb)
}

// Encode writes the response to a writer.
// Returns the number of bytes written and any error that occurs.
func (req *SnapshotRecoveryResponse) Encode(w io.Writer) (int, error) {
	return encode(req, w)
}

func (req *SnapshotRecoveryResponse) Unmarshal(data []byte) error {
	pb := &protobuf.SnapshotRecoveryResponse{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	req.Term = pb.GetTerm()
	req.Success = pb.GetSuccess()
	req.CommitIndex = pb.GetCommitIndex()
	return nil
}

// Decodes the SnapshotRecoveryResponse from a buffer.
func (req *SnapshotRecoveryResponse) Decode(r io.Reader) (int, error) {
	return decode(req, r)
}

// Creates a new Snapshot request.
func newSnapshotRequest(leaderName string, snapshot *Snapshot) *SnapshotRequest {
	return &SnapshotRequest{
		LeaderName: leaderName,
		LastIndex:  snapshot.LastIndex,
		LastTerm:   snapshot.LastTerm,
	}
}

func (req *SnapshotRequest) Marshal() ([]byte, error) {
	pb := &protobuf.SnapshotRequest{
		LeaderName: proto.String(req.LeaderName),
		LastIndex:  proto.Uint64(req.LastIndex),
		LastTerm:   proto.Uint64(req.LastTerm),
	}
	return proto.Marshal(pb)
}

// Encodes the SnapshotRequest to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (req *SnapshotRequest) Encode(w io.Writer) (int, error) {
	return encode(req, w)
}

func (req *SnapshotRequest) Unmarshal(data []byte) error {
	pb := &protobuf.SnapshotRequest{}

	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	req.LeaderName = pb.GetLeaderName()
	req.LastIndex = pb.GetLastIndex()
	req.LastTerm = pb.GetLastTerm()
	return nil
}

// Decodes the SnapshotRequest from a buffer. Returns the number of bytes read and
// any error that occurs.
func (req *SnapshotRequest) Decode(r io.Reader) (int, error) {
	return decode(req, r)

}

// Creates a new Snapshot response.
func newSnapshotResponse(success bool) *SnapshotResponse {
	return &SnapshotResponse{
		Success: success,
	}
}

func (resp *SnapshotResponse) Marshal() ([]byte, error) {
	pb := &protobuf.SnapshotResponse{
		Success: proto.Bool(resp.Success),
	}
	return proto.Marshal(pb)
}

// Encodes the SnapshotResponse to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (resp *SnapshotResponse) Encode(w io.Writer) (int, error) {
	return encode(resp, w)
}

func (resp *SnapshotResponse) Unmarshal(data []byte) error {
	pb := &protobuf.SnapshotResponse{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}
	resp.Success = pb.GetSuccess()
	return nil
}

// Decodes the SnapshotResponse from a buffer. Returns the number of bytes read and
// any error that occurs.
func (resp *SnapshotResponse) Decode(r io.Reader) (int, error) {
	return decode(resp, r)
}

// returns the snapshot metadata and a readcloser of snapshot state
func readSnapState(path string) (*Snapshot, io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	in := bufio.NewReaderSize(f, 64*1024)
	var checksum uint32
	var headerLen int
	_, err = fmt.Fscanf(in, "%08x\n%08x\n", &checksum, &headerLen)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	header := make([]byte, headerLen, headerLen)
	nRead := 0
	for nRead < headerLen {
		n, err := in.Read(header[nRead:])
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		nRead += n
	}
	snap := &Snapshot{}
	err = json.Unmarshal(header, snap)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return snap, &sumReader{f, in, crc32.NewIEEE(), checksum}, nil
}

// pulls from the reader and adds up a sum, throws error at EOF or Close if sum not expected
type sumReader struct {
	f        *os.File
	in       io.Reader
	sum      hash.Hash32
	expected uint32
}

func (s *sumReader) Read(b []byte) (n int, err error) {
	n, err = s.in.Read(b)
	if n > 0 {
		s.sum.Write(b[:n])
	}
	if err == io.EOF {
		if s.sum.Sum32() != s.expected {
			return n, fmt.Errorf("Bad checksum! Got %d expected %d", s.sum.Sum32(), s.expected)
		}
	}
	return n, err
}

func (s *sumReader) Close() (err error) {
	err = s.f.Close()
	if s.sum.Sum32() != s.expected {
		return fmt.Errorf("Bad checksum! Got %d expected %d", s.sum.Sum32(), s.expected)
	}
	return err
}

// returns a writer which can be used to save a snapshot.
// upon close(), this snapshot has been fsync'd with a crc32 checksum in the first bytes of the file
func (ss *Snapshot) writeState() (io.WriteCloser, error) {
	// Open the file for writing.
	file, err := os.OpenFile(ss.Path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	// serialize header json
	b, err := json.Marshal(ss)
	if err != nil {
		return nil, err
	}
	// sum&size, then header bytes
	// checksum initially zero, we overwrite later
	bufOut := bufio.NewWriterSize(file, 64*1024)
	_, err = fmt.Fprintf(bufOut, "%08x\n%08x\n", 0, len(b))
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("Error writing header: %s", err)
	}
	if _, err := bufOut.Write(b); err != nil {
		file.Close()
		return nil, fmt.Errorf("Error writing header: %s", err)
	}
	// state streams after the checksum and json header
	return &sumWriter{
		file,
		bufOut,
		crc32.NewIEEE(),
	}, nil
}

type sumWriter struct {
	f      *os.File
	bufout *bufio.Writer // buffered writer
	sum    hash.Hash32
}

func (s *sumWriter) Write(b []byte) (n int, err error) {
	n, err = s.bufout.Write(b)
	if n > 0 {
		s.sum.Write(b[:n])
	}
	return n, err
}

func (s *sumWriter) Close() (err error) {
	// flush rest of data from bufout
	if err = s.bufout.Flush(); err != nil {
		return err
	}
	// seek to beginning to overwrite checksum
	if _, err = s.f.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	// write sum
	if _, err = fmt.Fprintf(s.f, "%08x\n", s.sum.Sum32()); err != nil {
		return err
	}
	// fsync
	if err = s.f.Sync(); err != nil {
		return fmt.Errorf("Error fsyncing snapshot: %s", err)
	}
	return s.f.Close()
}

// remove deletes the snapshot file.
func (ss *Snapshot) remove() error {
	if err := os.Remove(ss.Path); err != nil {
		return err
	}
	return nil
}
