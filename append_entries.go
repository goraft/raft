package raft

import (
	"io"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/goraft/raft/protobuf"
)

// The request sent to a server to append entries to the log.
type AppendEntriesRequest struct {
	Term         uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	CommitIndex  uint64
	LeaderName   string
	Entries      []*protobuf.LogEntry
}

// Creates a new AppendEntries request.
func newAppendEntriesRequest(term uint64, prevLogIndex uint64, prevLogTerm uint64,
	commitIndex uint64, leaderName string, entries []*LogEntry) *AppendEntriesRequest {
	pbEntries := make([]*protobuf.LogEntry, len(entries))

	for i := range entries {
		pbEntries[i] = entries[i].pb
	}

	return &AppendEntriesRequest{
		Term:         term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderName:   leaderName,
		Entries:      pbEntries,
	}
}

func (req *AppendEntriesRequest) Marshal() ([]byte, error) {
	pb := &protobuf.AppendEntriesRequest{
		Term:         proto.Uint64(req.Term),
		PrevLogIndex: proto.Uint64(req.PrevLogIndex),
		PrevLogTerm:  proto.Uint64(req.PrevLogTerm),
		CommitIndex:  proto.Uint64(req.CommitIndex),
		LeaderName:   proto.String(req.LeaderName),
		Entries:      req.Entries,
	}

	return proto.Marshal(pb)
}

// Encodes the AppendEntriesRequest to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (req *AppendEntriesRequest) Encode(w io.Writer) (int, error) {
	return encode(req, w)
}

func (req *AppendEntriesRequest) Unmarshal(data []byte) error {

	pb := new(protobuf.AppendEntriesRequest)
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	req.Term = pb.GetTerm()
	req.PrevLogIndex = pb.GetPrevLogIndex()
	req.PrevLogTerm = pb.GetPrevLogTerm()
	req.CommitIndex = pb.GetCommitIndex()
	req.LeaderName = pb.GetLeaderName()
	req.Entries = pb.GetEntries()

	return nil
}

// Decodes the AppendEntriesRequest from a buffer. Returns the number of bytes read and
// any error that occurs.
func (req *AppendEntriesRequest) Decode(r io.Reader) (int, error) {
	return decode(req, r)
}

// The response returned from a server appending entries to the log.
type AppendEntriesResponse struct {
	pb     *protobuf.AppendEntriesResponse
	peer   string
	append bool
}

// Creates a new AppendEntries response.
func newAppendEntriesResponse(term uint64, success bool, index uint64, commitIndex uint64) *AppendEntriesResponse {
	pb := &protobuf.AppendEntriesResponse{
		Term:        proto.Uint64(term),
		Index:       proto.Uint64(index),
		Success:     proto.Bool(success),
		CommitIndex: proto.Uint64(commitIndex),
	}

	return &AppendEntriesResponse{
		pb: pb,
	}
}

func (aer *AppendEntriesResponse) Index() uint64 {
	return aer.pb.GetIndex()
}

func (aer *AppendEntriesResponse) CommitIndex() uint64 {
	return aer.pb.GetCommitIndex()
}

func (aer *AppendEntriesResponse) Term() uint64 {
	return aer.pb.GetTerm()
}

func (aer *AppendEntriesResponse) Success() bool {
	return aer.pb.GetSuccess()
}

func (resp *AppendEntriesResponse) Marshal() ([]byte, error) {
	p, err := proto.Marshal(resp.pb)
	return p, err
}

// Encodes the AppendEntriesResponse to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (resp *AppendEntriesResponse) Encode(w io.Writer) (int, error) {
	return encode(resp, w)
}

func (resp *AppendEntriesResponse) Unmarshal(b []byte) error {
	if resp.pb == nil {
		resp.pb = &protobuf.AppendEntriesResponse{}
	}
	err := resp.pb.Unmarshal(b)
	return err
}

// Decodes the AppendEntriesResponse from a buffer. Returns the number of bytes read and
// any error that occurs.
func (resp *AppendEntriesResponse) Decode(r io.Reader) (int, error) {
	return decode(resp, r)
}
