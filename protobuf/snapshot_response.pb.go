// Code generated by protoc-gen-go.
// source: snapshot_response.proto
// DO NOT EDIT!

package protobuf

import proto "github.com/golang/protobuf/proto"
import math "math"

// discarding unused import gogoproto "github.com/gogo/protobuf/gogoproto/gogo.pb"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type SnapshotResponse struct {
	Success          *bool  `protobuf:"varint,1,req" json:"Success,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *SnapshotResponse) Reset()         { *m = SnapshotResponse{} }
func (m *SnapshotResponse) String() string { return proto.CompactTextString(m) }
func (*SnapshotResponse) ProtoMessage()    {}

func (m *SnapshotResponse) GetSuccess() bool {
	if m != nil && m.Success != nil {
		return *m.Success
	}
	return false
}

func init() {
}
