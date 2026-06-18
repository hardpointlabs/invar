// Package mongo implements the MongoDB wire protocol listener for Invar.
// It handles connection handshake, command dispatch, and response encoding
// using the BSON binary format and MongoDB wire protocol version 6 (MongoDB
// 3.6+, driver wire version 8–25).
package mongo

// ---------------------------------------------------------------------------
// §3  OpCode registry
// (wire-protocol-messages.md §3; source: mongo-go-driver wiremessage.go)
// ---------------------------------------------------------------------------

// OpCode identifies the wire-protocol message type carried in MsgHeader.
// All values are signed 32-bit integers on the wire (little-endian).
type OpCode int32

const (
	// OpReply is sent by the server in response to an OP_QUERY (legacy).
	OpReply OpCode = 1 // 0x0001

	// OpUpdate is a deprecated client-to-server update (replaced by the
	// "update" command over OP_MSG).
	OpUpdate OpCode = 2001 // 0x07D1

	// OpInsert is a deprecated client-to-server insert (replaced by "insert").
	OpInsert OpCode = 2002 // 0x07D2

	// OpQuery is the legacy client-to-server query/command opcode.
	// In MongoDB 3.6+ it is only used for the initial isMaster handshake
	// when the server wire version is not yet known.
	OpQuery OpCode = 2004 // 0x07D4

	// OpGetMore is the deprecated cursor-fetch opcode.
	OpGetMore OpCode = 2005 // 0x07D5

	// OpDelete is the deprecated document-delete opcode.
	OpDelete OpCode = 2006 // 0x07D6

	// OpKillCursors is the deprecated cursor-cleanup opcode.
	OpKillCursors OpCode = 2007 // 0x07D7

	// OpCompressed wraps another opcode's body with compression.
	OpCompressed OpCode = 2012 // 0x07DC

	// OpMsg is the primary modern opcode (MongoDB 3.6+). All commands and
	// their responses use this opcode after the initial handshake.
	OpMsg OpCode = 2013 // 0x07DD
)

// ---------------------------------------------------------------------------
// §2  MsgHeader — the common 16-byte header
// (wire-protocol-messages.md §2)
// ---------------------------------------------------------------------------

// MsgHeader is the fixed 16-byte header that begins every MongoDB wire
// message. All fields are little-endian signed 32-bit integers.
//
// Wire layout:
//
//	Offset  Size  Field
//	0       4     MessageLength  — total message length, including this header
//	4       4     RequestID      — sender-assigned monotonic request identifier
//	8       4     ResponseTo     — echoes the RequestID of the message being answered
//	12      4     OpCode         — identifies the message type (see OpCode constants)
type MsgHeader struct {
	// MessageLength is the total byte count of the entire wire message,
	// including these 16 header bytes. Minimum valid value: 16.
	MessageLength int32

	// RequestID is a monotonically increasing identifier assigned by the
	// sender. For client requests ResponseTo is 0; servers echo the client
	// RequestID in their replies.
	RequestID int32

	// ResponseTo holds the RequestID of the message this is a reply to.
	// Set to 0 for all client-originated messages.
	ResponseTo int32

	// OpCode identifies the wire message type.
	OpCode OpCode
}

// maxWireMessageSize is the maximum accepted wire message length (16 MiB).
// Derived from the pool-recycle guard in the Go driver's operation.go:
//
//	if c := cap(*wm); c < 16*1024*1024 && c/2 < len(*wm)
//
// (wire-protocol-messages.md §1.2)
const maxWireMessageSize = 16 * 1024 * 1024 // 16,777,216 bytes

// msgHeaderSize is the fixed byte length of MsgHeader on the wire.
const msgHeaderSize = 16

// ParseHeader reads a MsgHeader from the first 16 bytes of src using
// little-endian byte order (wire-protocol-messages.md §2.3). It returns
// the parsed header, the remaining bytes after the header, and whether
// parsing succeeded.
//
// Parsing fails (ok=false) when src contains fewer than 16 bytes; in that
// case rem is the original src slice, unchanged.
func ParseHeader(src []byte) (hdr MsgHeader, rem []byte, ok bool) {
	if len(src) < msgHeaderSize {
		return MsgHeader{}, src, false
	}
	hdr.MessageLength = readI32(src[0:])
	hdr.RequestID = readI32(src[4:])
	hdr.ResponseTo = readI32(src[8:])
	hdr.OpCode = OpCode(readI32(src[12:]))
	return hdr, src[msgHeaderSize:], true
}

// AppendHeader serialises hdr onto dst using little-endian byte order and
// returns the extended slice. The MessageLength field is written as provided;
// callers that do not yet know the final message length should use
// AppendHeaderStart / FillMessageLength instead.
func AppendHeader(dst []byte, hdr MsgHeader) []byte {
	dst = appendI32(dst, hdr.MessageLength)
	dst = appendI32(dst, hdr.RequestID)
	dst = appendI32(dst, hdr.ResponseTo)
	dst = appendI32(dst, int32(hdr.OpCode))
	return dst
}

// AppendHeaderStart writes the 16-byte header onto dst with the
// MessageLength field zeroed (to be filled in later). It returns the byte
// index of the MessageLength field so the caller can back-fill it once the
// message body is complete.
//
// Usage pattern (mirrors AppendHeaderStart in the Go driver's wiremessage
// package; wire-protocol-messages.md §2.2):
//
//	idx, dst := AppendHeaderStart(dst, reqID, responseTo, OpMsg)
//	// ... append body ...
//	dst = FillMessageLength(dst, idx)
func AppendHeaderStart(dst []byte, requestID, responseTo int32, opcode OpCode) (lengthIndex int, out []byte) {
	lengthIndex = len(dst)
	dst = appendI32(dst, 0) // placeholder; filled by FillMessageLength
	dst = appendI32(dst, requestID)
	dst = appendI32(dst, responseTo)
	dst = appendI32(dst, int32(opcode))
	return lengthIndex, dst
}

// FillMessageLength back-fills the MessageLength field at lengthIndex with
// the total byte count of dst[lengthIndex:]. It must be called after the
// entire message body has been appended.
//
// This mirrors bsoncore.UpdateLength used by the Go driver
// (wire-protocol-messages.md §2.2).
func FillMessageLength(dst []byte, lengthIndex int) []byte {
	l := int32(len(dst) - lengthIndex)
	dst[lengthIndex+0] = byte(l)
	dst[lengthIndex+1] = byte(l >> 8)
	dst[lengthIndex+2] = byte(l >> 16)
	dst[lengthIndex+3] = byte(l >> 24)
	return dst
}

// ---------------------------------------------------------------------------
// Little-endian integer primitives
// (wire-protocol-messages.md §1.1; bson-document-structure.md §1)
//
// All multi-byte integers in both the wire protocol and BSON are
// little-endian. These helpers encapsulate that encoding in one place so
// every higher-level function stays free of manual bit-shifting.
// ---------------------------------------------------------------------------

// readI32 reads a little-endian int32 from the first 4 bytes of src.
// The caller is responsible for ensuring len(src) >= 4.
func readI32(src []byte) int32 {
	return int32(src[0]) | int32(src[1])<<8 | int32(src[2])<<16 | int32(src[3])<<24
}

// readU32 reads a little-endian uint32 from the first 4 bytes of src.
// The caller is responsible for ensuring len(src) >= 4.
func readU32(src []byte) uint32 {
	return uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
}

// readI64 reads a little-endian int64 from the first 8 bytes of src.
// The caller is responsible for ensuring len(src) >= 8.
func readI64(src []byte) int64 {
	lo := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
	hi := uint32(src[4]) | uint32(src[5])<<8 | uint32(src[6])<<16 | uint32(src[7])<<24
	return int64(uint64(lo) | uint64(hi)<<32)
}

// appendI32 encodes v as a little-endian int32 and appends it to dst.
func appendI32(dst []byte, v int32) []byte {
	return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

// appendU32 encodes v as a little-endian uint32 and appends it to dst.
func appendU32(dst []byte, v uint32) []byte {
	return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

// appendI64 encodes v as a little-endian int64 and appends it to dst.
func appendI64(dst []byte, v int64) []byte {
	u := uint64(v)
	return append(dst,
		byte(u), byte(u>>8), byte(u>>16), byte(u>>24),
		byte(u>>32), byte(u>>40), byte(u>>48), byte(u>>56))
}
