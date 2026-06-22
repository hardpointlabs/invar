package mongo

// commands.go — BSON response builders and wire-message envelope constructors.
//
// Wire envelope builders (buildOpReply, buildOpMsgReply) are implemented
// entirely from the spec using the little-endian integer primitives in
// wire.go. No /x/ internal packages are used.
//
// BSON document construction uses bson.Marshal with bson.D, the public
// ordered-document type from go.mongodb.org/mongo-driver/v2/bson.
// Document parsing uses bson.Raw.Elements() / RawElement.Key().

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---------------------------------------------------------------------------
// Wire-message envelope builders
// ---------------------------------------------------------------------------

// buildOpReply wraps responseDoc in an OP_REPLY envelope addressed to
// requestID. Used to respond to OP_QUERY messages.
//
// Wire layout (wire-protocol-messages.md §7):
//
//	Offset  Size   Field
//	0       16     MsgHeader  (opCode = 1)
//	16      4      responseFlags  int32 LE = 0
//	20      8      cursorID       int64 LE = 0
//	28      4      startingFrom   int32 LE = 0
//	32      4      numberReturned int32 LE = 1
//	36      n      document
func buildOpReply(requestID int32, responseDoc []byte) []byte {
	// Reserve space for the messageLength placeholder at index 0, then
	// write requestID=0 (server-assigned; driver does not validate), and
	// responseTo=requestID, opCode=OpReply.
	// wire-protocol-messages.md §2.2 — AppendHeaderStart pattern.
	idx, dst := AppendHeaderStart(nil, 0, requestID, OpReply)

	// responseFlags (4 bytes, int32 LE) = 0
	// wire-protocol-messages.md §7.2
	dst = appendI32(dst, 0)

	// cursorID (8 bytes, int64 LE) = 0  (no open cursor)
	dst = appendI64(dst, 0)

	// startingFrom (4 bytes, int32 LE) = 0
	dst = appendI32(dst, 0)

	// numberReturned (4 bytes, int32 LE) = 1  (one response document)
	dst = appendI32(dst, 1)

	// document
	dst = append(dst, responseDoc...)

	// Back-fill messageLength now that the total size is known.
	dst = FillMessageLength(dst, idx)
	return dst
}

// buildOpMsgReply wraps responseDoc in an OP_MSG envelope addressed to
// requestID. Used to respond to OP_MSG messages.
//
// Wire layout (wire-protocol-messages.md §4):
//
//	Offset  Size   Field
//	0       16     MsgHeader  (opCode = 2013)
//	16      4      flagBits   uint32 LE = 0  (no MoreToCome, no checksum)
//	20      1      sectionType byte   = 0x00 (Kind 0 / SingleDocument)
//	21      n      document
func buildOpMsgReply(requestID int32, responseDoc []byte) []byte {
	idx, dst := AppendHeaderStart(nil, 0, requestID, OpMsg)

	// flagBits (4 bytes, uint32 LE) = 0
	// wire-protocol-messages.md §4.2
	dst = appendU32(dst, 0)

	// sectionType = 0x00 (Kind 0 / SingleDocument)
	// wire-protocol-messages.md §4.3.1
	dst = append(dst, 0x00)

	// document
	dst = append(dst, responseDoc...)

	dst = FillMessageLength(dst, idx)
	return dst
}

// ---------------------------------------------------------------------------
// BSON document builders
// ---------------------------------------------------------------------------

// buildHandshakeDoc returns the minimal server-description document required
// for a successful connection handshake.
//
// Required fields (04-connection-handshake.md §"Recommended minimal standalone
// response"):
//
//	ok                           int32 = 1   — mandatory
//	ismaster                     bool = true — signals standalone/writable
//	minWireVersion               int32 = 8   — driver requires >= 8
//	maxWireVersion               int32 = 25  — driver supports up to 25
//	maxBsonObjectSize            int32 = 16777216
//	maxMessageSizeBytes          int32 = 48000000
//	maxWriteBatchSize            int32 = 100000
//	logicalSessionTimeoutMinutes int32 = 30
//
// Integer fields are sent as int32; the driver parser uses AsInt64OK() which
// accepts both int32 and int64 (04-connection-handshake.md §"BSON Encoding
// Rules for the Response").
func buildHandshakeDoc() ([]byte, error) {
	return bson.Marshal(bson.D{
		{Key: "ok", Value: int32(1)},
		{Key: "ismaster", Value: true},
		{Key: "minWireVersion", Value: int32(8)},
		{Key: "maxWireVersion", Value: int32(25)},
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
		{Key: "maxMessageSizeBytes", Value: int32(48000000)},
		{Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
	})
}

// buildPingDoc returns the response document for a ping command.
// (05-core-command-specifications.md §ping)
func buildPingDoc() ([]byte, error) {
	return bson.Marshal(bson.D{
		{Key: "ok", Value: int32(1)},
	})
}

// buildBuildInfoDoc returns a minimal buildInfo response document.
// The driver treats the response as opaque beyond checking ok and version.
// (05-core-command-specifications.md §buildInfo)
func buildBuildInfoDoc() ([]byte, error) {
	return bson.Marshal(bson.D{
		{Key: "ok", Value: int32(1)},
		{Key: "version", Value: "0.0.0-invar"},
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
	})
}

// buildCommandNotFoundDoc returns an error response for an unrecognised
// command. Code 59 (CommandNotFound) is not retryable.
// (08-behavioural-edge-cases.md §1.1; 06-error-response-structure.md §2.1)
//
// Note: the top-level "code" field must be BSON int32 — the driver reads it
// with Int32OK() only, not AsInt64OK().
// (06-error-response-structure.md §2.1 — "code … must be BSON int32")
func buildCommandNotFoundDoc(cmdName string) ([]byte, error) {
	log.Debug().Str("command", cmdName).Msg("unknown command")
	return bson.Marshal(bson.D{
		{Key: "ok", Value: int32(0)},
		{Key: "errmsg", Value: fmt.Sprintf("no such command: '%s'", cmdName)},
		{Key: "code", Value: int32(59)}, // CommandNotFound
		{Key: "codeName", Value: "CommandNotFound"},
	})
}

// ---------------------------------------------------------------------------
// BSON document helper
// ---------------------------------------------------------------------------

// firstKey returns the key of the first element in a raw BSON document byte
// slice. The first key is the command name in every MongoDB wire protocol
// command document.
// (05-core-command-specifications.md — "The first element of every command
// document is the command name")
func firstKey(doc []byte) (string, error) {
	raw := bson.Raw(doc)
	elems, err := raw.Elements()
	if err != nil {
		return "", fmt.Errorf("parsing BSON document: %w", err)
	}
	if len(elems) == 0 {
		return "", fmt.Errorf("empty BSON document")
	}
	return elems[0].Key(), nil
}
