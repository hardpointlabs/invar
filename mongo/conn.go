package mongo

// conn.go — per-connection read/write loop for the MongoDB wire protocol.
//
// Each accepted TCP connection is handled by serveConn. The loop:
//   1. Reads the 4-byte messageLength prefix to frame the incoming message
//      (wire-protocol-messages.md §9.3).
//   2. Enforces size limits before allocating further memory or reading more
//      bytes (wire-protocol-messages.md §1.2, §11).
//   3. Reads the remaining body bytes.
//   4. Delegates to dispatchMessage for opcode-specific parsing and dispatch.
//
// A framing error or an unrecoverable dispatch error closes the connection.
// Command-level errors (ok:0 responses) are returned to the client and do
// not close the connection.

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/rs/zerolog/log"
)

// serveConn handles a single MongoDB client connection until it is closed or
// an unrecoverable framing error is encountered.
func serveConn(c net.Conn) {
	defer c.Close()
	addr := c.RemoteAddr()

	for {
		// ----------------------------------------------------------------
		// Step 1: read the 4-byte little-endian messageLength prefix.
		// The length field counts itself, so the minimum value is 16
		// (the complete header with an empty body).
		// wire-protocol-messages.md §2.1, §9.3
		// ----------------------------------------------------------------
		var lenBuf [4]byte
		if _, err := io.ReadFull(c, lenBuf[:]); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Debug().Err(err).Stringer("addr", addr).
					Msg("mongo: read length prefix failed")
			}
			return
		}

		msgLen := int32(binary.LittleEndian.Uint32(lenBuf[:]))

		// ----------------------------------------------------------------
		// Step 2: enforce size limits.
		//
		// Minimum: msgHeaderSize (16 bytes) — the header itself.
		// Maximum: maxWireMessageSize (16 MiB).
		// wire-protocol-messages.md §1.2, §11;
		// 08-behavioural-edge-cases.md §2.1
		// ----------------------------------------------------------------
		if msgLen < int32(msgHeaderSize) {
			log.Debug().Int32("msgLen", msgLen).Stringer("addr", addr).
				Msg("mongo: rejecting message below header minimum")
			return
		}
		if msgLen > int32(maxWireMessageSize) {
			log.Debug().Int32("msgLen", msgLen).Stringer("addr", addr).
				Msg("mongo: rejecting oversized message")
			return
		}

		// ----------------------------------------------------------------
		// Step 3: read the remaining body (msgLen-4 bytes; the 4-byte
		// length prefix is already consumed).
		// ----------------------------------------------------------------
		body := make([]byte, msgLen-4)
		if _, err := io.ReadFull(c, body); err != nil {
			log.Debug().Err(err).Stringer("addr", addr).
				Msg("mongo: incomplete message body read")
			return
		}

		// Reassemble the full wire message so ParseHeader has all 16 bytes.
		msg := make([]byte, msgLen)
		copy(msg[0:4], lenBuf[:])
		copy(msg[4:], body)

		// ----------------------------------------------------------------
		// Step 4: parse the 16-byte header and dispatch by opcode.
		// wire-protocol-messages.md §2
		// ----------------------------------------------------------------
		hdr, rem, ok := ParseHeader(msg)
		if !ok {
			log.Debug().Stringer("addr", addr).Msg("mongo: malformed header")
			return
		}

		resp, err := dispatchMessage(hdr, rem)
		if err != nil {
			log.Debug().Err(err).Stringer("addr", addr).
				Msg("mongo: dispatch error, closing connection")
			return
		}
		if resp == nil {
			continue // no reply needed (e.g. moreToCome)
		}

		if _, err := c.Write(resp); err != nil {
			log.Debug().Err(err).Stringer("addr", addr).
				Msg("mongo: write response failed")
			return
		}
	}
}

// dispatchMessage routes an incoming wire message to the correct handler
// based on the opcode in hdr. body contains all bytes after the 16-byte
// header.
//
// Returns the response bytes to write, nil if no reply is needed, or an
// error to close the connection.
func dispatchMessage(hdr MsgHeader, body []byte) ([]byte, error) {
	switch hdr.OpCode {
	case OpQuery:
		return handleOpQuery(hdr, body)
	case OpMsg:
		return handleOpMsg(hdr, body)
	default:
		// SPEC-GAP: spec does not define what the server must do when it
		// receives an opcode it does not implement (e.g. OP_UPDATE, OP_INSERT).
		// Closing the connection is the conservative choice.
		// Verify against driver test suite before shipping.
		return nil, fmt.Errorf("unsupported opcode %d", hdr.OpCode)
	}
}

// ---------------------------------------------------------------------------
// OP_QUERY handler
// wire-protocol-messages.md §6
// ---------------------------------------------------------------------------

// handleOpQuery handles an OP_QUERY message (opcode 2004).
//
// In MongoDB 3.6+ OP_QUERY is only used for the initial isMaster/hello
// handshake when the driver does not yet know the server's wire version
// (04-connection-handshake.md — "Phase 1 — OP_QUERY Handshake").
//
// Wire layout of body (everything after the 16-byte MsgHeader):
//
//	[flags:              4 bytes, int32  ]
//	[fullCollectionName: cstring         ]
//	[numberToSkip:       4 bytes, int32  ]
//	[numberToReturn:     4 bytes, int32  ]
//	[query:              BSON document   ]
//	[returnFieldsSelector: BSON doc, opt ]
func handleOpQuery(hdr MsgHeader, body []byte) ([]byte, error) {
	rem := body

	// flags (4 bytes, int32) — wire-protocol-messages.md §6.2
	if len(rem) < 4 {
		return nil, fmt.Errorf("OP_QUERY: truncated flags")
	}
	rem = rem[4:]

	// fullCollectionName — cstring (bytes + 0x00 terminator)
	// wire-protocol-messages.md §12 ("cstring")
	nul := indexByte(rem, 0x00)
	if nul < 0 {
		return nil, fmt.Errorf("OP_QUERY: unterminated fullCollectionName")
	}
	rem = rem[nul+1:]

	// numberToSkip (4 bytes, int32)
	if len(rem) < 4 {
		return nil, fmt.Errorf("OP_QUERY: truncated numberToSkip")
	}
	rem = rem[4:]

	// numberToReturn (4 bytes, int32)
	if len(rem) < 4 {
		return nil, fmt.Errorf("OP_QUERY: truncated numberToReturn")
	}
	rem = rem[4:]

	// query — BSON document.
	// The document's own first 4 bytes are its total length (LE int32),
	// which is also the number of bytes we must consume.
	// bson-document-structure.md §2–3
	queryDoc, err := sliceBSONDoc(rem)
	if err != nil {
		return nil, fmt.Errorf("OP_QUERY: query document: %w", err)
	}

	cmdName, err := firstKey(queryDoc)
	if err != nil {
		return nil, fmt.Errorf("OP_QUERY: malformed query document: %w", err)
	}

	var responseDoc []byte
	switch cmdName {
	case "isMaster", "ismaster", "hello":
		responseDoc, err = buildHandshakeDoc()
	default:
		// SPEC-GAP: only isMaster/hello are expected over OP_QUERY per the
		// spec. All other commands receive CommandNotFound (code 59).
		// 08-behavioural-edge-cases.md §1.1
		responseDoc, err = buildCommandNotFoundDoc(cmdName)
	}
	if err != nil {
		return nil, err
	}

	return buildOpReply(hdr.RequestID, responseDoc), nil
}

// ---------------------------------------------------------------------------
// OP_MSG handler
// wire-protocol-messages.md §4
// ---------------------------------------------------------------------------

// handleOpMsg handles an OP_MSG message (opcode 2013).
//
// Wire layout of body (everything after the 16-byte MsgHeader):
//
//	[flagBits: 4 bytes, uint32]
//	[sections: one or more sections until end of message]
//	[checksum: 4 bytes CRC-32C, present only if ChecksumPresent flag set]
//
// Each section begins with a 1-byte sectionType:
//
//	0x00 — Kind 0 / SingleDocument: [type byte][BSON document]
//	0x01 — Kind 1 / DocumentSequence: [type byte][int32 size][cstring id][BSON docs...]
func handleOpMsg(hdr MsgHeader, body []byte) ([]byte, error) {
	// flagBits (4 bytes, uint32) — wire-protocol-messages.md §4.2
	if len(body) < 4 {
		return nil, fmt.Errorf("OP_MSG: truncated flagBits")
	}
	flags := readU32(body[0:])
	rem := body[4:]

	// ChecksumPresent (bit 0): if set, the last 4 bytes are a CRC-32C
	// checksum. We strip them before parsing sections.
	// wire-protocol-messages.md §4.5
	// SPEC-GAP: spec says checksums are a receiver-side concern and the Go
	// driver never sets ChecksumPresent on outgoing messages. We accept the
	// checksum bytes but do not verify them — validation can be added later.
	checksumPresent := flags&0x01 != 0
	if checksumPresent {
		if len(rem) < 4 {
			return nil, fmt.Errorf("OP_MSG: ChecksumPresent set but body too short")
		}
		rem = rem[:len(rem)-4]
	}

	// Parse sections. There must be exactly one Kind 0 section (the command
	// document). Kind 1 sections carry document sequences for bulk ops.
	// wire-protocol-messages.md §4.3
	var cmdDoc []byte
	for len(rem) > 0 {
		if len(rem) < 1 {
			break
		}
		stype := rem[0]
		rem = rem[1:]

		switch stype {
		case 0x00: // Kind 0 — SingleDocument
			// The section body is exactly one BSON document.
			// wire-protocol-messages.md §4.3.1
			doc, err := sliceBSONDoc(rem)
			if err != nil {
				return nil, fmt.Errorf("OP_MSG: Kind 0 section: %w", err)
			}
			if cmdDoc == nil {
				cmdDoc = doc
			}
			rem = rem[len(doc):]

		case 0x01: // Kind 1 — DocumentSequence
			// Layout: [int32 size][cstring identifier][BSON docs...]
			// size counts from the first byte of size through the last
			// byte of the last document.
			// wire-protocol-messages.md §4.3.2
			if len(rem) < 4 {
				return nil, fmt.Errorf("OP_MSG: Kind 1 section: truncated size")
			}
			secSize := readI32(rem[0:])
			if secSize < 4 || int(secSize) > len(rem) {
				return nil, fmt.Errorf("OP_MSG: Kind 1 section: invalid size %d", secSize)
			}
			// Advance past this entire section.
			rem = rem[secSize:]

		default:
			// SPEC-GAP: unknown section type. Close the connection.
			return nil, fmt.Errorf("OP_MSG: unknown section type 0x%02x", stype)
		}
	}

	if cmdDoc == nil {
		return nil, fmt.Errorf("OP_MSG: no Kind 0 section found")
	}

	cmdName, err := firstKey(cmdDoc)
	if err != nil {
		return nil, fmt.Errorf("OP_MSG: malformed command document: %w", err)
	}

	var responseDoc []byte
	switch cmdName {
	case "isMaster", "ismaster", "hello":
		responseDoc, err = buildHandshakeDoc()
	case "ping":
		responseDoc, err = buildPingDoc()
	case "buildInfo", "buildinfo":
		responseDoc, err = buildBuildInfoDoc()
	default:
		// SPEC-GAP: unimplemented command — return CommandNotFound (code 59).
		// 08-behavioural-edge-cases.md §1.1
		responseDoc, err = buildCommandNotFoundDoc(cmdName)
	}
	if err != nil {
		return nil, err
	}

	return buildOpMsgReply(hdr.RequestID, responseDoc), nil
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

// sliceBSONDoc returns the slice of src that contains exactly one BSON
// document, using the document's own 4-byte LE int32 length prefix to
// determine its extent.
//
// The returned slice is a sub-slice of src; no copy is made.
// bson-document-structure.md §2–3
func sliceBSONDoc(src []byte) ([]byte, error) {
	if len(src) < 5 {
		// Minimum valid BSON document is 5 bytes (length + terminator).
		// bson-document-structure.md §11
		return nil, fmt.Errorf("bson document too short (%d bytes)", len(src))
	}
	docLen := int(readI32(src[0:]))
	if docLen < 5 {
		return nil, fmt.Errorf("bson document length %d < 5", docLen)
	}
	if docLen > len(src) {
		return nil, fmt.Errorf("bson document length %d > available %d", docLen, len(src))
	}
	return src[:docLen], nil
}

// indexByte returns the index of the first occurrence of b in s, or -1.
// Used for cstring parsing (scanning for the 0x00 terminator).
func indexByte(s []byte, b byte) int {
	for i, v := range s {
		if v == b {
			return i
		}
	}
	return -1
}
