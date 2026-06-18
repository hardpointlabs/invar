package mongo

import (
	"bytes"
	"testing"
)

// ---------------------------------------------------------------------------
// MsgHeader round-trip and encoding tests
// ---------------------------------------------------------------------------

// TestParseHeader_TooShort verifies that ParseHeader returns ok=false when
// the input slice is shorter than the mandatory 16 bytes.
func TestParseHeader_TooShort(t *testing.T) {
	for _, n := range []int{0, 1, 15} {
		in := make([]byte, n)
		_, rem, ok := ParseHeader(in)
		if ok {
			t.Errorf("len=%d: expected ok=false, got true", n)
		}
		if !bytes.Equal(rem, in) {
			t.Errorf("len=%d: rem should equal original slice when parsing fails", n)
		}
	}
}

// TestParseHeader_Exact16 verifies that a 16-byte input is consumed fully.
func TestParseHeader_Exact16(t *testing.T) {
	// Wire-protocol-messages.md §2.4 test vector:
	// reqid=2, responseTo=1, opcode=OpMsg (2013 = 0x07DD)
	raw := []byte{
		0x00, 0x00, 0x00, 0x00, // messageLength placeholder
		0x02, 0x00, 0x00, 0x00, // requestID = 2
		0x01, 0x00, 0x00, 0x00, // responseTo = 1
		0xDD, 0x07, 0x00, 0x00, // opCode = 2013
	}
	hdr, rem, ok := ParseHeader(raw)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(rem) != 0 {
		t.Errorf("expected empty rem, got %d bytes", len(rem))
	}
	if hdr.MessageLength != 0 {
		t.Errorf("MessageLength: want 0, got %d", hdr.MessageLength)
	}
	if hdr.RequestID != 2 {
		t.Errorf("RequestID: want 2, got %d", hdr.RequestID)
	}
	if hdr.ResponseTo != 1 {
		t.Errorf("ResponseTo: want 1, got %d", hdr.ResponseTo)
	}
	if hdr.OpCode != OpMsg {
		t.Errorf("OpCode: want OpMsg (%d), got %d", OpMsg, hdr.OpCode)
	}
}

// TestParseHeader_WithBody verifies that rem contains the bytes after the
// 16-byte header.
func TestParseHeader_WithBody(t *testing.T) {
	body := []byte{0xAA, 0xBB, 0xCC}
	raw := make([]byte, 0, 19)
	raw = appendI32(raw, 19)             // messageLength = 19
	raw = appendI32(raw, 42)             // requestID = 42
	raw = appendI32(raw, 0)              // responseTo = 0
	raw = appendI32(raw, int32(OpQuery)) // opCode = OpQuery
	raw = append(raw, body...)

	hdr, rem, ok := ParseHeader(raw)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if hdr.MessageLength != 19 {
		t.Errorf("MessageLength: want 19, got %d", hdr.MessageLength)
	}
	if hdr.RequestID != 42 {
		t.Errorf("RequestID: want 42, got %d", hdr.RequestID)
	}
	if hdr.OpCode != OpQuery {
		t.Errorf("OpCode: want OpQuery (%d), got %d", OpQuery, hdr.OpCode)
	}
	if !bytes.Equal(rem, body) {
		t.Errorf("rem: want %v, got %v", body, rem)
	}
}

// TestAppendHeader_LittleEndian verifies the byte-exact encoding of AppendHeader
// against the test vector from wire-protocol-messages.md §2.4.
//
// Vector: reqid=2, responseTo=1, opcode=OpMsg (2013)
// Expected bytes:
//
//	[00 00 00 00] [02 00 00 00] [01 00 00 00] [DD 07 00 00]
func TestAppendHeader_LittleEndian(t *testing.T) {
	hdr := MsgHeader{
		MessageLength: 0,
		RequestID:     2,
		ResponseTo:    1,
		OpCode:        OpMsg,
	}
	got := AppendHeader(nil, hdr)
	want := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0xDD, 0x07, 0x00, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Errorf("AppendHeader bytes:\n got:  %x\n want: %x", got, want)
	}
}

// TestAppendHeader_OpQuery verifies the opcode byte encoding for OpQuery (2004).
// From wire-protocol-messages.md §2.4: bytes 12-15 should be [D4 07 00 00].
func TestAppendHeader_OpQuery(t *testing.T) {
	hdr := MsgHeader{OpCode: OpQuery}
	raw := AppendHeader(nil, hdr)
	opBytes := raw[12:16]
	want := []byte{0xD4, 0x07, 0x00, 0x00}
	if !bytes.Equal(opBytes, want) {
		t.Errorf("OpQuery bytes [12:16]: got %x, want %x", opBytes, want)
	}
}

// TestAppendHeaderStart_FillMessageLength verifies that the placeholder
// mechanism correctly back-fills the MessageLength field once the body is
// known (wire-protocol-messages.md §2.2).
func TestAppendHeaderStart_FillMessageLength(t *testing.T) {
	var dst []byte
	idx, dst := AppendHeaderStart(dst, 7, 0, OpReply)

	// Simulate writing a 4-byte body
	body := []byte{0x01, 0x02, 0x03, 0x04}
	dst = append(dst, body...)

	dst = FillMessageLength(dst, idx)

	// The total length is 16 (header) + 4 (body) = 20.
	gotLen := readI32(dst[idx:])
	if gotLen != 20 {
		t.Errorf("FillMessageLength: want 20, got %d", gotLen)
	}

	// The rest of the header should be intact.
	hdr, _, ok := ParseHeader(dst[idx:])
	if !ok {
		t.Fatal("ParseHeader after FillMessageLength failed")
	}
	if hdr.RequestID != 7 {
		t.Errorf("RequestID: want 7, got %d", hdr.RequestID)
	}
	if hdr.OpCode != OpReply {
		t.Errorf("OpCode: want OpReply (%d), got %d", OpReply, hdr.OpCode)
	}
}

// TestParseAppendHeader_RoundTrip verifies that AppendHeader → ParseHeader
// round-trips without data loss for several opcode values.
func TestParseAppendHeader_RoundTrip(t *testing.T) {
	cases := []MsgHeader{
		{MessageLength: 16, RequestID: 0, ResponseTo: 0, OpCode: OpMsg},
		{MessageLength: 100, RequestID: 1234567, ResponseTo: 99, OpCode: OpReply},
		{MessageLength: 1024, RequestID: -1, ResponseTo: 0, OpCode: OpQuery},
	}
	for _, want := range cases {
		raw := AppendHeader(nil, want)
		got, _, ok := ParseHeader(raw)
		if !ok {
			t.Fatalf("ParseHeader failed for %+v", want)
		}
		if got != want {
			t.Errorf("round-trip mismatch:\n got:  %+v\n want: %+v", got, want)
		}
	}
}

// TestReadI32_SignedNegative confirms that readI32 correctly decodes -1 as
// [FF FF FF FF] (wire-protocol-messages.md §1.1 test vector).
func TestReadI32_SignedNegative(t *testing.T) {
	src := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	if got := readI32(src); got != -1 {
		t.Errorf("readI32(0xFF…): want -1, got %d", got)
	}
}

// TestReadI32_MaxInt32 confirms the encoding of math.MaxInt32 as
// [FF FF FF 7F] (wire-protocol-messages.md §1.1 test vector).
func TestReadI32_MaxInt32(t *testing.T) {
	src := []byte{0xFF, 0xFF, 0xFF, 0x7F}
	want := int32(2147483647)
	if got := readI32(src); got != want {
		t.Errorf("readI32: want %d, got %d", want, got)
	}
}

// TestOpCodeValues spot-checks the numeric values of the opcode constants
// against the authoritative table in wire-protocol-messages.md §3.
func TestOpCodeValues(t *testing.T) {
	checks := []struct {
		name string
		code OpCode
		want int32
	}{
		{"OpReply", OpReply, 1},
		{"OpQuery", OpQuery, 2004},
		{"OpMsg", OpMsg, 2013},
		{"OpCompressed", OpCompressed, 2012},
		{"OpGetMore", OpGetMore, 2005},
		{"OpKillCursors", OpKillCursors, 2007},
		{"OpUpdate", OpUpdate, 2001},
		{"OpInsert", OpInsert, 2002},
		{"OpDelete", OpDelete, 2006},
	}
	for _, c := range checks {
		if int32(c.code) != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, int32(c.code))
		}
	}
}
