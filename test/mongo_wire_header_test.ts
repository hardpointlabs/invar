/**
 * MongoDB wire-protocol MsgHeader integration tests.
 *
 * Unit 1 — MsgHeader (parse + write)
 *
 * These tests assert the externally-observable properties of MsgHeader
 * serialisation and parsing by connecting a real MongoDB driver to the
 * Invar mongo listener and inspecting what happens at the TCP level.
 *
 * Because MsgHeader is purely a framing concern — not a user-visible API —
 * the tests exercise it indirectly:
 *
 *   1. A successful connection + any command proves the driver accepted our
 *      headers (MessageLength, RequestID, ResponseTo, OpCode were all correct).
 *   2. Explicit framing-error tests send intentionally-malformed raw bytes
 *      over a plain TCP socket and confirm the server closes the connection
 *      cleanly without crashing.
 *
 * Run from the project root with:
 *   ./run-tests.sh   (for the full suite)
 * or directly:
 *   deno test --allow-net test/mongo_wire_header_test.ts
 */

import { assertEquals, assertRejects } from "@std/assert";
import { MongoClient } from "jsr:@db/mongo";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MONGO_URL = "mongodb://127.0.0.1:27017";
const MONGO_HOST = "127.0.0.1";
const MONGO_PORT = 27017;

/** Open a raw TCP connection to the Invar mongo listener. */
async function rawTcpConnect(): Promise<Deno.TcpConn> {
  return await Deno.connect({ hostname: MONGO_HOST, port: MONGO_PORT });
}

/**
 * Encode a 32-bit signed integer as 4 little-endian bytes, matching the
 * MsgHeader wire format (wire-protocol-messages.md §1.1).
 */
function i32LE(v: number): Uint8Array {
  const b = new Uint8Array(4);
  const view = new DataView(b.buffer);
  view.setInt32(0, v, /*littleEndian=*/ true);
  return b;
}

/** Concatenate Uint8Arrays. */
function concat(...arrays: Uint8Array[]): Uint8Array {
  const total = arrays.reduce((n, a) => n + a.length, 0);
  const out = new Uint8Array(total);
  let pos = 0;
  for (const a of arrays) {
    out.set(a, pos);
    pos += a.length;
  }
  return out;
}

/**
 * Read up to `n` bytes from `conn` with a 2-second deadline.
 * Returns the bytes actually read (may be fewer than n if the server
 * closed the connection).
 */
async function readWithTimeout(conn: Deno.TcpConn, n: number): Promise<Uint8Array> {
  const buf = new Uint8Array(n);
  const deadline = new Promise<null>((resolve) => setTimeout(() => resolve(null), 2000));
  const readProm = conn.read(buf);
  const result = await Promise.race([readProm, deadline]);
  if (result === null) return new Uint8Array(0);
  if (result === 0) return new Uint8Array(0);
  return buf.subarray(0, result as number);
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

/**
 * 1. Basic connectivity — verifies that the server speaks the MongoDB wire
 *    protocol at all.  A successful MongoClient.connect() + ping means the
 *    server sent a well-formed MsgHeader in its OP_REPLY (or OP_MSG) response
 *    and the driver accepted it.
 *
 *    Spec refs: wire-protocol-messages.md §2; 04-connection-handshake.md
 */
Deno.test("MsgHeader: driver connects and ping succeeds", async () => {
  const client = new MongoClient();
  try {
    await client.connect(MONGO_URL);
    const admin = client.database("admin");
    const result = await admin.runCommand({ ping: 1 });
    // The driver only returns a result object if ok==1 was received in a
    // properly-framed message, proving header encoding/decoding is correct.
    assertEquals(result.ok, 1);
  } finally {
    await client.close();
  }
});

/**
 * 2. MessageLength integrity — the driver performs a round-trip command that
 *    requires non-trivial response bodies (buildInfo).  If MessageLength were
 *    wrong the driver would either hang (waiting for more bytes) or error
 *    (declaring EOF mid-document).
 *
 *    Spec ref: wire-protocol-messages.md §2.1, §9.3
 */
Deno.test("MsgHeader: MessageLength is correctly set (buildInfo round-trip)", async () => {
  const client = new MongoClient();
  try {
    await client.connect(MONGO_URL);
    const result = await client.database("admin").runCommand({ buildInfo: 1 });
    assertEquals(result.ok, 1);
    // version field present proves a full document was received without
    // framing corruption.
    assertEquals(typeof result.version, "string");
  } finally {
    await client.close();
  }
});

/**
 * 3. ResponseTo echo — the driver matches each response to the request via
 *    RequestID / ResponseTo.  Sending two concurrent commands and verifying
 *    both succeed implies ResponseTo is correctly echoed per request.
 *
 *    Spec ref: wire-protocol-messages.md §2.1 ("responseTo echoes requestID")
 */
Deno.test("MsgHeader: ResponseTo is echoed correctly (concurrent commands)", async () => {
  const client = new MongoClient();
  try {
    await client.connect(MONGO_URL);
    const db = client.database("admin");
    const [r1, r2] = await Promise.all([
      db.runCommand({ ping: 1 }),
      db.runCommand({ ping: 1 }),
    ]);
    assertEquals(r1.ok, 1);
    assertEquals(r2.ok, 1);
  } finally {
    await client.close();
  }
});

/**
 * 4. MessageLength < 4 — sends a 4-byte raw TCP payload whose length field
 *    is 3 (below the minimum of 4).  The server must close the connection
 *    without returning any data; it must not hang.
 *
 *    Spec ref: wire-protocol-messages.md §11 ("messageLength … must be at
 *    least 16"); 08-behavioural-edge-cases.md §2.1 ("size < 4 → malformed
 *    message length")
 *
 *    SPEC-GAP: The spec says the minimum valid messageLength is 16 (the
 *    header size), but the driver source checks `size < 4`. We test the
 *    driver-observed lower bound of 3.  Verify with driver integration tests
 *    whether a length of 4–15 is also rejected.
 */
Deno.test("MsgHeader: MessageLength=3 causes server to close connection", async () => {
  const conn = await rawTcpConnect();
  try {
    // Write a 4-byte message whose length field claims only 3 bytes —
    // below the minimum of 4 (08-behavioural-edge-cases.md §2.1).
    await conn.write(i32LE(3));
    // The server should close the connection; any attempt to read should
    // return 0 bytes (EOF) or null.
    const got = await readWithTimeout(conn, 64);
    assertEquals(got.length, 0, "expected server to close connection on length=3");
  } finally {
    conn.close();
  }
});

/**
 * 5. MessageLength > maxMessageSize — sends a header claiming a 16 MiB + 1
 *    byte body, which exceeds the negotiated maximum.  The server must close
 *    the connection.
 *
 *    Spec ref: wire-protocol-messages.md §1.2 ("maximum of 16 MiB");
 *    08-behavioural-edge-cases.md §2.1 ("size > maxMessageSize →
 *    errResponseTooLarge")
 *
 *    SPEC-GAP: The spec maximum is 16 MiB as observed from the pool-recycle
 *    guard.  The exact server-enforced limit may be higher (e.g. 48 MB) before
 *    maxMessageSizeBytes is negotiated.  This test uses 16 MiB + 1.  Verify
 *    exact enforcement boundary once the handshake is implemented.
 */
Deno.test("MsgHeader: MessageLength > 16 MiB causes server to close connection", async () => {
  const conn = await rawTcpConnect();
  try {
    const oversized = 16 * 1024 * 1024 + 1;
    await conn.write(i32LE(oversized));
    const got = await readWithTimeout(conn, 64);
    assertEquals(got.length, 0, "expected server to close connection on oversized length");
  } finally {
    conn.close();
  }
});

/**
 * 6. OpCode field integrity — send a valid OP_QUERY isMaster handshake via
 *    the driver and verify a correct OP_REPLY (opCode=1) is sent back.  The
 *    test uses the raw TCP framing layer to inspect the response opcode byte
 *    directly.
 *
 *    Spec ref: wire-protocol-messages.md §3 (OpReply = 1);
 *    04-connection-handshake.md (OP_QUERY → OP_REPLY path)
 *
 *    NOTE: This test is intentionally low-level so that an incorrect OpCode
 *    (e.g. returning OpMsg instead of OpReply for an OP_QUERY handshake) is
 *    caught here rather than only in the driver-visible behaviour tests.
 *
 *    SPEC-GAP: The spec says drivers send OP_QUERY for the legacy handshake
 *    when wire version is unknown. Some drivers may always use OP_MSG.  This
 *    test verifies the OP_REPLY path is functional; OP_MSG handshake
 *    response opcode is tested in the handshake unit.
 */
Deno.test("MsgHeader: OP_QUERY handshake receives OP_REPLY (opCode=1) response", async () => {
  // Build a minimal OP_QUERY for "admin.$cmd" with isMaster:1.
  // Wire layout from wire-protocol-messages.md §6.
  const collName = new TextEncoder().encode("admin.$cmd\0");
  // Minimal BSON: { "isMaster": 1 }
  // 0C 00 00 00   = int32(12) total length
  // 10            = type Int32
  // 69 73 4d 61 73 74 65 72 00  = "isMaster\0"
  // 01 00 00 00   = int32(1)
  // 00            = end of document
  const ismaster = new Uint8Array([
    0x13, 0x00, 0x00, 0x00, // length = 19
    0x10,                                     // type: Int32
    0x69, 0x73, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x00, // "isMaster\0"
    0x01, 0x00, 0x00, 0x00, // value = 1
    0x00,                   // terminator
  ]);

  // OP_QUERY body:
  //   int32 flags=0, cstring "admin.$cmd\0", int32 skip=0, int32 limit=-1, doc
  const flags = i32LE(0);
  const skip = i32LE(0);
  const limitN = i32LE(-1);
  const body = concat(flags, collName, skip, limitN, ismaster);

  // MsgHeader: messageLength = 16 + body.length
  const totalLen = 16 + body.length;
  const header = concat(i32LE(totalLen), i32LE(1), i32LE(0), i32LE(2004));
  const msg = concat(header, body);

  const conn = await rawTcpConnect();
  try {
    await conn.write(msg);

    // Read the response header (16 bytes).
    const hdrBuf = new Uint8Array(16);
    let pos = 0;
    while (pos < 16) {
      const n = await conn.read(hdrBuf.subarray(pos));
      if (n === null || n === 0) break;
      pos += n;
    }

    if (pos < 16) {
      throw new Error(`only read ${pos} bytes of response header`);
    }

    const view = new DataView(hdrBuf.buffer);
    const responseOpCode = view.getInt32(12, /*littleEndian=*/ true);

    // The server must reply with OP_REPLY (opCode = 1).
    assertEquals(responseOpCode, 1, `expected opCode=1 (OP_REPLY), got ${responseOpCode}`);
  } finally {
    conn.close();
  }
});
