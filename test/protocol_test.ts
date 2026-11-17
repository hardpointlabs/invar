// Protocol negotiation: what each side of HELLO / RESET puts on the wire.
//
// The command battery in main_test.ts covers reply *shapes* under each
// protocol through a client library. These tests are about the connection
// itself, so they speak raw RESP over a socket and assert the exact bytes,
// then check that the two client libraries the suite uses negotiate as
// expected.
import { createClient } from "redis-resp3";
import { Redis } from "ioredis";
import { assert, assertEquals, assertMatch, assertStringIncludes } from "@std/assert";

const HOST = "localhost";
const PORT = 6379;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Index just past the RESP frame that starts at `pos`, or -1 if incomplete. */
function frameEnd(buf: Uint8Array, pos: number): number {
  if (pos >= buf.length) return -1;
  const eol = (from: number) => {
    for (let i = from; i + 1 < buf.length; i++) {
      if (buf[i] === 13 && buf[i + 1] === 10) return i;
    }
    return -1;
  };
  const type = String.fromCharCode(buf[pos]);
  const end = eol(pos);
  if (end < 0) return -1;
  const line = decoder.decode(buf.subarray(pos + 1, end));
  const next = end + 2;
  switch (type) {
    case "$":
    case "=": {
      const len = parseInt(line);
      if (len < 0) return next;
      return buf.length >= next + len + 2 ? next + len + 2 : -1;
    }
    case "*":
    case "~":
    case ">":
    case "%": {
      const n = parseInt(line);
      if (n < 0) return next;
      let at = next;
      for (let i = 0; i < (type === "%" ? n * 2 : n); i++) {
        at = frameEnd(buf, at);
        if (at < 0) return -1;
      }
      return at;
    }
    default:
      return next; // + - : _ # , (
  }
}

/** A bare RESP socket that hands back each reply's exact wire text. */
class RawClient {
  private buf = new Uint8Array(0);
  private constructor(private conn: Deno.TcpConn) {}

  static async connect(): Promise<RawClient> {
    return new RawClient(await Deno.connect({ hostname: HOST, port: PORT }));
  }

  async send(...args: string[]): Promise<void> {
    const wire = `*${args.length}\r\n` +
      args.map((a) => `$${encoder.encode(a).length}\r\n${a}\r\n`).join("");
    await this.conn.write(encoder.encode(wire));
  }

  /** The next reply frame, verbatim. */
  async read(): Promise<string> {
    const chunk = new Uint8Array(65536);
    for (let attempt = 0; attempt < 500; attempt++) {
      const end = frameEnd(this.buf, 0);
      if (end >= 0) {
        const frame = decoder.decode(this.buf.subarray(0, end));
        this.buf = this.buf.slice(end);
        return frame;
      }
      const n = await Promise.race([
        this.conn.read(chunk),
        new Promise<null>((r) => setTimeout(() => r(null), 5000)),
      ]);
      if (n === null) throw new Error(`timed out; buffered: ${JSON.stringify(decoder.decode(this.buf))}`);
      const joined = new Uint8Array(this.buf.length + n);
      joined.set(this.buf);
      joined.set(chunk.subarray(0, n), this.buf.length);
      this.buf = joined;
    }
    throw new Error("no complete frame");
  }

  /** Sends a command and returns its reply. */
  async call(...args: string[]): Promise<string> {
    await this.send(...args);
    return await this.read();
  }

  close(): void {
    try {
      this.conn.close();
    } catch {
      // already closed
    }
  }
}

async function withClient(fn: (c: RawClient) => Promise<void>): Promise<void> {
  const c = await RawClient.connect();
  try {
    await fn(c);
  } finally {
    c.close();
  }
}

const NULL2 = "$-1\r\n";
const NULL3 = "_\r\n";

Deno.test("a new connection speaks RESP2", async () => {
  await withClient(async (c) => {
    assertEquals(await c.call("GET", "proto_missing"), NULL2);
    assertEquals(await c.call("HGETALL", "proto_missing"), "*0\r\n");
    // HELLO with no arguments reports the protocol without changing it.
    const hello = await c.call("HELLO");
    assertMatch(hello, /^\*14\r\n\$6\r\nserver\r\n\$5\r\nredis\r\n\$7\r\nversion\r\n/);
    assertStringIncludes(hello, "$5\r\nproto\r\n:2\r\n");
  });
});

Deno.test("HELLO 3 replies with a map, and nulls become _", async () => {
  await withClient(async (c) => {
    const hello = await c.call("HELLO", "3");
    // The reply to HELLO 3 is itself RESP3.
    assertMatch(hello, /^%7\r\n\$6\r\nserver\r\n\$5\r\nredis\r\n\$7\r\nversion\r\n/);
    assertStringIncludes(hello, "$5\r\nproto\r\n:3\r\n");
    assertStringIncludes(hello, "$7\r\nmodules\r\n*0\r\n");

    assertEquals(await c.call("GET", "proto_missing"), NULL3);
    assertEquals(await c.call("HGETALL", "proto_missing"), "%0\r\n");
    assertEquals(await c.call("SMEMBERS", "proto_missing"), "~0\r\n");
  });
});

Deno.test("HELLO 2 after HELLO 3 goes back to RESP2", async () => {
  await withClient(async (c) => {
    await c.call("HELLO", "3");
    assertEquals(await c.call("GET", "proto_missing"), NULL3);

    // The reply to HELLO 2 is already RESP2.
    const hello = await c.call("HELLO", "2");
    assertMatch(hello, /^\*14\r\n/);
    assertStringIncludes(hello, "$5\r\nproto\r\n:2\r\n");
    assertEquals(await c.call("GET", "proto_missing"), NULL2);
  });
});

Deno.test("a bare HELLO keeps the current protocol", async () => {
  await withClient(async (c) => {
    await c.call("HELLO", "3");
    const hello = await c.call("HELLO");
    assertMatch(hello, /^%7\r\n/);
    assertStringIncludes(hello, "$5\r\nproto\r\n:3\r\n");
    assertEquals(await c.call("GET", "proto_missing"), NULL3);
  });
});

Deno.test("an unsupported version is refused and changes nothing", async () => {
  await withClient(async (c) => {
    for (const v of ["4", "1", "0"]) {
      assertMatch(await c.call("HELLO", v), /^-NOPROTO /);
    }
    assertEquals(await c.call("GET", "proto_missing"), NULL2);

    // A refused HELLO leaves RESP3 connections in RESP3, and does not apply SETNAME.
    await c.call("HELLO", "3");
    assertMatch(await c.call("HELLO", "9", "SETNAME", "refused"), /^-NOPROTO /);
    assertEquals(await c.call("GET", "proto_missing"), NULL3);
    assertEquals(await c.call("CLIENT", "GETNAME"), NULL3);
  });
});

Deno.test("HELLO 3 accepts AUTH and SETNAME", async () => {
  await withClient(async (c) => {
    const hello = await c.call("HELLO", "3", "AUTH", "default", "pw", "SETNAME", "proto-test");
    assertMatch(hello, /^%7\r\n/);
    assertEquals(await c.call("CLIENT", "GETNAME"), "$10\r\nproto-test\r\n");
  });
});

Deno.test("HELLO is rejected inside MULTI under either protocol", async () => {
  for (const proto of ["2", "3"]) {
    await withClient(async (c) => {
      await c.call("HELLO", proto);
      assertEquals(await c.call("MULTI"), "+OK\r\n");
      assertMatch(await c.call("HELLO", "3", "SETNAME", "queued"), /^-.*not allowed inside a transaction/);
      // The rejected command dirties the transaction.
      assertMatch(await c.call("EXEC"), /^-EXECABORT/);
      // It changed neither the protocol nor the name.
      assertEquals(await c.call("GET", "proto_missing"), proto === "3" ? NULL3 : NULL2);
      assertEquals(await c.call("CLIENT", "GETNAME"), proto === "3" ? NULL3 : NULL2);
    });
  }
});

Deno.test("RESET returns the connection to RESP2", async () => {
  await withClient(async (c) => {
    await c.call("HELLO", "3", "SETNAME", "before-reset");
    await c.call("SELECT", "4");
    assertEquals(await c.call("RESET"), "+RESET\r\n");
    assertEquals(await c.call("GET", "proto_missing"), NULL2);
    assertEquals(await c.call("CLIENT", "GETNAME"), NULL2);
    // And DB 0 again.
    await c.call("SET", "proto_reset_key", "v");
    await c.call("SELECT", "0");
    assertEquals(await c.call("GET", "proto_reset_key"), "$1\r\nv\r\n");
    await c.call("DEL", "proto_reset_key");
  });
});

Deno.test("RESET discards a MULTI and WATCH", async () => {
  await withClient(async (c) => {
    await c.call("WATCH", "proto_watched");
    await c.call("MULTI");
    await c.call("SET", "proto_queued", "1");
    assertEquals(await c.call("RESET"), "+RESET\r\n");
    assertMatch(await c.call("EXEC"), /^-.*EXEC without MULTI/);
    assertEquals(await c.call("GET", "proto_queued"), NULL2);
  });
});

// --- subscribe mode ---

Deno.test("RESP3 subscribe mode sends push frames", async () => {
  const sub = await RawClient.connect();
  const pub = await RawClient.connect();
  try {
    await sub.call("HELLO", "3");
    assertEquals(await sub.call("SUBSCRIBE", "proto_ch"), ">3\r\n$9\r\nsubscribe\r\n$8\r\nproto_ch\r\n:1\r\n");
    assertEquals(
      await sub.call("PSUBSCRIBE", "proto_p*"),
      ">3\r\n$10\r\npsubscribe\r\n$8\r\nproto_p*\r\n:2\r\n",
    );

    assertEquals(await pub.call("PUBLISH", "proto_ch", "hi"), ":1\r\n");
    assertEquals(await sub.read(), ">3\r\n$7\r\nmessage\r\n$8\r\nproto_ch\r\n$2\r\nhi\r\n");
    assertEquals(await pub.call("PUBLISH", "proto_px", "yo"), ":1\r\n");
    assertEquals(
      await sub.read(),
      ">4\r\n$8\r\npmessage\r\n$8\r\nproto_p*\r\n$8\r\nproto_px\r\n$2\r\nyo\r\n",
    );

    assertEquals(
      await sub.call("UNSUBSCRIBE", "proto_ch"),
      ">3\r\n$11\r\nunsubscribe\r\n$8\r\nproto_ch\r\n:1\r\n",
    );
  } finally {
    sub.close();
    pub.close();
  }
});

Deno.test("RESP2 subscribe mode still sends arrays", async () => {
  const sub = await RawClient.connect();
  const pub = await RawClient.connect();
  try {
    assertEquals(await sub.call("SUBSCRIBE", "proto_ch2"), "*3\r\n$9\r\nsubscribe\r\n$9\r\nproto_ch2\r\n:1\r\n");
    assertEquals(await pub.call("PUBLISH", "proto_ch2", "hi"), ":1\r\n");
    assertEquals(await sub.read(), "*3\r\n$7\r\nmessage\r\n$9\r\nproto_ch2\r\n$2\r\nhi\r\n");
    assertEquals(await sub.call("PING"), "*2\r\n$4\r\npong\r\n$0\r\n\r\n");
  } finally {
    sub.close();
    pub.close();
  }
});

Deno.test("subscribe mode restrictions are the same under both protocols", async () => {
  const errors: string[] = [];
  for (const proto of ["2", "3"]) {
    await withClient(async (c) => {
      await c.call("HELLO", proto);
      await c.call("SUBSCRIBE", "proto_restrict");
      errors.push(await c.call("SET", "a", "b"));
      errors.push(await c.call("GET", "a"));
    });
  }
  assertMatch(errors[0], /^-/);
  assertEquals(errors[0], errors[2]);
  assertEquals(errors[1], errors[3]);
});

Deno.test("PING while subscribed: array on RESP2, plain reply on RESP3", async () => {
  await withClient(async (c) => {
    await c.call("HELLO", "3");
    await c.call("SUBSCRIBE", "proto_ping");
    assertEquals(await c.call("PING"), "+PONG\r\n");
    assertEquals(await c.call("PING", "msg"), "$3\r\nmsg\r\n");
  });
});

Deno.test("RESET leaves subscribe mode and the connection keeps working", async () => {
  await withClient(async (c) => {
    await c.call("HELLO", "3");
    await c.call("SUBSCRIBE", "proto_reset_ch");
    assertEquals(await c.call("RESET"), "+RESET\r\n");
    // Several commands in a row: the connection must not be closed.
    for (let i = 0; i < 3; i++) {
      assertEquals(await c.call("PING"), "+PONG\r\n");
    }
    assertEquals(await c.call("GET", "proto_missing"), NULL2);
    assertEquals(await c.call("PUBLISH", "proto_reset_ch", "x"), ":0\r\n");
  });
});

// --- client libraries ---

Deno.test("ioredis negotiates RESP3 by default and RESP2 when asked", async () => {
  const modern = new Redis({ host: HOST, port: PORT });
  const legacy = new Redis({ host: HOST, port: PORT, protocol: 2 });
  try {
    const protoOf = (reply: unknown) => (reply as unknown[])[5];
    assertEquals(protoOf(await modern.call("HELLO")), 3);
    assertEquals(protoOf(await legacy.call("HELLO")), 2);

    for (const r of [modern, legacy]) {
      await r.del("proto_io_h", "proto_io_z", "proto_io_s");
      await r.hset("proto_io_h", "a", "1");
      await r.zadd("proto_io_z", 1.5, "m");
      await r.sadd("proto_io_s", "x");
      assertEquals(await r.hgetall("proto_io_h"), { a: "1" });
      assertEquals(await r.zscore("proto_io_z", "m"), "1.5");
      assertEquals(await r.zrange("proto_io_z", "0", "-1", "WITHSCORES"), ["m", "1.5"]);
      assertEquals(await r.smembers("proto_io_s"), ["x"]);
      assertEquals(await r.get("proto_io_missing"), null);
      await r.del("proto_io_h", "proto_io_z", "proto_io_s");
    }
  } finally {
    modern.disconnect();
    legacy.disconnect();
  }
});

Deno.test("node-redis receives pub/sub messages on RESP2 and RESP3", async () => {
  for (const resp of [2, 3] as const) {
    const sub = createClient({ url: `redis://${HOST}:${PORT}`, RESP: resp, socket: { reconnectStrategy: false } });
    const pub = createClient({ url: `redis://${HOST}:${PORT}`, RESP: resp, socket: { reconnectStrategy: false } });
    await sub.connect();
    await pub.connect();
    try {
      const received = new Promise<[string, string]>((resolve) => {
        sub.subscribe(`proto_nr_${resp}`, (message, channel) => resolve([message, channel]));
      });
      // Wait until the subscription is in place before publishing.
      for (let i = 0; i < 50; i++) {
        const counts = await pub.pubSubNumSub(`proto_nr_${resp}`);
        if (Number(counts[`proto_nr_${resp}`]) > 0) break;
        await new Promise((r) => setTimeout(r, 20));
      }
      await pub.publish(`proto_nr_${resp}`, "hello");
      const timeout = new Promise<never>((_, reject) => setTimeout(() => reject(new Error("no message")), 5000));
      assertEquals(await Promise.race([received, timeout]), ["hello", `proto_nr_${resp}`]);
      assert(true);
    } finally {
      await sub.destroy();
      await pub.destroy();
    }
  }
});
