// Integration tests for TTL/PTTL semantics, focusing on two fixed bugs:
//
// 1. PTTL on a key with no expiry previously returned -1000 (a leftover of a
//    `seconds * 1000` conversion applied to the "-1 no-expiry" sentinel).  It
//    must return -1, matching the Redis contract.
//
// 2. TTL/PTTL were computed from whole-second-truncated expiry, so a key with a
//    sub-second TTL (e.g. PX 500) could be reported as gone (pttl=-2) shortly
//    after being set -- well before its expiry actually elapsed.  After a short
//    delay the key must still be alive, with a positive remaining TTL/PTTL.
//
// A final test confirms real expiration still happens once the TTL has truly
// elapsed, so the fix didn't merely disable expiry.
import { createClient } from "redis";
import { assertEquals } from "@std/assert";

const REDIS_URL = "redis://localhost:6379";

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(() => resolve(), ms));
}

async function connect() {
  const client = createClient({ url: REDIS_URL, socket: { reconnectStrategy: false } });
  await client.connect();
  return client;
}

Deno.test("PTTL and TTL return -1 for a key with no expiry", async () => {
  const client = await connect();
  try {
    await client.set("ttl_no_expiry", "v");
    // Redis returns -1 when the key exists but has no associated TTL.  The bug
    // under test previously surfaced the "no expiry" sentinel as -1000.
    assertEquals(await client.sendCommand(["PTTL", "ttl_no_expiry"]), -1);
    assertEquals(await client.sendCommand(["TTL", "ttl_no_expiry"]), -1);
  } finally {
    await client.quit();
  }
});

Deno.test("PTTL/TTL still report a live key shortly after a sub-second PX expiry", async () => {
  const client = await connect();
  try {
    // PX 500 gives a TTL that is a fraction of a second, which is exactly where
    // the whole-second-truncation bug prematurely reported the key as expired.
    await client.set("ttl_ms_precision", "v", { PX: 500 });
    await sleep(250);

    // After only ~250ms of a 500ms expiry the key must still be alive: PTTL is
    // positive (not -2 / -1) and TTL is real (not -2).
    const pttl = await client.sendCommand(["PTTL", "ttl_ms_precision"]);
    const ttl = await client.sendCommand(["TTL", "ttl_ms_precision"]);
    assertEquals(typeof pttl, "number");
    assertEquals(typeof ttl, "number");
    assertEquals((pttl as number) > 0, true, `PTTL should be positive, got ${pttl}`);
    assertEquals((ttl as number) >= 0, true, `TTL should be >= 0, got ${ttl}`);
    assertEquals(await client.get("ttl_ms_precision"), "v");
  } finally {
    await client.quit();
  }
});

Deno.test("key is reported as expired once its PX TTL has truly elapsed", async () => {
  const client = await connect();
  try {
    await client.set("ttl_real_expiry", "v", { PX: 200 });
    await sleep(450);

    // With the sub-second TTL fully elapsed the key must be gone: GET is null
    // and PTTL is the "no such key" sentinel -2.
    assertEquals(await client.get("ttl_real_expiry"), null);
    assertEquals(await client.sendCommand(["PTTL", "ttl_real_expiry"]), -2);
  } finally {
    await client.quit();
  }
});
