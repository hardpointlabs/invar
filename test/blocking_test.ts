// Producer/consumer integration tests for the blocking sorted-set pop commands
// BZPOPMIN and BZPOPMAX.
//
// Unlike the JSON-driven suites in this directory, the full sequencing for these
// tests is defined here in TypeScript: a "consumer" connection issues the blocking
// command while a "producer" connection pushes members from a separate client to
// drive the consumer's unblocking.
import { createClient } from "redis";
import { assertEquals } from "@std/assert";

const REDIS_URL = "redis://localhost:6379";

// Heuristic pause after issuing a blocking command so the server has time to park
// the waiter before the producer pushes.  This is a heuristic, not a guarantee.
const WAITER_REGISTRATION_MS = 50;

// If a blocking command has not returned within this window, fail rather than hang.
const UNBLOCK_DEADLINE_MS = 2000;

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(() => resolve(), ms));
}

async function connect() {
  const client = createClient({ url: REDIS_URL, socket: { reconnectStrategy: false } });
  await client.connect();
  return client;
}

// Races a blocking command against a deadline so a command that never unblocks
// fails the test instead of waiting out the server-side timeout.  The deadline
// timer is cleared as soon as the command settles to avoid leaking timers.
function withDeadline<T>(promise: Promise<T>, message: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), UNBLOCK_DEADLINE_MS);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

Deno.test("BZPOPMIN blocks then receives a member pushed by another connection", async () => {
  const consumer = await connect();
  const producer = await connect();
  try {
    await consumer.del("bzpopmin_wake");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMIN", "bzpopmin_wake", "5"]),
      "BZPOPMIN never unblocked"
    );

    // Give the server a moment to park the waiter before pushing.
    await sleep(WAITER_REGISTRATION_MS);

    await producer.zAdd("bzpopmin_wake", { score: 1, value: "member1" });

    assertEquals(await popped, ["bzpopmin_wake", "member1", "1"]);
  } finally {
    await consumer.quit();
    await producer.quit();
  }
});

Deno.test("BZPOPMAX blocks then receives a member pushed by another connection", async () => {
  const consumer = await connect();
  const producer = await connect();
  try {
    await consumer.del("bzpopmax_wake");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMAX", "bzpopmax_wake", "5"]),
      "BZPOPMAX never unblocked"
    );

    await sleep(WAITER_REGISTRATION_MS);

    await producer.zAdd("bzpopmax_wake", { score: 1, value: "member1" });

    assertEquals(await popped, ["bzpopmax_wake", "member1", "1"]);
  } finally {
    await consumer.quit();
    await producer.quit();
  }
});

Deno.test("BZPOPMIN times out and returns null when nothing is pushed", async () => {
  const consumer = await connect();
  try {
    await consumer.del("bzpopmin_timeout");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMIN", "bzpopmin_timeout", "1"]),
      "BZPOPMIN should have timed out by now"
    );

    assertEquals(await popped, null);
  } finally {
    await consumer.quit();
  }
});

Deno.test("BZPOPMAX times out and returns null when nothing is pushed", async () => {
  const consumer = await connect();
  try {
    await consumer.del("bzpopmax_timeout");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMAX", "bzpopmax_timeout", "1"]),
      "BZPOPMAX should have timed out by now"
    );

    assertEquals(await popped, null);
  } finally {
    await consumer.quit();
  }
});

Deno.test("BZPOPMIN blocking on multiple keys wakes when any key is pushed", async () => {
  const consumer = await connect();
  const producer = await connect();
  try {
    await consumer.del("bzpopmin_multi_a");
    await consumer.del("bzpopmin_multi_b");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMIN", "bzpopmin_multi_a", "bzpopmin_multi_b", "5"]),
      "BZPOPMIN never unblocked"
    );

    await sleep(WAITER_REGISTRATION_MS);

    // Push only to the second key; the waiter must wake with that key's data.
    await producer.zAdd("bzpopmin_multi_b", { score: 42, value: "member_b" });

    assertEquals(await popped, ["bzpopmin_multi_b", "member_b", "42"]);
  } finally {
    await consumer.quit();
    await producer.quit();
  }
});

Deno.test("BZPOPMIN returns the lowest-score member among those pushed", async () => {
  const consumer = await connect();
  const producer = await connect();
  try {
    await consumer.del("bzpopmin_lowest");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMIN", "bzpopmin_lowest", "5"]),
      "BZPOPMIN never unblocked"
    );

    await sleep(WAITER_REGISTRATION_MS);

    await producer.zAdd("bzpopmin_lowest", [
      { score: 2, value: "high" },
      { score: 1, value: "low" },
    ]);

    assertEquals(await popped, ["bzpopmin_lowest", "low", "1"]);
  } finally {
    await consumer.quit();
    await producer.quit();
  }
});

Deno.test("BZPOPMAX returns the highest-score member among those pushed", async () => {
  const consumer = await connect();
  const producer = await connect();
  try {
    await consumer.del("bzpopmax_highest");

    const popped = withDeadline(
      consumer.sendCommand(["BZPOPMAX", "bzpopmax_highest", "5"]),
      "BZPOPMAX never unblocked"
    );

    await sleep(WAITER_REGISTRATION_MS);

    await producer.zAdd("bzpopmax_highest", [
      { score: 1, value: "low" },
      { score: 2, value: "high" },
    ]);

    assertEquals(await popped, ["bzpopmax_highest", "high", "2"]);
  } finally {
    await consumer.quit();
    await producer.quit();
  }
});

Deno.test("BZPOPMIN unblocks the longest-waiting consumer first (FIFO)", async () => {
  const first = await connect();
  const second = await connect();
  const producer = await connect();
  try {
    await first.del("bzpopmin_fifo");

    const firstPop = withDeadline(
      first.sendCommand(["BZPOPMIN", "bzpopmin_fifo", "5"]),
      "first consumer never unblocked"
    );
    await sleep(WAITER_REGISTRATION_MS);

    const secondPop = withDeadline(
      second.sendCommand(["BZPOPMIN", "bzpopmin_fifo", "5"]),
      "second consumer never unblocked"
    );
    await sleep(WAITER_REGISTRATION_MS);

    // A single push wakes exactly one waiter — the one parked first.
    await producer.zAdd("bzpopmin_fifo", { score: 1, value: "for_first" });
    assertEquals(await firstPop, ["bzpopmin_fifo", "for_first", "1"]);

    // The second consumer must still be blocked until the next push arrives.
    let secondResolvedEarly = false;
    await Promise.race([
      secondPop.then(() => { secondResolvedEarly = true; }),
      sleep(WAITER_REGISTRATION_MS),
    ]);
    assertEquals(secondResolvedEarly, false);

    await producer.zAdd("bzpopmin_fifo", { score: 2, value: "for_second" });
    assertEquals(await secondPop, ["bzpopmin_fifo", "for_second", "2"]);
  } finally {
    await first.quit();
    await second.quit();
    await producer.quit();
  }
});
