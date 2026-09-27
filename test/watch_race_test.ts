// Correctness test for WATCH/MULTI/EXEC against Invar's SSI-backed
// implementation: two workers race to claim the same job. Real Redis
// WATCH semantics guarantee exactly one wins per round -- the loser's
// EXEC resolves to null (ioredis represents Redis's nil multi-bulk
// reply this way), never both, never neither.
//
// Run with:
//   deno test --allow-net --allow-env watch-race.test.ts
//
// Config via env vars:
//   INVAR_HOST    default "localhost"
//   INVAR_PORT    default 6379
//   ROUNDS        default 200

import { assertEquals } from "jsr:@std/assert";
import { Redis } from "npm:ioredis";

const HOST = Deno.env.get("INVAR_HOST") ?? "localhost";
const PORT = Number(Deno.env.get("INVAR_PORT") ?? "6379");
const ROUNDS = Number(Deno.env.get("ROUNDS") ?? 200);

const JOB_KEY = "job:42:status";
const MAX_RETRIES = 5;

function makeClient(name: string): Redis {
  const client = new Redis({ host: HOST, port: PORT, lazyConnect: false });
  client.on("error", (err) => console.error(`[${name}] connection error:`, err.message));
  return client;
}

function jitterMs(baseMs: number): number {
  return baseMs + Math.random() * baseMs;
}

// Mirrors the real Redis retry loop: WATCH the key, read it to decide
// whether there's anything to do, then MULTI/EXEC the actual claim. A
// null result from exec() means the watched key changed underneath us --
// back off briefly and retry from WATCH, same as any real WATCH-based
// client would.
async function claimJob(client: Redis, worker: string): Promise<"claimed" | "already-claimed"> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    await client.watch(JOB_KEY);
    const status = await client.get(JOB_KEY);

    if (status !== "pending") {
      await client.unwatch();
      return "already-claimed";
    }

    // Small artificial gap between the read and the transaction, so both
    // workers are actually likely to overlap instead of one finishing
    // before the other even starts -- otherwise the race rarely triggers.
    await new Promise((r) => setTimeout(r, 5 + Math.random() * 10));

    const result = await client
      .multi()
      .set(JOB_KEY, "processing")
      .lpush(`worker:${worker}:active`, "42")
      .exec();

    if (result !== null) {
      return "claimed"; // transaction committed -- we won the race
    }

    // result === null: watched key changed since WATCH -- someone else
    // claimed it first. Back off briefly and retry from WATCH.
    await new Promise((r) => setTimeout(r, jitterMs(5)));
  }

  throw new Error(`${worker}: exceeded ${MAX_RETRIES} retries without resolving`);
}

Deno.test({
  name: "WATCH-based job claim resolves to exactly one winner, every round",
  // ioredis manages its own sockets/timers in ways Deno's resource
  // sanitizer doesn't reliably recognize as closed even after a clean
  // .quit() -- disable the sanitizers rather than fight false positives
  // from an npm-compat library Deno doesn't fully introspect.
  sanitizeResources: false,
  sanitizeOps: false,
  async fn(t) {
    const clientA = makeClient("A");
    const clientB = makeClient("B");
    let aWins = 0;
    let bWins = 0;

    try {
      for (let round = 1; round <= ROUNDS; round++) {
        // t.step reports pass/fail per round individually -- a step
        // failing doesn't stop subsequent rounds, so a single flaky
        // round is visible without masking whether it's a one-off or
        // systematic across the whole run.
        await t.step(`round ${round}`, async () => {
          await clientA.set(JOB_KEY, "pending");
          await clientA.del("worker:A:active", "worker:B:active");

          const [resultA, resultB] = await Promise.all([
            claimJob(clientA, "A"),
            claimJob(clientB, "B"),
          ]);

          const claims = [resultA, resultB].filter((r) => r === "claimed").length;
          assertEquals(
            claims,
            1,
            `expected exactly 1 claim, got ${claims} (A=${resultA}, B=${resultB})`,
          );

          if (resultA === "claimed") aWins++;
          else bWins++;
        });
      }
    } finally {
      console.log(`\nA won ${aWins}, B won ${bWins} across ${ROUNDS} rounds.`);
      await clientA.quit().catch(() => {});
      await clientB.quit().catch(() => {});
    }
  },
});