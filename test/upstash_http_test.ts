// Runs the command battery from redis-commands.json against Invar's
// Redis-over-HTTP listener (RFC 0003), through the Upstash TypeScript SDK's own
// HTTP transport, with the same expectations main_test.ts uses over RESP2.
//
// The SDK's typed helpers reshape replies (JSON-parsing strings, turning flat
// arrays into objects), which would change what the shared expectations mean.
// So commands are sent as raw argument arrays through the SDK's request
// pipeline (URL handling, headers, retries, base64 decoding, error mapping) and
// the reply is compared as-is.
//
// Each case runs once per response encoding: plain JSON and `base64`.

import { Redis } from "@upstash/redis";
import { assertEquals } from "@std/assert";

const HTTP_URL = Deno.env.get("INVAR_HTTP_URL") ?? "http://localhost:8080";

interface TestCase {
  name: string;
  command: string[];
  expected: unknown;
  type: string;
  setup?: string[][];
}

const testCases: TestCase[] = JSON.parse(
  new TextDecoder().decode(Deno.readFileSync("redis-commands.json")),
);

// Exposes the SDK's transport so arbitrary commands can be sent unmodified.
class RawRedis extends Redis {
  send(args: string[]): Promise<unknown> {
    return this.client.request<unknown>({ body: args }).then((r) => r.result);
  }
}

// Commands the HTTP listener rejects by design (see RFC 0003, "Command
// availability"): they need a long-lived connection, or they block.
const UNSUPPORTED = new Set([
  "SELECT", "AUTH", "HELLO", "CLIENT", "MULTI", "EXEC", "DISCARD", "WATCH",
  "UNWATCH", "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "QUIT", "RESET",
  "BLPOP", "BRPOP", "BLMOVE", "BZPOPMIN", "BZPOPMAX", "BGSAVE",
]);

// Cases with no setup of their own that read state written by an earlier case
// this suite skips, so they can't be meaningful here.
const DEPENDS_ON_SKIPPED = new Set([
  "Confirm the SET before the WRONGTYPE persisted despite the later failure",
]);

function unsupported(cmd: string[]): boolean {
  const name = cmd[0].toUpperCase();
  return UNSUPPORTED.has(name) ||
    (name === "XREAD" && cmd.some((a) => a.toUpperCase() === "BLOCK"));
}

function isErrorMarker(value: unknown): boolean {
  return value !== null && typeof value === "object" && !Array.isArray(value) &&
    "error" in value;
}

function compare(actual: unknown, expected: unknown, type: string): boolean {
  switch (type) {
    case "bulk_string_startswith":
      return typeof actual === "string" && actual.startsWith(expected as string);
    case "bulk_string_contains":
      return typeof actual === "string" && actual.includes(expected as string);
    case "integer":
      return Number(actual) === Number(expected);
    case "integer_gt": {
      const threshold = Number((expected as string).substring(2));
      return typeof actual === "number" && actual > threshold;
    }
    case "null_or_bulk":
      return actual === null || actual === expected;
    case "array_length":
      return Array.isArray(actual) && actual.length === Number(expected);
    case "set":
      if (!Array.isArray(actual) || !Array.isArray(expected)) return false;
      return actual.length === expected.length &&
        expected.every((item) => actual.includes(item));
    case "array":
      return arrayMatches(actual, expected);
    default:
      return actual === expected;
  }
}

function arrayMatches(actual: unknown, expected: unknown): boolean {
  if (!Array.isArray(actual) || !Array.isArray(expected)) return false;
  if (actual.length !== expected.length) return false;
  return expected.every((exp, i) => {
    if (isErrorMarker(exp)) return false; // errors can't appear inside a JSON result array
    if (Array.isArray(exp)) return arrayMatches(actual[i], exp);
    return actual[i] === exp;
  });
}

type Outcome =
  | { kind: "value"; value: unknown }
  | { kind: "error"; message: string };

async function run(redis: RawRedis, cmd: string[]): Promise<Outcome> {
  try {
    return { kind: "value", value: await redis.send(cmd) };
  } catch (err) {
    return { kind: "error", message: String(err) };
  }
}

for (const encoding of [false, "base64"] as const) {
  Deno.test(`Redis-over-HTTP command battery (${encoding || "plain"} responses)`, async () => {
    const redis = new RawRedis({
      url: HTTP_URL,
      token: "unused", // the SDK insists on a token; Invar ignores it
      responseEncoding: encoding,
      automaticDeserialization: false,
      enableTelemetry: false,
      retry: false,
    });
    await redis.send(["FLUSHALL"]);

    const failures: string[] = [];
    let skipped = 0;
    for (const tc of testCases) {
      if (DEPENDS_ON_SKIPPED.has(tc.name) || [tc.command, ...(tc.setup ?? [])].some(unsupported)) {
        skipped++;
        continue;
      }
      for (const setup of tc.setup ?? []) await run(redis, setup);
      const out = await run(redis, tc.command);

      let ok: boolean;
      let detail: string;
      if (out.kind === "error") {
        ok = isErrorMarker(tc.expected) ||
          (tc.type === "error" && typeof tc.expected === "string" &&
            out.message.includes(tc.expected));
        detail = `error: ${out.message}`;
      } else {
        ok = !isErrorMarker(tc.expected) && tc.type !== "error" &&
          compare(out.value, tc.expected, tc.type);
        detail = `got ${JSON.stringify(out.value)}`;
      }
      if (!ok) {
        failures.push(`${tc.name}: expected ${JSON.stringify(tc.expected)} (${tc.type}), ${detail}`);
      }
    }

    console.log(`ran ${testCases.length - skipped}, skipped ${skipped} connection-bound cases`);
    for (const f of failures) console.error(`FAILED: ${f}`);
    assertEquals(failures.length, 0, `${failures.length} case(s) failed`);
  });
}
