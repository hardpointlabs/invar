import { createClient, RESP_TYPES } from "redis-resp3";
import { assertEquals, assertExists } from "@std/assert";

interface TestCase {
  name: string;
  command: string[];
  expected: unknown;
  type: string;
  version: string;
  description: string;
  setup?: string[][];
  // Optional overrides for RESP3 connections, for the commands whose reply
  // shape differs there (maps, sets, doubles, pairs, a single null). Omitted
  // means the RESP2 expectation holds on both protocols.
  expected_resp3?: unknown;
  type_resp3?: string;
}

// Every case runs once per protocol, each time on a fresh connection that
// negotiated that protocol with HELLO.
const PROTOCOLS = [2, 3] as const;
type Protocol = (typeof PROTOCOLS)[number];

type Client = Awaited<ReturnType<typeof connect>>;

// RESP3 maps and sets are surfaced as Map and Set so a test can tell them
// apart from the arrays RESP2 sends. Doubles arrive as JS numbers (RESP2 sends
// a bulk string, so a double is distinguishable by type too).
async function connect(resp: Protocol) {
  const base = createClient({
    url: "redis://localhost:6379",
    RESP: resp,
    socket: { reconnectStrategy: false },
  });
  await base.connect();
  return resp === 3
    ? base.withTypeMapping({ [RESP_TYPES.MAP]: Map, [RESP_TYPES.SET]: Set })
    : base.withTypeMapping({});
}

function effectiveCase(testCase: TestCase, resp: Protocol): TestCase {
  if (resp === 3 && "expected_resp3" in testCase) {
    return {
      ...testCase,
      expected: testCase.expected_resp3,
      type: testCase.type_resp3 ?? testCase.type,
    };
  }
  return testCase;
}

// Structural comparison for RESP3 expectations. JSON can't spell a Map or a
// Set, so expectations tag them: {"$map": [[k, v], ...]} and {"$set": [...]}.
// Maps compare by key (hash order is not part of the contract), sets ignore
// order, arrays are ordered, and numbers must be actual numbers.
function resp3Equal(actual: unknown, expected: unknown): boolean {
  if (expected !== null && typeof expected === "object" && !Array.isArray(expected)) {
    const tagged = expected as { $map?: [unknown, unknown][]; $set?: unknown[] };
    if (tagged.$map) {
      if (!(actual instanceof Map) || actual.size !== tagged.$map.length) return false;
      return tagged.$map.every(([k, v]) => actual.has(k) && resp3Equal(actual.get(k), v));
    }
    if (tagged.$set) {
      if (!(actual instanceof Set) || actual.size !== tagged.$set.length) return false;
      return tagged.$set.every((item) => actual.has(item));
    }
    return false;
  }
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && actual.length === expected.length &&
      expected.every((item, i) => resp3Equal(actual[i], item));
  }
  return actual === expected;
}

interface TestResult {
  name: string;
  version: string;
  status: "passed" | "failed" | "skipped";
  expected: unknown;
  actual: unknown;
  error?: string;
}

const testCases: TestCase[] = JSON.parse(
  new TextDecoder().decode(Deno.readFileSync("redis-commands.json"))
);

// An error marker ({"error": true}) means "any error reply matches here", so
// tests can assert that an error appears without pinning its exact message.
function isErrorMarker(value: unknown): boolean {
  return value !== null && typeof value === "object" && "error" in value;
}

let client: Client | null = null;
const results: TestResult[] = [];

function compareResult(actual: unknown, expected: unknown, type: string): boolean {
  switch (type) {
    case "simple_string":
    case "bulk_string":
      return actual === expected;
    case "bulk_string_startswith":
      return typeof actual === "string" && actual.startsWith(expected as string);
    case "bulk_string_contains":
      return typeof actual === "string" && actual.includes(expected as string);
    case "integer":
      return Number(actual) === Number(expected);
    case "integer_gt":
      // expected is like "> -1" meaning actual should be > -1
      if (typeof expected !== "string" || !expected.startsWith("> ")) return false;
      const threshold = Number(expected.substring(2));
      return typeof actual === "number" && actual > threshold;
    case "null":
      return actual === null;
    case "resp3":
      return resp3Equal(actual, expected);
    case "resp3_map_contains": {
      // A map that includes at least the expected entries.
      const want = (expected as { $map: [unknown, unknown][] }).$map;
      return actual instanceof Map &&
        want.every(([k, v]) => actual.has(k) && resp3Equal(actual.get(k), v));
    }
    case "error":
      if (isErrorMarker(expected)) return typeof actual === "string" && actual.length > 0;
      return typeof actual === "string" && actual.includes(expected as string);
    case "null_or_bulk":
      return actual === null || actual === expected;
    case "array_length":
      return Array.isArray(actual) && actual.length === Number(expected);
    case "array":
      if (!Array.isArray(expected) || !Array.isArray(actual)) return false;
      if (expected.length !== actual.length) return false;
      for (let i = 0; i < expected.length; i++) {
        const exp = expected[i];
        if (isErrorMarker(exp)) {
          // error marker: any Error reply in this slot matches
          if (!(actual[i] instanceof Error)) return false;
        } else if (exp === null) {
          if (actual[i] !== null) return false;
        } else if (Array.isArray(exp)) {
          // nested array (e.g. the keys list in a SCAN reply): recurse
          if (!compareResult(actual[i], exp, "array")) return false;
        } else {
          if (actual[i] !== exp) return false;
        }
      }
      return true;
    case "set":
      if (!Array.isArray(actual)) return false;
      if (!Array.isArray(expected)) return actual === expected;
      if (expected.length !== actual.length) return false;
      const actualSet = new Set(actual);
      for (const item of expected) {
        if (!actualSet.has(item)) return false;
      }
      return true;
    default:
      return actual === expected;
  }
}

function formatResult(actual: unknown): string {
  if (actual === null) return "null";
  if (actual === undefined) return "undefined";
  if (Array.isArray(actual)) {
    return "[" + actual.map(formatResult).join(", ") + "]";
  }
  if (actual instanceof Map) {
    return "Map{" + [...actual].map(([k, v]) => `${formatResult(k)} => ${formatResult(v)}`).join(", ") + "}";
  }
  if (actual instanceof Set) {
    return "Set{" + [...actual].map(formatResult).join(", ") + "}";
  }
  return String(actual);
}

async function executeCommand(client: Client, cmd: string[]): Promise<unknown> {
  const command = cmd[0].toLowerCase();
  const args = cmd.slice(1);

  try {
    switch (command) {
      case "ping":
        if (args.length > 0) {
          return await client.ping(args[0]);
        }
        return await client.ping();
      case "set":
        await client.set(args[0], args[1]);
        return "OK";
      case "get":
        return await client.get(args[0]);
      case "del":
        const delResult = await client.del(args);
        return delResult;
      case "exists":
        const existsResult = await client.exists(args);
        return existsResult;
      case "incr":
        return await client.incr(args[0]);
      case "decr":
        return await client.decr(args[0]);
      case "incrby":
        return await client.incrBy(args[0], parseInt(args[1]));
      case "decrby":
        return await client.decrBy(args[0], parseInt(args[1]));
      case "setnx":
        const setnxResult = await client.setNX(args[0], args[1]);
        return setnxResult ? 1 : 0;
      case "setex":
        await client.setEx(args[0], parseInt(args[1]), args[2]);
        return "OK";
      case "ttl":
        return await client.ttl(args[0]);
      case "pttl":
        return await client.pTTL(args[0]);
      case "type":
        return await client.type(args[0]);
      case "getset":
        return await client.getSet(args[0], args[1]);
      case "strlen":
        return await client.strLen(args[0]);
      case "substr":
        const str = await client.get(args[0]);
        if (str === null) return null;
        const start = parseInt(args[1]);
        const end = parseInt(args[2]);
        return str.substring(start, end + 1);
      case "mget":
        return await client.mGet(args);
      case "getdel":
        const getdelResult = await client.getDel(args[0]);
        return getdelResult;
      case "rename":
        await client.rename(args[0], args[1]);
        return "OK";
      case "renamenx":
        try {
          const result = await client.sendCommand(["RENAMENX", ...args]);
          return Number(result) === 1 ? 1 : 0;
        } catch {
          return 0;
        }
      case "dbsize":
        return await client.dbSize();
      case "select":
        await client.select(parseInt(args[0]));
        return "OK";
      case "flushdb":
        await client.flushDb();
        return "OK";
      case "flushall":
        await client.flushAll();
        return "OK";
      case "expire":
        const expireResult = await client.expire(args[0], parseInt(args[1]));
        return expireResult ? 1 : 0;
      case "move":
        // Return the server's own reply (1 moved, 0 not moved); an error reply
        // surfaces through the catch below like any other command's.
        return await client.sendCommand(["MOVE", ...args]);
      case "client":
        return await client.sendCommand(["CLIENT", ...args]);
      case "bgsave":
        await client.bgSave();
        return "OK";
      case "lpush":
        return await client.lPush(args[0], args.slice(1));
      case "rpush":
        return await client.rPush(args[0], args.slice(1));
      case "llen":
        return await client.lLen(args[0]);
      case "lrange":
        return await client.lRange(args[0], parseInt(args[1]), parseInt(args[2]));
      case "lindex":
        return await client.lIndex(args[0], parseInt(args[1]));
      case "lpop":
        return await client.lPop(args[0]);
      case "rpop":
        return await client.rPop(args[0]);
      case "lset":
        await client.lSet(args[0], parseInt(args[1]), args[2]);
        return "OK";
      case "lrem":
        return await client.lRem(args[0], parseInt(args[1]), args[2]);
      case "ltrim":
        await client.lTrim(args[0], parseInt(args[1]), parseInt(args[2]));
        return "OK";
      case "lpushx":
        try {
          const result = await client.sendCommand(["LPUSHX", args[0], args[1]]);
          return result;
        } catch {
          return 0;
        }
      case "rpushx":
        try {
          const result = await client.sendCommand(["RPUSHX", args[0], args[1]]);
          return result;
        } catch {
          return 0;
        }
      case "linsert":
        return await client.lInsert(args[0], args[1].toUpperCase() as "BEFORE" | "AFTER", args[2], args[3]);
      default:
        try {
          const result = await client.sendCommand([cmd[0], ...args]);
          return result;
        } catch (err) {
          return { error: String(err) };
        }
    }
  } catch (err) {
    return { error: String(err) };
  }
}

async function runTest(base: TestCase, resp: Protocol): Promise<TestResult> {
  const testCase = effectiveCase(base, resp);
  try {
    if (!client) throw new Error("Client not connected");

    // Run setup commands if any
    if (testCase.setup) {
      for (const setupCmd of testCase.setup) {
        await executeCommand(client, setupCmd);
      }
    }

    // Execute the main command
    const actual = await executeCommand(client, testCase.command);

    // Check if there was an error
    if (actual && typeof actual === "object" && "error" in actual) {
      // An error reply may be the expected outcome (e.g. EXECABORT on a dirty tx).
      // A top-level {"error": true} marker accepts any error reply without
      // pinning the exact message.
      if (isErrorMarker(testCase.expected) || testCase.type === "error") {
        const errorStr = (actual as { error: string }).error;
        const passed = isErrorMarker(testCase.expected)
          ? true
          : compareResult(errorStr, testCase.expected, testCase.type);
        return {
          name: testCase.name,
          version: testCase.version,
          status: passed ? "passed" : "failed",
          expected: testCase.expected,
          actual: null,
          error: passed ? undefined : `Expected error matching ${formatResult(testCase.expected)}, got ${errorStr}`,
        };
      }
      return {
        name: testCase.name,
        version: testCase.version,
        status: "failed",
        expected: testCase.expected,
        actual: null,
        error: (actual as { error: string }).error,
      };
    }

    // The command did not raise an error, but the test asserted one should appear.
    if (isErrorMarker(testCase.expected)) {
      return {
        name: testCase.name,
        version: testCase.version,
        status: "failed",
        expected: testCase.expected,
        actual: actual,
        error: `Expected an error reply (${formatResult(testCase.expected)}), got ${formatResult(actual)}`,
      };
    }

    // Compare result
    const passed = compareResult(actual, testCase.expected, testCase.type);

    return {
      name: testCase.name,
      version: testCase.version,
      status: passed ? "passed" : "failed",
      expected: testCase.expected,
      actual: actual,
      error: passed ? undefined : `Expected ${formatResult(testCase.expected)}, got ${formatResult(actual)}`,
    };
  } catch (err) {
    return {
      name: testCase.name,
      version: testCase.version,
      status: "failed",
      expected: testCase.expected,
      actual: null,
      error: String(err),
    };
  }
}

function printResultsTable(results: TestResult[]) {
  console.log("\n" + "=".repeat(80));
  console.log("TEST RESULTS");
  console.log("=".repeat(80));

  const passed = results.filter(r => r.status === "passed").length;
  const failed = results.filter(r => r.status === "failed").length;

  console.log(`Total: ${results.length} | Passed: ${passed} | Failed: ${failed}`);
  console.log("-".repeat(80));

  // Print header
  console.log(
    "Status | Version | Test Name".padEnd(80)
  );
  console.log("-".repeat(80));

  // Print each result
  for (const result of results) {
    const statusIcon = result.status === "passed" ? "✓" : "✗";
    const statusStr = result.status === "passed" ? "PASS" : "FAIL";
    const line = `${statusStr} | ${result.version} | ${result.name}`;
    console.log(line);

    if (result.error) {
      console.log(`  Error: ${result.error}`);
    }
  }

  console.log("=".repeat(80));
  console.log(`\nSummary: ${passed}/${results.length} tests passed`);

  if (failed > 0) {
    console.log("\nFailed tests:");
    for (const result of results.filter(r => r.status === "failed")) {
      console.log(`  - ${result.name} (v${result.version})`);
      if (result.error) {
        console.log(`    ${result.error}`);
      }
    }
  }
}

// Main test suite, run once per protocol.
for (const resp of PROTOCOLS) {
  Deno.test(`Redis Command Test Suite (RESP${resp})`, async () => {
    results.length = 0;
    client = await connect(resp);

    try {
      // The protocol the connection is really speaking is part of the test.
      const hello = await client.sendCommand(["HELLO"]);
      const proto = hello instanceof Map ? hello.get("proto") : (hello as unknown as unknown[])[5];
      assertEquals(proto, resp, "negotiated protocol");
      await client.flushAll();

      // Run all tests
      for (const testCase of testCases) {
        const result = await runTest(testCase, resp);
        results.push(result);

        // Assert for Deno test framework
        if (result.status === "failed") {
          console.error(`FAILED: ${result.name}`);
          if (result.error) {
            console.error(`  ${result.error}`);
          }
        }
      }

      // Print results table
      printResultsTable(results);

      // Assert all passed
      const failedResults = results.filter(r => r.status === "failed");
      assertEquals(
        failedResults.length,
        0,
        `Some tests failed: ${failedResults.map(r => r.name).join(", ")}`
      );
    } finally {
      if (client) {
        await client.quit();
      }
    }
  });
}
