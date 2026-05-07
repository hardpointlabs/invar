import { createClient, type RedisClientType } from "redis";
import { assertEquals, assertExists } from "@std/assert";

interface TestCase {
  name: string;
  command: string[];
  expected: unknown;
  type: string;
  version: string;
  description: string;
  setup?: string[][];
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

let client: RedisClientType | null = null;
const results: TestResult[] = [];

function compareResult(actual: unknown, expected: unknown, type: string): boolean {
  switch (type) {
    case "simple_string":
    case "bulk_string":
      return actual === expected;
    case "bulk_string_startswith":
      return typeof actual === "string" && actual.startsWith(expected as string);
    case "integer":
      return Number(actual) === Number(expected);
    case "integer_gt":
      // expected is like "> -1" meaning actual should be > -1
      if (typeof expected !== "string" || !expected.startsWith("> ")) return false;
      const threshold = Number(expected.substring(2));
      return typeof actual === "number" && actual > threshold;
    case "null":
      return actual === null;
    case "null_or_bulk":
      return actual === null || actual === expected;
    case "array":
      if (!Array.isArray(expected) || !Array.isArray(actual)) return false;
      if (expected.length !== actual.length) return false;
      for (let i = 0; i < expected.length; i++) {
        if (expected[i] === null) {
          if (actual[i] !== null) return false;
        } else {
          if (actual[i] !== expected[i]) return false;
        }
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
  return String(actual);
}

async function executeCommand(client: RedisClientType, cmd: string[]): Promise<unknown> {
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
          return result === "OK" ? 1 : 0;
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
        try {
          await client.sendCommand(["MOVE", ...args]);
          return 1;
        } catch {
          return 0;
        }
      case "client":
        if (args[0].toLowerCase() === "id") {
          const result = await client.sendCommand(["CLIENT", ...args]);
          return result;
        } else if (args[0].toLowerCase() === "info") {
          const result = await client.sendCommand(["CLIENT", ...args]);
          return result;
        }
        return null;
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

async function runTest(testCase: TestCase): Promise<TestResult> {
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
      return {
        name: testCase.name,
        version: testCase.version,
        status: "failed",
        expected: testCase.expected,
        actual: null,
        error: (actual as { error: string }).error,
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

// Main test suite
Deno.test("Redis Command Test Suite", async () => {
  client = await createClient({
    url: "redis://localhost:6379",
    socket: {
      reconnectStrategy: false,
    },
  });

  try {
    await client.connect();

    // Run all tests
    for (const testCase of testCases) {
      const result = await runTest(testCase);
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
