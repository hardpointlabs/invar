import { createClient } from "redis";
import { assertEquals } from "@std/assert";

Deno.test(async function setTest() {
  const client = await createClient({url: "redis://localhost:6379"});
  await client.connect();
  await client.set("foo", "bar");
  const value = await client.get("foo");
  assertEquals(value, "bar");
  await client.disconnect();
});

Deno.test(async function deleteTest() {
  assertEquals(1 + 1, 3);
})
