// BullMQ smoke test
import { Queue, Worker } from "bullmq";

const connection = { host: "127.0.0.1", port: 6379 };
const queueName = `smoke-${Date.now()}`;

Deno.test({name: "Validate basic BullMQ interaction"}, async () => {
  const queue = new Queue(queueName, { connection });
  const worker = new Worker(
    queueName,
    async (job) => {
      console.log(`processing job ${job.id} with data`, job.data);
      return { ok: true };
    },
    { connection }
  );
  
  const done = new Promise<void>((resolve, reject) => {
    worker.on("completed", (job) => {
      console.log(`job ${job.id} completed`);
      resolve();
    });
    worker.on("failed", (job, err) => {
      reject(new Error(`job ${job?.id} failed: ${err.message}`));
    });
  });
  
  await queue.add("smoke-job", { hello: "world" });
  console.log("job added, waiting for completion...");
  
  let timer: number | undefined;
  const timeout = new Promise<void>((_, reject) => {
    timer = setTimeout(() => reject(new Error("timed out waiting for job completion")), 5_000);
  });
  
  try {
    await Promise.race([done, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    await worker.close();
    await queue.close();
  }
});
