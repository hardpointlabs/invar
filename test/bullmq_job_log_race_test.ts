// Reproduces BullMQ issue #4682: race condition in job.log() causing
// unique constraint violations when log() is called rapidly.
// Validates that Invar avoids this by using server-side index assignment
// over the Redis wire protocol.
import { Queue, Worker } from "bullmq";

const connection = { host: "127.0.0.1", port: 6379 };
const LOG_LINES = 20;
const JOB_COUNT = 25;
const CONCURRENCY = 5;
const TIMEOUT_MS = 30_000;

Deno.test({
  name: "Validate job.log() race condition (BullMQ issue #4682)",
  async fn() {
    const queueName = `job-log-race-${Date.now()}`;
    const queue = new Queue(queueName, { connection });
    const worker = new Worker(
      queueName,
      async (job) => {
        for (let i = 0; i < LOG_LINES; i++) {
          await job.log(`line ${i}`);
        }
        return { ok: true };
      },
      { connection, concurrency: CONCURRENCY },
    );

    const errors: Error[] = [];
    worker.on("error", (err) => errors.push(err));

    const completed = new Map<string, string[]>();
    const failed: { id: string; error: Error }[] = [];

    const allDone = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`timed out after ${TIMEOUT_MS}ms`)),
        TIMEOUT_MS,
      );

      let settled = 0;

      worker.on("completed", async (job) => {
        const logs = await queue.getJobLogs(job.id!);
        completed.set(job.id!, logs.logs);
        if (++settled === JOB_COUNT) {
          clearTimeout(timer);
          resolve();
        }
      });

      worker.on("failed", (job, err) => {
        failed.push({ id: job?.id ?? "unknown", error: err });
        clearTimeout(timer);
        reject(err);
      });
    });

    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add(`log-job-${i}`, { iteration: i });
    }

    await allDone;

    if (errors.length > 0) {
      throw new Error(
        `worker emitted ${errors.length} error(s): ${errors.map((e) => e.message).join(", ")}`,
      );
    }

    if (failed.length > 0) {
      throw new Error(
        `${failed.length} job(s) failed: ${failed.map((f) => `${f.id}: ${f.error.message}`).join(", ")}`,
      );
    }

    if (completed.size !== JOB_COUNT) {
      throw new Error(
        `expected ${JOB_COUNT} completed jobs, got ${completed.size}`,
      );
    }

    for (const [id, logs] of completed) {
      if (logs.length !== LOG_LINES) {
        throw new Error(
          `job ${id}: expected ${LOG_LINES} log lines, got ${logs.length}`,
        );
      }
    }
  },
  sanitizeResources: false,
  sanitizeOps: false,
});
