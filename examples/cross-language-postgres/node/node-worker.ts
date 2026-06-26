import { NODE_WORKFLOW, configureAndLaunch, loadJournio, nodeQueue, print } from "./common";

interface FraudInput {
  orderId: string;
  amountCents: number;
}

const Journio = loadJournio();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntilTerminal(workflowID: string): Promise<void> {
  while (true) {
    try {
      const status = await Journio.retrieveWorkflow(workflowID).getStatus();
      if (
        status &&
        ["SUCCESS", "ERROR", "CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"].includes(
          status.status
        )
      ) {
        return;
      }
    } catch (_error) {
      // The caller may not have inserted the workflow row yet.
    }
    await sleep(100);
  }
}

async function main(): Promise<void> {
  const queue = nodeQueue();

  Journio.registerWorkflow(
    async ({ orderId, amountCents }: FraudInput) => {
      await Journio.setEvent("fraud_status", "normalizing");
      await Journio.writeStream("fraud_updates", {
        stage: "received",
        orderId,
        amountCents
      });

      const normalized = await Journio.runStep(
        async () => ({
          orderId: String(orderId).trim(),
          amountCents: Number(amountCents)
        }),
        { name: "node_normalize_order" }
      );

      await Journio.setEvent("fraud_status", "scoring");
      const riskScore = await Journio.runStep(
        async () => {
          const cents = normalized.amountCents;
          return Math.min(99, Math.floor(cents / 1000) + (normalized.orderId.length % 17));
        },
        { name: "node_score_risk" }
      );

      const approved = riskScore < 50;
      await Journio.writeStream("fraud_updates", {
        stage: "scored",
        riskScore,
        approved
      });
      await Journio.closeStream("fraud_updates");
      await Journio.setEvent("fraud_status", approved ? "approved" : "review");

      return {
        engine: "node",
        orderId: normalized.orderId,
        approved,
        riskScore
      };
    },
    { name: NODE_WORKFLOW }
  );

  await configureAndLaunch(Journio, [queue]);
  await Journio.registerQueue(queue, { concurrency: 1 });
  print({ event: "ready", worker: "node", queue, workflow: NODE_WORKFLOW });

  const exitAfter = process.env.JOURNIO_EXIT_AFTER_WORKFLOW_ID;
  if (exitAfter) {
    await waitUntilTerminal(exitAfter);
    print({ event: "observed-terminal", worker: "node", workflowID: exitAfter });
  } else {
    await new Promise(() => undefined);
  }

  await Journio.shutdown();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
