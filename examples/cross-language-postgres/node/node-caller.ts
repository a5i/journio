import {
  RUST_WORKFLOW,
  configureAndLaunch,
  loadJournio,
  print,
  quotePayload,
  rustQueue,
  workflowID
} from "./common";

interface RustQuoteResult {
  engine: "rust";
  sku: string;
  quantity: number;
  totalCents: number;
}

const Journio = loadJournio();

async function main(): Promise<void> {
  const queue = rustQueue();
  const id = workflowID("node-calls-rust");
  await configureAndLaunch(Journio);

  const handle = await Journio.enqueueWorkflow(RUST_WORKFLOW, {
    workflowID: id,
    queueName: queue
  })(quotePayload());

  print({
    event: "started",
    workflowID: handle.workflowID,
    queue,
    workflow: RUST_WORKFLOW
  });

  const result = (await handle.getResult({ timeoutMS: 20_000 })) as RustQuoteResult;
  print({
    event: "result",
    workflowID: handle.workflowID,
    result
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
