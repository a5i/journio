import { Journio } from "../src";

async function main() {
  Journio.setConfig({
    name: "journio-node-basic",
    systemDatabaseUrl: "sqlite://journio-node-basic.db"
  });

  const doubleNumber = Journio.registerWorkflow(async (value: number) => {
    return Journio.runStep(
      async () => {
        return value * 2;
      },
      { name: "double_number" }
    );
  }, { name: "double-number" });

  await Journio.launch();

  const handle = await Journio.startWorkflow(doubleNumber)(21);
  const result = await handle.getResult({ timeoutMS: 2_000 });
  console.log(`workflow ${handle.workflowID} => ${result}`);

  await Journio.shutdown();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
