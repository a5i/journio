import { afterEach, describe, expect, it } from "vitest";
import { Journio } from "../src";

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function sqliteUrl(prefix: string): string {
  return `sqlite://${uniqueName(prefix)}.db`;
}

describe("Journio Node binding", () => {
  afterEach(async () => {
    await Journio.shutdown().catch(() => undefined);
  });

  it("runs a workflow with a checkpointed step", async () => {
    Journio.setConfig({
      name: "journio-node-test-step",
      systemDatabaseUrl: sqliteUrl("journio-node-test-step")
    });

    const workflowName = uniqueName("double");
    const workflow = Journio.registerWorkflow(
      async (value: number) => {
        return Journio.runStep(async () => value * 2, { name: "double" });
      },
      { name: workflowName }
    );

    await Journio.launch();

    const handle = await Journio.startWorkflow(workflow)(21);
    await expect(handle.getResult({ timeoutMS: 5_000 })).resolves.toBe(42);

    const steps = await Journio.listWorkflowSteps(handle.workflowID);
    expect(steps?.map((step) => step.name)).toContain("double");
  });

  it("publishes and reads workflow events", async () => {
    Journio.setConfig({
      name: "journio-node-test-event",
      systemDatabaseUrl: sqliteUrl("journio-node-test-event")
    });

    const workflowName = uniqueName("eventer");
    const workflow = Journio.registerWorkflow(
      async (name: string) => {
        const greeting = await Journio.runStep(async () => `Hello, ${name}!`, {
          name: "greet"
        });
        await Journio.setEvent("greeting", greeting);
        return greeting;
      },
      { name: workflowName }
    );

    await Journio.launch();

    const handle = await Journio.startWorkflow(workflow, {
      workflowID: `eventer-${Date.now()}`
    })("Journio");

    await expect(handle.getResult({ timeoutMS: 5_000 })).resolves.toBe("Hello, Journio!");
    await expect(
      Journio.getEvent(handle.workflowID, "greeting", { timeoutMS: 1_000 })
    ).resolves.toBe("Hello, Journio!");
  });

  it("supports direct workflow invocation and exposes workflow context", async () => {
    Journio.setConfig({
      name: "journio-node-test-context",
      systemDatabaseUrl: sqliteUrl("journio-node-test-context"),
      applicationVersion: "node-test-version",
      executorID: "node-test-executor"
    });

    const workflow = Journio.registerWorkflow(
      async (value: number) => {
        const workflowID = Journio.workflowID;
        const step = await Journio.runStep(
          async () => ({
            stepID: Journio.stepID,
            workflowID: Journio.workflowID,
            applicationVersion: Journio.applicationVersion,
            executorID: Journio.executorID
          }),
          { name: "capture_context" }
        );

        return {
          value,
          workflowID,
          step
        };
      },
      { name: uniqueName("context") }
    );

    await Journio.launch();

    const result = await workflow(7);
    expect(result.value).toBe(7);
    expect(result.workflowID).toEqual(expect.any(String));
    expect(result.step).toMatchObject({
      stepID: 1,
      workflowID: result.workflowID,
      applicationVersion: "node-test-version",
      executorID: "node-test-executor"
    });
  });

  it("sends and receives workflow messages", async () => {
    Journio.setConfig({
      name: "journio-node-test-messages",
      systemDatabaseUrl: sqliteUrl("journio-node-test-messages")
    });

    const workflow = Journio.registerWorkflow(
      async () => {
        const message = await Journio.recv<{ status: string }>("payment", {
          timeoutMS: 5_000
        });
        return message?.status;
      },
      { name: uniqueName("receiver") }
    );

    await Journio.launch();

    const handle = await Journio.startWorkflow(workflow)();
    await Journio.send(handle.workflowID, { status: "paid" }, "payment");

    await expect(handle.getResult({ timeoutMS: 5_000 })).resolves.toBe("paid");
  });

  it("writes, closes, and reads durable streams", async () => {
    Journio.setConfig({
      name: "journio-node-test-streams",
      systemDatabaseUrl: sqliteUrl("journio-node-test-streams")
    });

    const workflow = Journio.registerWorkflow(
      async () => {
        await Journio.writeStream("updates", { step: 1 });
        await Journio.writeStream("updates", { step: 2 });
        await Journio.closeStream("updates");
        return "done";
      },
      { name: uniqueName("streamer") }
    );

    await Journio.launch();

    const handle = await Journio.startWorkflow(workflow)();
    await expect(handle.getResult({ timeoutMS: 5_000 })).resolves.toBe("done");

    const values: Array<{ step: number }> = [];
    for await (const value of Journio.readStream<{ step: number }>(handle.workflowID, "updates")) {
      values.push(value);
    }

    expect(values).toEqual([{ step: 1 }, { step: 2 }]);
  });

  it("runs queued workflows and exposes queue metadata in status/list calls", async () => {
    Journio.setConfig({
      name: "journio-node-test-queue",
      systemDatabaseUrl: sqliteUrl("journio-node-test-queue")
    });

    const workflowName = uniqueName("queued-task");
    const queueName = uniqueName("jobs");
    const workflow = Journio.registerWorkflow(
      async (value: number) => {
        return Journio.runStep(async () => value + 1, { name: "increment" });
      },
      { name: workflowName }
    );

    await Journio.launch();
    await Journio.registerQueue(queueName, {
      concurrency: 1,
      priorityEnabled: true
    });

    const handle = await Journio.startWorkflow(workflow, {
      queueName,
      enqueueOptions: { priority: 5 }
    })(41);

    await expect(handle.getResult({ timeoutMS: 5_000 })).resolves.toBe(42);

    const status = await handle.getStatus();
    expect(status).toMatchObject({
      workflowID: handle.workflowID,
      workflowName,
      queueName,
      status: "SUCCESS",
      priority: 5
    });

    const listed = await Journio.listWorkflows({ workflowName });
    expect(listed.map((workflow) => workflow.workflowID)).toContain(handle.workflowID);
  });

  it("retrieves handles and lists recorded workflow steps", async () => {
    Journio.setConfig({
      name: "journio-node-test-retrieve",
      systemDatabaseUrl: sqliteUrl("journio-node-test-retrieve")
    });

    const workflow = Journio.registerWorkflow(
      async (value: number) => {
        await Journio.runStep(async () => "first", { name: "first_step" });
        return Journio.runStep(async () => value * 3, { name: "triple" });
      },
      { name: uniqueName("retrievable") }
    );

    await Journio.launch();

    const handle = await Journio.startWorkflow(workflow)(14);
    const retrieved = Journio.retrieveWorkflow<number>(handle.workflowID);

    await expect(retrieved.getResult({ timeoutMS: 5_000 })).resolves.toBe(42);

    const steps = await Journio.listWorkflowSteps(handle.workflowID);
    expect(steps?.map((step) => step.name)).toEqual(["first_step", "triple"]);
    expect(steps?.find((step) => step.name === "triple")?.output).toBe(42);
  });
});
