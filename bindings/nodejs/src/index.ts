import { AsyncLocalStorage } from "node:async_hooks";
import nativeBinding = require("../native");

type NativeBinding = Record<string, (...args: any[]) => any>;
const native = nativeBinding as NativeBinding;

export interface JournioConfig {
  name: string;
  systemDatabaseUrl: string;
  systemDatabaseSchemaName?: string;
  applicationVersion?: string;
  executorID?: string;
  runAdminServer?: boolean;
  adminPort?: number;
  listenQueues?: string[];
  logLevel?: string;
}

export interface StartWorkflowParams {
  workflowID?: string;
  queueName?: string;
  timeoutMS?: number;
  workflowAttributes?: Record<string, unknown>;
  enqueueOptions?: {
    deduplicationID?: string;
    priority?: number;
    queuePartitionKey?: string;
  };
}

export interface QueueOptions {
  concurrency?: number;
  workerConcurrency?: number;
  rateLimit?: {
    limitPerPeriod: number;
    periodSec: number;
  };
  priorityEnabled?: boolean;
  partitionQueue?: boolean;
}

export interface GetResultOptions {
  pollingIntervalMs?: number;
  timeoutMS?: number;
}

export interface RecvOptions {
  timeoutMS?: number;
}

export interface GetEventOptions {
  timeoutMS?: number;
}

export interface WorkflowHandle<R = unknown> {
  readonly workflowID: string;
  getResult(options?: GetResultOptions): Promise<R>;
  getStatus(): Promise<WorkflowStatus | null>;
  cancel(): Promise<boolean>;
  resume(options?: { queueName?: string }): Promise<boolean>;
  fork(options?: ForkWorkflowOptions): Promise<WorkflowHandle<R>>;
}

export interface WorkflowStatus {
  workflowID: string;
  status: string;
  workflowName: string;
  queueName?: string;
  input?: unknown[];
  output?: unknown;
  error?: unknown;
  executorId?: string;
  applicationVersion?: string;
  createdAt: number;
  updatedAt?: number;
  timeoutMS?: number;
  deadlineEpochMS?: number;
  deduplicationID?: string;
  priority: number;
  queuePartitionKey?: string;
  forkedFrom?: string;
  wasForkedFrom?: boolean;
}

export interface ListWorkflowsInput {
  workflowIDs?: string[];
  workflowName?: string;
  status?: string;
  workflow_id_prefix?: string;
  queueName?: string;
  queuesOnly?: boolean;
  limit?: number;
  offset?: number;
  sortDesc?: boolean;
}

export interface StepInfo {
  functionID: number;
  name: string;
  output?: unknown;
  error: Error | null;
  childWorkflowID: string | null;
}

export interface ForkWorkflowOptions {
  newWorkflowID?: string;
  startStep?: number;
  applicationVersion?: string;
  queueName?: string;
  queuePartitionKey?: string;
}

type WorkflowFunction<Args extends unknown[], Return> = ((...args: Args) => Promise<Return>) & {
  __journioWorkflowName: string;
};

type Callback = (...args: unknown[]) => unknown | Promise<unknown>;

interface ExecutionContext {
  workflowID: string;
  stepID?: number;
  applicationVersion: string;
  executorID: string;
}

interface DispatcherRequest {
  requestID: string;
  kind: "workflow" | "step";
  callbackID: string;
  workflowID: string;
  stepID?: number;
  applicationVersion: string;
  executorID: string;
  input: unknown;
}

const callbacks = new Map<string, Callback>();
const workflowNames = new WeakMap<Function, string>();
const executionContext = new AsyncLocalStorage<ExecutionContext>();
let callbackCounter = 0;
let configReady: Promise<void> | undefined;
let launched = false;
const registeredWorkflows: Array<{ name: string; callbackID: string; nativeRegistered: boolean }> = [];

function callNative<T>(name: string, ...args: unknown[]): T {
  const fn = native[name] ?? native[snakeCase(name)];
  if (typeof fn !== "function") {
    throw new Error(`Journio native method is unavailable: ${name}`);
  }
  return fn(...args) as T;
}

function snakeCase(name: string): string {
  return name.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

function nextCallbackID(prefix: string): string {
  callbackCounter += 1;
  return `${prefix}-${callbackCounter}`;
}

function currentContext(): ExecutionContext {
  const ctx = executionContext.getStore();
  if (!ctx) {
    throw new Error("This Journio method requires an active workflow context.");
  }
  return ctx;
}

function makeHandle<R>(workflowID: string): WorkflowHandle<R> {
  return {
    workflowID,
    async getResult(options?: GetResultOptions): Promise<R> {
      return callNative<Promise<R>>("nativeGetResult", workflowID, options?.timeoutMS);
    },
    async getStatus(): Promise<WorkflowStatus | null> {
      const status = await callNative<Promise<unknown>>("nativeGetStatus", workflowID);
      return status == null ? null : normalizeStatus(status);
    },
    async cancel(): Promise<boolean> {
      return callNative<Promise<boolean>>("nativeCancelWorkflow", workflowID);
    },
    async resume(options?: { queueName?: string }): Promise<boolean> {
      return callNative<Promise<boolean>>("nativeResumeWorkflow", workflowID, options?.queueName);
    },
    async fork(options?: ForkWorkflowOptions): Promise<WorkflowHandle<R>> {
      const id = await callNative<Promise<string>>("nativeForkWorkflow", workflowID, options ?? {});
      return makeHandle<R>(id);
    }
  };
}

function normalizeStatus(raw: any): WorkflowStatus {
  return {
    workflowID: raw.id,
    status: raw.status,
    workflowName: raw.name,
    queueName: raw.queue_name,
    input: raw.input,
    output: raw.output,
    error: raw.error,
    executorId: raw.executor_id,
    applicationVersion: raw.application_version,
    createdAt: Date.parse(raw.created_at),
    updatedAt: raw.updated_at ? Date.parse(raw.updated_at) : undefined,
    timeoutMS: durationToMs(raw.timeout),
    deadlineEpochMS: raw.deadline ? Date.parse(raw.deadline) : undefined,
    deduplicationID: raw.deduplication_id,
    priority: raw.priority ?? 0,
    queuePartitionKey: raw.queue_partition_key,
    forkedFrom: raw.forked_from,
    wasForkedFrom: raw.was_forked_from ?? false
  };
}

function normalizeStep(raw: any): StepInfo {
  let output: unknown = undefined;
  if (raw.output != null) {
    try {
      output = JSON.parse(raw.output);
    } catch {
      output = raw.output;
    }
  }
  return {
    functionID: raw.function_id,
    name: raw.function_name,
    output,
    error: raw.error ? new Error(raw.error) : null,
    childWorkflowID: raw.child_workflow_id ?? null
  };
}

function durationToMs(value: unknown): number | undefined {
  if (typeof value === "number") return value;
  if (typeof value === "object" && value !== null && "secs" in value) {
    const duration = value as { secs?: number; nanos?: number };
    return (duration.secs ?? 0) * 1000 + Math.floor((duration.nanos ?? 0) / 1_000_000);
  }
  return undefined;
}

function normalizeListFilter(input: ListWorkflowsInput = {}): Record<string, unknown> {
  return {
    workflowIDs: input.workflowIDs,
    workflowIdPrefix: input.workflow_id_prefix,
    workflowName: input.workflowName,
    status: input.status,
    queueName: input.queueName,
    queuesOnly: input.queuesOnly,
    limit: input.limit,
    offset: input.offset,
    sortDesc: input.sortDesc
  };
}

async function ensureConfigured(): Promise<void> {
  if (!configReady) {
    throw new Error("Journio.setConfig must be called before this method.");
  }
  await configReady;
}

function registerNativeWorkflow(entry: {
  name: string;
  callbackID: string;
  nativeRegistered: boolean;
}): void {
  if (!entry.nativeRegistered) {
    callNative<void>("nativeRegisterWorkflow", entry.name, entry.callbackID);
    entry.nativeRegistered = true;
  }
}

async function dispatcher(request: DispatcherRequest): Promise<void> {
  const callback = callbacks.get(request.callbackID);
  if (!callback) {
    callNative<void>(
      "nativeCompleteCallback",
      request.requestID,
      false,
      null,
      `Unknown Journio callback: ${request.callbackID}`
    );
    return;
  }

  const ctx: ExecutionContext = {
    workflowID: request.workflowID,
    stepID: request.stepID,
    applicationVersion: request.applicationVersion,
    executorID: request.executorID
  };

  try {
    const input = Array.isArray(request.input) ? request.input : [request.input];
    const result = await executionContext.run(ctx, async () => callback(...input));
    callNative<void>("nativeCompleteCallback", request.requestID, true, result ?? null, null);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    callNative<void>("nativeCompleteCallback", request.requestID, false, null, message);
  }
}

callNative<void>("nativeRegisterDispatcher", dispatcher);

export class Journio {
  static logger = console;

  static get workflowID(): string | undefined {
    return executionContext.getStore()?.workflowID;
  }

  static get stepID(): number | undefined {
    return executionContext.getStore()?.stepID;
  }

  static get applicationVersion(): string {
    return executionContext.getStore()?.applicationVersion ?? this.runtimeInfo().applicationVersion;
  }

  static get executorID(): string {
    return executionContext.getStore()?.executorID ?? this.runtimeInfo().executorID;
  }

  static setConfig(config: JournioConfig): void {
    launched = false;
    for (const workflow of registeredWorkflows) {
      workflow.nativeRegistered = false;
    }
    configReady = callNative<Promise<void>>("nativeSetConfig", {
      name: config.name,
      systemDatabaseUrl: config.systemDatabaseUrl,
      systemDatabaseSchemaName: config.systemDatabaseSchemaName,
      applicationVersion: config.applicationVersion,
      executorID: config.executorID,
      runAdminServer: config.runAdminServer,
      adminPort: config.adminPort,
      listenQueues: config.listenQueues
    });
  }

  static async launch(): Promise<void> {
    await ensureConfigured();
    for (const workflow of registeredWorkflows) {
      registerNativeWorkflow(workflow);
    }
    await callNative<Promise<void>>("nativeLaunch");
    launched = true;
  }

  static async shutdown(timeoutMS = 1000): Promise<void> {
    await callNative<Promise<void>>("nativeShutdown", timeoutMS);
  }

  static registerWorkflow<Args extends unknown[], Return>(
    fn: (...args: Args) => Promise<Return> | Return,
    options?: { name?: string }
  ): WorkflowFunction<Args, Return> {
    const name = options?.name ?? fn.name;
    if (!name) {
      throw new Error("Journio.registerWorkflow requires a workflow name.");
    }

    const callbackID = nextCallbackID("workflow");
    callbacks.set(callbackID, fn as Callback);
    const entry = { name, callbackID, nativeRegistered: false };
    registeredWorkflows.push(entry);
    if (launched) {
      registerNativeWorkflow(entry);
    }

    const wrapped = (async (...args: Args): Promise<Return> => {
      const workflowID = await callNative<Promise<string>>("nativeRunWorkflow", name, args);
      return makeHandle<Return>(workflowID).getResult();
    }) as WorkflowFunction<Args, Return>;

    wrapped.__journioWorkflowName = name;
    workflowNames.set(wrapped, name);
    return wrapped;
  }

  static async runStep<Return>(
    fn: () => Promise<Return> | Return,
    options?: { name?: string }
  ): Promise<Return> {
    const ctx = currentContext();
    const name = options?.name ?? (fn.name || "anonymous_step");
    const callbackID = nextCallbackID("step");
    callbacks.set(callbackID, fn as Callback);
    try {
      return await callNative<Promise<Return>>("nativeRunStep", ctx.workflowID, name, callbackID);
    } finally {
      callbacks.delete(callbackID);
    }
  }

  static startWorkflow<Args extends unknown[], Return>(
    workflow: WorkflowFunction<Args, Return>,
    params?: StartWorkflowParams
  ): (...args: Args) => Promise<WorkflowHandle<Awaited<Return>>> {
    const name = workflowNames.get(workflow) ?? workflow.__journioWorkflowName;
    if (!name) {
      throw new Error("Journio.startWorkflow requires a workflow returned by registerWorkflow.");
    }
    return async (...args: Args) => {
      const workflowID = await callNative<Promise<string>>(
        "nativeStartWorkflow",
        name,
        args,
        params ?? {}
      );
      return makeHandle<Awaited<Return>>(workflowID);
    };
  }

  static enqueueWorkflow<Args extends unknown[], Return>(
    workflowName: string,
    params: StartWorkflowParams & { queueName: string }
  ): (...args: Args) => Promise<WorkflowHandle<Awaited<Return>>> {
    return async (...args: Args) => {
      await ensureConfigured();
      const input = args.length === 1 ? args[0] : args;
      const workflowID = await callNative<Promise<string>>(
        "nativeStartWorkflow",
        workflowName,
        input,
        params
      );
      return makeHandle<Awaited<Return>>(workflowID);
    };
  }

  static retrieveWorkflow<R = unknown>(workflowID: string): WorkflowHandle<R> {
    return makeHandle<R>(workflowID);
  }

  static async sleep(durationMS: number): Promise<number> {
    return callNative<Promise<number>>("nativeSleep", currentContext().workflowID, durationMS);
  }

  static async send<T>(destinationID: string, message: T, topic?: string): Promise<void> {
    await callNative<Promise<void>>(
      "nativeSend",
      executionContext.getStore()?.workflowID,
      destinationID,
      message,
      topic
    );
  }

  static async recv<T>(topic?: string, options?: RecvOptions): Promise<T | null> {
    return callNative<Promise<T | null>>(
      "nativeRecv",
      currentContext().workflowID,
      topic,
      options?.timeoutMS
    );
  }

  static async setEvent<T>(key: string, value: T): Promise<void> {
    await callNative<Promise<void>>("nativeSetEvent", currentContext().workflowID, key, value);
  }

  static async getEvent<T>(
    workflowID: string,
    key: string,
    options?: GetEventOptions
  ): Promise<T | null> {
    return callNative<Promise<T | null>>(
      "nativeGetEvent",
      executionContext.getStore()?.workflowID,
      workflowID,
      key,
      options?.timeoutMS
    );
  }

  static async writeStream<T>(key: string, value: T): Promise<void> {
    await callNative<Promise<void>>("nativeWriteStream", currentContext().workflowID, key, value);
  }

  static async closeStream(key: string): Promise<void> {
    await callNative<Promise<void>>("nativeCloseStream", currentContext().workflowID, key);
  }

  static async *readStream<T>(workflowID: string, key: string): AsyncGenerator<T, void, unknown> {
    let fromOffset = 0;
    while (true) {
      const result = await callNative<Promise<{ values: T[]; closed: boolean }>>(
        "nativeReadStream",
        workflowID,
        key,
        false,
        fromOffset
      );
      for (const value of result.values) {
        fromOffset += 1;
        yield value;
      }
      if (result.closed) return;
    }
  }

  static async registerQueue(name: string, options?: QueueOptions): Promise<unknown> {
    return callNative<Promise<unknown>>("nativeRegisterQueue", name, options ?? {});
  }

  static async listWorkflows(input: ListWorkflowsInput = {}): Promise<WorkflowStatus[]> {
    const workflows = await callNative<Promise<unknown[]>>(
      "nativeListWorkflows",
      normalizeListFilter(input)
    );
    return workflows.map(normalizeStatus);
  }

  static async listQueuedWorkflows(input: ListWorkflowsInput = {}): Promise<WorkflowStatus[]> {
    return this.listWorkflows({ ...input, queuesOnly: true });
  }

  static async listWorkflowSteps(workflowID: string): Promise<StepInfo[] | undefined> {
    const steps = await callNative<Promise<unknown[]>>("nativeListWorkflowSteps", workflowID);
    return steps.map(normalizeStep);
  }

  static async cancelWorkflow(workflowID: string): Promise<void> {
    await callNative<Promise<boolean>>("nativeCancelWorkflow", workflowID);
  }

  static async resumeWorkflow<R = unknown>(
    workflowID: string,
    options?: { queueName?: string }
  ): Promise<WorkflowHandle<Awaited<R>>> {
    await callNative<Promise<boolean>>("nativeResumeWorkflow", workflowID, options?.queueName);
    return makeHandle<Awaited<R>>(workflowID);
  }

  static async forkWorkflow<R = unknown>(
    workflowID: string,
    startStep: number,
    options?: Omit<ForkWorkflowOptions, "startStep">
  ): Promise<WorkflowHandle<Awaited<R>>> {
    const forkedID = await callNative<Promise<string>>("nativeForkWorkflow", workflowID, {
      ...options,
      startStep
    });
    return makeHandle<Awaited<R>>(forkedID);
  }

  static async patch(patchName: string): Promise<boolean> {
    return callNative<Promise<boolean>>("nativePatch", currentContext().workflowID, patchName);
  }

  static async deprecatePatch(patchName: string): Promise<boolean> {
    return callNative<Promise<boolean>>(
      "nativeDeprecatePatch",
      currentContext().workflowID,
      patchName
    );
  }

  private static runtimeInfo(): { applicationVersion: string; executorID: string } {
    return callNative("nativeRuntimeInfo");
  }
}

export default Journio;
