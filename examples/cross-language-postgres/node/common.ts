import crypto from "node:crypto";

export const RUST_WORKFLOW = "rust_price_quote";
export const NODE_WORKFLOW = "node_fraud_check";
export const DEFAULT_SCHEMA = "journio_cross_language";
export const DEFAULT_RUST_QUEUE = "cross_language_rust_queue";
export const DEFAULT_NODE_QUEUE = "cross_language_node_queue";

export interface QuotePayload {
  sku: string;
  quantity: number;
}

export function loadJournio() {
  try {
    return require("@journio/sdk").Journio;
  } catch (_error) {
    return require("../../../bindings/nodejs/dist").Journio;
  }
}

export function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

export function config(listenQueues?: string[]) {
  return {
    name: "cross-language-node",
    systemDatabaseUrl: required("JOURNIO_SYSTEM_DATABASE_URL"),
    systemDatabaseSchemaName: process.env.JOURNIO_SYSTEM_DATABASE_SCHEMA || DEFAULT_SCHEMA,
    listenQueues
  };
}

export function rustQueue(): string {
  return process.env.JOURNIO_RUST_QUEUE || DEFAULT_RUST_QUEUE;
}

export function nodeQueue(): string {
  return process.env.JOURNIO_NODE_QUEUE || DEFAULT_NODE_QUEUE;
}

export function workflowID(prefix: string): string {
  return process.env.JOURNIO_WORKFLOW_ID || `${prefix}-${crypto.randomUUID()}`;
}

export function quotePayload(): QuotePayload {
  return {
    sku: process.env.JOURNIO_QUOTE_SKU || "starter-widget",
    quantity: Number(process.env.JOURNIO_QUOTE_QUANTITY || 3)
  };
}

export function print(value: unknown): void {
  console.log(JSON.stringify(value));
}

export async function configureAndLaunch(
  Journio: ReturnType<typeof loadJournio>,
  listenQueues?: string[]
): Promise<void> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      Journio.setConfig(config(listenQueues));
      await Journio.launch();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  throw lastError;
}
