/**
 * API composables for talking to the DBOS admin server.
 *
 * All fetches go through `useApiFetch`, which prepends the configured
 * `apiBase` and handles JSON. Polling composables (`usePolling`) re-fetch on
 * an interval so the UI reflects live workflow state.
 */

/** A workflow row as returned by the admin API. */
export interface Workflow {
  workflow_uuid: string
  status: string
  workflow_name: string
  authenticated_user: string | null
  assumed_role: string | null
  authenticated_roles: string[] | null
  output: string
  executor_id: string
  application_version: string
  application_id: string | null
  attempts: number
  queue_name: string | null
  timeout: number | null
  deduplication_id: string | null
  priority: number
  queue_partition_key: string | null
  input: string
  error: string
  created_at: string | null
  updated_at: string | null
  workflow_deadline_epoch_ms: string | null
  started_at: string | null
}

/** A single recorded step. */
export interface WorkflowStep {
  function_id: number
  function_name: string
  output: string
  error: string | null
  child_workflow_id: string | null
  started_at_epoch_ms?: number
  completed_at_epoch_ms?: number
}

export interface RegisteredWorkflow {
  name: string
}

export interface QueueMetadata {
  name: string
  concurrency?: number
  worker_concurrency?: number
  priority_enabled?: boolean
  partition_queue?: boolean
}

export interface IndexInfo {
  service: string
  app_name: string
  admin_server_port: number | null
}

/** List-workflows filter — mirrors Go's `listWorkflowsRequest`. */
export interface ListFilter {
  workflow_uuids?: string[]
  status?: string
  workflow_name?: string
  limit?: number
  offset?: number
  sort_desc?: boolean
  queue_name?: string
}

/** Base fetch wrapper. */
function useApiFetch() {
  const config = useRuntimeConfig()
  const base = config.public.apiBase.replace(/\/$/, '')

  async function request<T>(path: string, options: any = {}): Promise<T> {
    const url = path.startsWith('http') ? path : `${base}${path}`
    const res = await $fetch<T>(url, {
      headers: { 'Content-Type': 'application/json' },
      ...options,
    })
    return res as T
  }

  return {
    get: <T>(path: string) => request<T>(path, { method: 'GET' }),
    post: <T>(path: string, body?: any) =>
      request<T>(path, { method: 'POST', body: body ?? {} }),
  }
}

/** Reactive ref that re-fetches on an interval. Auto-stops on unmount. */
function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number = 2000,
) {
  const data = ref<T | null>(null)
  const error = ref<string | null>(null)
  const loading = ref(true)
  let timer: ReturnType<typeof setInterval> | null = null

  async function refresh() {
    try {
      data.value = await fetcher()
      error.value = null
    } catch (e: any) {
      error.value = e?.message ?? String(e)
    } finally {
      loading.value = false
    }
  }

  onMounted(() => {
    refresh()
    timer = setInterval(refresh, intervalMs)
  })

  onUnmounted(() => {
    if (timer) clearInterval(timer)
  })

  return { data, error, loading, refresh }
}

// ---- Domain-specific composables ------------------------------------------

/** List workflows with optional filters, polling every `intervalMs`. */
export function useWorkflows(filter: MaybeRef<ListFilter> = {}, intervalMs = 2000) {
  const api = useApiFetch()
  return usePolling(
    async () => {
      const f = unref(filter)
      return api.post<Workflow[]>('/workflows', f)
    },
    intervalMs,
  )
}

/** Single workflow by id, polling. */
export function useWorkflow(id: MaybeRef<string>, intervalMs = 2000) {
  const api = useApiFetch()
  const workflowId = computed(() => unref(id))
  return usePolling(async () => {
    const wid = workflowId.value
    return api.get<Workflow>(`/workflows/${wid}`)
  }, intervalMs)
}

/** Steps for a workflow, polling. */
export function useWorkflowSteps(id: MaybeRef<string>, intervalMs = 2000) {
  const api = useApiFetch()
  const workflowId = computed(() => unref(id))
  return usePolling(async () => {
    const wid = workflowId.value
    return api.get<WorkflowStep[]>(`/workflows/${wid}/steps`)
  }, intervalMs)
}

/** Registered workflows (in-process registry), polling. */
export function useRegisteredWorkflows(intervalMs = 5000) {
  const api = useApiFetch()
  return usePolling(
    async () => api.get<RegisteredWorkflow[]>('/workflows/registered'),
    intervalMs,
  )
}

/** Queue metadata, polling. */
export function useQueues(intervalMs = 5000) {
  const api = useApiFetch()
  return usePolling(
    async () => api.get<QueueMetadata[]>('/dbos-workflow-queues-metadata'),
    intervalMs,
  )
}

/** Service index info (one-shot). */
export function useIndex() {
  const api = useApiFetch()
  return usePolling(async () => api.get<IndexInfo>('/'), 10000)
}

// ---- Actions (imperative) -------------------------------------------------

export function useApi() {
  const api = useApiFetch()
  return {
    startWorkflow: (name: string, input: any = null, queueName?: string) =>
      api.post<{ workflow_id: string }>(`/workflows/${name}/start`, {
        input,
        queue_name: queueName,
      }),
    cancelWorkflow: (id: string) =>
      api.post(`/workflows/${id}/cancel`),
    resumeWorkflow: (id: string) =>
      api.post(`/workflows/${id}/resume`),
  }
}

// ---- Helpers --------------------------------------------------------------

/** Human-readable relative time from an epoch-ms string. */
export function formatTime(epochMsStr: string | null): string {
  if (!epochMsStr) return '—'
  const ms = Number(epochMsStr)
  if (!Number.isFinite(ms)) return '—'
  const date = new Date(ms)
  const diff = Date.now() - ms
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return date.toLocaleDateString()
}

/** Pretty-print a JSON string (the API returns input/output as JSON strings). */
export function formatJson(raw: string): string {
  if (!raw) return ''
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

/** Tailwind classes for a workflow status badge. */
export function statusClasses(status: string): string {
  switch (status?.toUpperCase()) {
    case 'SUCCESS':
      return 'bg-success/20 text-success border-success/40'
    case 'ERROR':
      return 'bg-error/20 text-error border-error/40'
    case 'PENDING':
      return 'bg-pending/20 text-pending border-pending/40 animate-pulse-soft'
    case 'ENQUEUED':
      return 'bg-enqueued/20 text-enqueued border-enqueued/40'
    case 'CANCELLED':
      return 'bg-cancelled/20 text-cancelled border-cancelled/40'
    case 'DELAYED':
      return 'bg-delayed/20 text-delayed border-delayed/40'
    default:
      return 'bg-slate-700 text-slate-300 border-slate-600'
  }
}
