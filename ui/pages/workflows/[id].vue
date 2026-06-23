<script setup lang="ts">
import {
  useWorkflow,
  useWorkflowSteps,
  useApi,
  formatTime,
  formatJson,
} from '~/composables/useApi'

const route = useRoute()
const id = computed(() => String(route.params.id))

const { data: workflow, error, refresh } = useWorkflow(id)
const { data: steps } = useWorkflowSteps(id)
const api = useApi()

const actionError = ref<string | null>(null)
const acting = ref(false)

async function doAction(fn: () => Promise<any>) {
  acting.value = true
  actionError.value = null
  try {
    await fn()
    await refresh()
  } catch (e: any) {
    const data = e?.data
    actionError.value =
      (typeof data === 'string' ? data : data?.message) ??
      e?.message ??
      String(e)
  } finally {
    acting.value = false
  }
}

const isActive = computed(() => {
  const s = workflow.value?.status?.toUpperCase()
  return s === 'PENDING' || s === 'ENQUEUED' || s === 'DELAYED'
})
</script>

<template>
  <div class="space-y-6">
    <div class="flex items-center gap-3">
      <NuxtLink to="/" class="text-sm text-slate-400 hover:text-slate-200">← back</NuxtLink>
    </div>

    <!-- Error / not found -->
    <div v-if="error" class="rounded-lg border border-red-900/50 bg-red-950/30 p-6 text-center">
      <p class="text-red-300">Failed to load workflow</p>
      <p class="mt-1 text-sm text-slate-500">{{ error }}</p>
    </div>

    <template v-else-if="workflow">
      <!-- Header -->
      <section class="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <div class="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div class="mb-2 flex items-center gap-3">
              <StatusBadge :status="workflow.status" />
              <span class="font-mono text-lg text-blue-300">{{ workflow.workflow_name }}</span>
            </div>
            <p class="font-mono text-xs text-slate-500">{{ workflow.workflow_uuid }}</p>
          </div>

          <!-- Actions -->
          <div class="flex gap-2">
            <button
              v-if="isActive"
              :disabled="acting"
              class="rounded-md border border-red-800 bg-red-950/40 px-3 py-1.5 text-sm text-red-300 hover:bg-red-950 disabled:opacity-50"
              @click="doAction(() => api.cancelWorkflow(workflow!.workflow_uuid))"
            >
              Cancel
            </button>
            <button
              v-if="workflow.status?.toUpperCase() === 'CANCELLED'"
              :disabled="acting"
              class="rounded-md border border-emerald-800 bg-emerald-950/40 px-3 py-1.5 text-sm text-emerald-300 hover:bg-emerald-950 disabled:opacity-50"
              @click="doAction(() => api.resumeWorkflow(workflow!.workflow_uuid))"
            >
              Resume
            </button>
          </div>
        </div>

        <p v-if="actionError" class="mt-3 rounded-md bg-red-950/50 px-3 py-2 text-sm text-red-300">
          {{ actionError }}
        </p>

        <!-- Metadata grid -->
        <dl class="mt-5 grid grid-cols-2 gap-x-6 gap-y-3 text-sm sm:grid-cols-4">
          <div>
            <dt class="text-slate-500">Executor</dt>
            <dd class="font-mono text-slate-300">{{ workflow.executor_id || '—' }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">Queue</dt>
            <dd class="font-mono text-slate-300">{{ workflow.queue_name ?? '—' }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">Attempts</dt>
            <dd class="font-mono text-slate-300">{{ workflow.attempts }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">App version</dt>
            <dd class="font-mono text-slate-300">{{ workflow.application_version || '—' }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">Created</dt>
            <dd class="text-slate-300">{{ formatTime(workflow.created_at) }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">Updated</dt>
            <dd class="text-slate-300">{{ formatTime(workflow.updated_at) }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">Started</dt>
            <dd class="text-slate-300">{{ formatTime(workflow.started_at) }}</dd>
          </div>
          <div>
            <dt class="text-slate-500">Priority</dt>
            <dd class="font-mono text-slate-300">{{ workflow.priority }}</dd>
          </div>
        </dl>
      </section>

      <!-- Error banner -->
      <section
        v-if="workflow.error"
        class="rounded-lg border border-red-900/50 bg-red-950/30 p-4"
      >
        <h3 class="mb-1 flex items-center gap-2 text-sm font-semibold text-red-300">
          <span>⚠</span> Error
        </h3>
        <pre class="whitespace-pre-wrap font-mono text-sm text-red-200">{{ workflow.error }}</pre>
      </section>

      <!-- Steps timeline -->
      <section>
        <h2 class="mb-3 text-lg font-semibold">Steps</h2>
        <div v-if="!steps?.length" class="text-sm text-slate-500">
          No steps recorded yet.
        </div>
        <ol v-else class="space-y-2">
          <li
            v-for="step in steps"
            :key="step.function_id"
            class="flex gap-3 rounded-lg border border-slate-800 bg-slate-900 p-3"
          >
            <div class="flex flex-col items-center">
              <span
                class="flex h-7 w-7 items-center justify-center rounded-full text-xs font-bold"
                :class="step.error
                  ? 'bg-red-950 text-red-400'
                  : step.output
                    ? 'bg-emerald-950 text-emerald-400'
                    : 'bg-slate-800 text-slate-400'"
              >
                {{ step.error ? '✕' : step.output ? '✓' : step.function_id }}
              </span>
            </div>
            <div class="min-w-0 flex-1">
              <div class="flex items-center justify-between">
                <span class="font-mono text-sm text-blue-300">{{ step.function_name }}</span>
                <span class="text-xs text-slate-500">#{{ step.function_id }}</span>
              </div>
              <p v-if="step.error" class="mt-1 font-mono text-xs text-red-300">
                {{ step.error }}
              </p>
              <pre v-else-if="step.output" class="mt-1 overflow-x-auto font-mono text-xs text-slate-400">{{ formatJson(step.output) }}</pre>
              <p v-if="step.child_workflow_id" class="mt-1 text-xs text-purple-400">
                → child: <NuxtLink :to="`/workflows/${step.child_workflow_id}`" class="underline">
                  {{ step.child_workflow_id.slice(0, 12) }}…
                </NuxtLink>
              </p>
            </div>
          </li>
        </ol>
      </section>

      <!-- Input / Output -->
      <section class="grid gap-4 md:grid-cols-2">
        <div class="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <h3 class="mb-2 text-sm font-semibold text-slate-400">Input</h3>
          <pre class="overflow-x-auto font-mono text-xs text-slate-300">{{ formatJson(workflow.input) || '—' }}</pre>
        </div>
        <div class="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <h3 class="mb-2 text-sm font-semibold text-slate-400">Output</h3>
          <pre class="overflow-x-auto font-mono text-xs text-emerald-300">{{ formatJson(workflow.output) || '—' }}</pre>
        </div>
      </section>
    </template>
  </div>
</template>
