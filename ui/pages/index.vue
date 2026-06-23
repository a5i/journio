<script setup lang="ts">
import {
  useRegisteredWorkflows,
  useWorkflows,
  useQueues,
  useApi,
  formatTime,
} from '~/composables/useApi'

const api = useApi()

const { data: registered } = useRegisteredWorkflows()
const { data: workflows, refresh } = useWorkflows({ sort_desc: true, limit: 50 })
const { data: queues } = useQueues()

// Hide the internal debouncer workflow from the registry list.
const visibleRegistered = computed(() =>
  (registered.value ?? []).filter((w) => !w.name.startsWith('__')),
)

// Stats for the summary cards.
const stats = computed(() => {
  const all = workflows.value ?? []
  const by = (s: string) => all.filter((w) => w.status?.toUpperCase() === s).length
  return {
    total: all.length,
    success: by('SUCCESS'),
    error: by('ERROR'),
    active: by('PENDING') + by('ENQUEUED') + by('DELAYED'),
  }
})

// Start-workflow modal state.
const showStart = ref(false)
const selectedName = ref<string | null>(null)
const startInput = ref('')
const startQueue = ref('')
const startError = ref<string | null>(null)
const starting = ref(false)

function openStart(name: string) {
  selectedName.value = name
  startInput.value = ''
  startQueue.value = ''
  startError.value = null
  showStart.value = true
}

// A per-workflow placeholder hinting at the expected input shape.
const inputPlaceholder = computed(() => {
  switch (selectedName.value) {
    case 'greet':
      return '"World"'
    case 'checkout':
      return '{"item":"Widget","quantity":2,"customer":"alice"}'
    case 'flaky_task':
      return '4  (odd numbers fail)'
    case 'long_running':
      return '3  (number of steps)'
    default:
      return '{"key": "value"}'
  }
})

async function doStart() {
  if (!selectedName.value) return
  starting.value = true
  startError.value = null
  try {
    let input: any = null
    if (startInput.value.trim()) {
      try {
        input = JSON.parse(startInput.value)
      } catch {
        input = startInput.value
      }
    }
    const res = await api.startWorkflow(
      selectedName.value,
      input,
      startQueue.value || undefined,
    )
    showStart.value = false
    await refresh()
    await navigateTo(`/workflows/${res.workflow_id}`)
  } catch (e: any) {
    // The admin API returns JSON {"message": "..."} on 4xx/5xx. `$fetch`
    // exposes it as `error.data` (already parsed). Handle both the object
    // shape and a raw string fallback.
    const data = e?.data
    startError.value =
      (typeof data === 'string' ? data : data?.message) ??
      e?.message ??
      String(e)
  } finally {
    starting.value = false
  }
}
</script>

<template>
  <div class="space-y-8">
    <!-- Summary cards -->
    <section class="grid grid-cols-2 gap-4 sm:grid-cols-4">
      <div class="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <p class="text-sm text-slate-400">Total</p>
        <p class="text-2xl font-bold">{{ stats.total }}</p>
      </div>
      <div class="rounded-lg border border-emerald-900/50 bg-emerald-950/30 p-4">
        <p class="text-sm text-emerald-400">Success</p>
        <p class="text-2xl font-bold text-emerald-300">{{ stats.success }}</p>
      </div>
      <div class="rounded-lg border border-red-900/50 bg-red-950/30 p-4">
        <p class="text-sm text-red-400">Errors</p>
        <p class="text-2xl font-bold text-red-300">{{ stats.error }}</p>
      </div>
      <div class="rounded-lg border border-blue-900/50 bg-blue-950/30 p-4">
        <p class="text-sm text-blue-400">In flight</p>
        <p class="text-2xl font-bold text-blue-300">{{ stats.active }}</p>
      </div>
    </section>

    <!-- Registered workflows -->
    <section>
      <h2 class="mb-3 text-lg font-semibold">Registered Workflows</h2>
      <div v-if="!visibleRegistered.length" class="text-sm text-slate-500">
        No workflows registered.
      </div>
      <div v-else class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <div
          v-for="wf in visibleRegistered"
          :key="wf.name"
          class="group rounded-lg border border-slate-800 bg-slate-900 p-4 transition hover:border-slate-600"
        >
          <div class="flex items-start justify-between">
            <div>
              <p class="font-mono text-sm font-medium text-blue-300">{{ wf.name }}</p>
              <p class="mt-1 text-xs text-slate-500">registered in-process</p>
            </div>
            <button
              class="rounded-md bg-blue-600 px-3 py-1 text-xs font-medium text-white opacity-0 transition hover:bg-blue-500 group-hover:opacity-100"
              @click="openStart(wf.name)"
            >
              Start
            </button>
          </div>
        </div>
      </div>
    </section>

    <!-- Queues -->
    <section v-if="queues && queues.length">
      <h2 class="mb-3 text-lg font-semibold">Queues</h2>
      <div class="flex flex-wrap gap-2">
        <div
          v-for="q in queues"
          :key="q.name"
          class="rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-sm"
        >
          <span class="font-mono text-purple-300">{{ q.name }}</span>
          <span v-if="q.concurrency" class="ml-2 text-xs text-slate-500">
            concurrency {{ q.concurrency }}
          </span>
        </div>
      </div>
    </section>

    <!-- Recent workflows -->
    <section>
      <div class="mb-3 flex items-center justify-between">
        <h2 class="text-lg font-semibold">Recent Workflows</h2>
        <button
          class="rounded-md border border-slate-700 px-2 py-1 text-xs text-slate-400 hover:bg-slate-800"
          @click="refresh()"
        >
          refresh
        </button>
      </div>

      <div class="overflow-hidden rounded-lg border border-slate-800">
        <table class="w-full text-sm">
          <thead class="bg-slate-900 text-xs uppercase text-slate-500">
            <tr>
              <th class="px-4 py-2 text-left">Status</th>
              <th class="px-4 py-2 text-left">Workflow</th>
              <th class="px-4 py-2 text-left">ID</th>
              <th class="px-4 py-2 text-left">Queue</th>
              <th class="px-4 py-2 text-right">Created</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-800">
            <tr
              v-for="wf in workflows"
              :key="wf.workflow_uuid"
              class="cursor-pointer hover:bg-slate-800/50"
              @click="navigateTo(`/workflows/${wf.workflow_uuid}`)"
            >
              <td class="px-4 py-2">
                <StatusBadge :status="wf.status" />
              </td>
              <td class="px-4 py-2 font-mono text-blue-300">{{ wf.workflow_name }}</td>
              <td class="px-4 py-2 font-mono text-xs text-slate-400">
                {{ wf.workflow_uuid.slice(0, 12) }}…
              </td>
              <td class="px-4 py-2 text-xs text-slate-500">{{ wf.queue_name ?? '—' }}</td>
              <td class="px-4 py-2 text-right text-xs text-slate-500">
                {{ formatTime(wf.created_at) }}
              </td>
            </tr>
            <tr v-if="!workflows?.length">
              <td colspan="5" class="px-4 py-8 text-center text-slate-500">
                No workflows yet.
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- Start modal -->
    <div
      v-if="showStart"
      class="fixed inset-0 z-20 flex items-center justify-center bg-black/60 p-4"
      @click.self="showStart = false"
    >
      <div class="w-full max-w-md rounded-lg border border-slate-700 bg-slate-900 p-6 shadow-xl">
        <h3 class="mb-4 text-lg font-semibold">
          Start: <span class="font-mono text-blue-300">{{ selectedName }}</span>
        </h3>

        <label class="mb-1 block text-sm text-slate-400">Input (JSON, optional)</label>
        <p class="mb-2 text-xs text-slate-500">
          Leave empty to use the workflow's default. Examples —
          greet: <code class="text-slate-400">"World"</code>, checkout:
          <code class="text-slate-400">{"{\"item\":\"Widget\",\"quantity\":2,\"customer\":\"alice\"}"}</code>,
          flaky_task: <code class="text-slate-400">4</code> (odd fails).
        </p>
        <textarea
          v-model="startInput"
          rows="4"
          :placeholder="inputPlaceholder"
          class="mb-3 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-sm focus:border-blue-500 focus:outline-none"
        />

        <label class="mb-1 block text-sm text-slate-400">Queue (optional)</label>
        <input
          v-model="startQueue"
          placeholder="leave empty to run immediately"
          class="mb-4 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
        />

        <p v-if="startError" class="mb-3 rounded-md bg-red-950/50 px-3 py-2 text-sm text-red-300">
          {{ startError }}
        </p>

        <div class="flex justify-end gap-2">
          <button
            class="rounded-md border border-slate-600 px-4 py-2 text-sm hover:bg-slate-800"
            @click="showStart = false"
          >
            Cancel
          </button>
          <button
            class="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-500 disabled:opacity-50"
            :disabled="starting"
            @click="doStart"
          >
            {{ starting ? 'Starting…' : 'Start workflow' }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
