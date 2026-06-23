<script setup lang="ts">
const props = defineProps<{ status: string }>()
import { statusClasses } from '~/composables/useApi'
</script>

<template>
  <span
    class="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium"
    :class="statusClasses(props.status)"
  >
    <span class="h-1.5 w-1.5 rounded-full bg-current" />
    {{ props.status ? props.status.toUpperCase() : 'UNKNOWN' }}
  </span>
</template>
