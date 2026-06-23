<script setup lang="ts">
import { useIndex } from '~/composables/useApi'

const { data: index } = useIndex()
</script>

<template>
  <div class="min-h-screen">
    <!-- Top nav -->
    <header class="sticky top-0 z-10 border-b border-slate-800 bg-slate-900/80 backdrop-blur">
      <div class="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
        <NuxtLink to="/" class="flex items-center gap-2 font-semibold">
          <span class="flex h-8 w-8 items-center justify-center rounded-lg bg-blue-600 text-white">D</span>
          <span>DBOS Console</span>
        </NuxtLink>
        <div class="flex items-center gap-3 text-sm text-slate-400">
          <span v-if="index?.app_name" class="rounded-md bg-slate-800 px-2 py-1">
            {{ index.app_name }}
          </span>
          <span class="flex items-center gap-1.5">
            <span class="h-2 w-2 rounded-full bg-emerald-500" />
            live
          </span>
        </div>
      </div>
    </header>

    <main class="mx-auto max-w-6xl px-4 py-6">
      <slot />
    </main>
  </div>
</template>
