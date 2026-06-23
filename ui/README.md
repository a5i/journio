# DBOS Console UI

A Nuxt 3 + Vue 3 + Tailwind dashboard for the DBOS admin API. Shows registered
workflows, execution history, step-level timelines, errors, and lets you start
/ cancel / resume workflows from the browser.

## Prerequisites

- Node.js 18+
- The SQLite demo app running (provides the admin API):

```sh
# from the repo root
cargo run -p sqlite-demo
# → API at http://localhost:3001
```

## Run

```sh
cd ui
npm install
npm run dev
# → http://localhost:3000
```

The UI polls the admin API every 2 seconds, so workflow state updates live.

## Features

- **Dashboard** (`/`) — summary stats, registered workflows (with Start buttons),
  queues, and a recent-workflows table.
- **Workflow detail** (`/workflows/[id]`) — status badge, metadata, full step
  timeline (with per-step output/errors), input/output JSON, and Cancel/Resume
  actions.
- **Live updates** — composables auto-poll on an interval; in-flight workflows
  pulse.

## Configuration

The admin API base URL defaults to `http://localhost:3001`. Override with:

```sh
NUXT_PUBLIC_API_BASE=http://my-host:3001 npm run dev
```

## Testing

The UI has three test layers:

```sh
npm test            # unit + component + page-integration (30 tests, no backend needed)
npm run test:watch  # same, in watch mode
npm run test:e2e    # browser E2E (gated — see below)
```

### Unit & component tests (`npm test`)

Fast tests that run in the Nuxt environment (happy-dom). No backend or
browser required — the admin API is mocked via `mockApi()`.

- `test/composables/useApi.spec.ts` — pure helpers (`formatTime`, `formatJson`,
  `statusClasses`).
- `test/components/StatusBadge.spec.ts` — the status badge renders the right
  colors/labels (caught a real bug: empty status now shows UNKNOWN).
- `test/pages/index.spec.ts` — the dashboard mounts with a mocked API and
  renders registered workflows, the recent-workflows table, queues, and stat
  counts.
- `test/pages/workflows-id.spec.ts` — the detail page renders the step
  timeline, error banner, input/output, and the Cancel/Resume actions.

### Browser E2E (`npm run test:e2e`)

Drives a real Chromium browser against the running demo stack. **Gated behind
`E2E_ENABLED=1`** because the browser build path is sensitive to the exact
Vite / `@vitejs/plugin-vue` versions:

```sh
# terminal 1 — the Rust backend
cargo run -p sqlite-demo

# terminal 2 — install a browser (once)
npx playwright-core install chromium

# run the E2E suite
E2E_ENABLED=1 npm run test:e2e
```

Without `E2E_ENABLED`, the suite skips gracefully with a diagnostic (so it's
safe to include in CI). See "Troubleshooting" if the Nuxt server build fails
with `MagicString is not a constructor`.

#### Troubleshooting

`MagicString is not a constructor` during `test:e2e` indicates a version
mismatch between `@vitejs/plugin-vue` and the Vite bundled inside
`@nuxt/vite-builder`. The dev server (`npm run dev`) is unaffected because it
uses the top-level Vite. If you hit this, align the versions, e.g. pin
`@vitejs/plugin-vue` to the major your Nuxt release expects.

## Architecture

```
composables/useApi.ts   # typed API client + polling composables
components/StatusBadge.vue
layouts/default.vue     # top nav with live health indicator
pages/index.vue         # dashboard
pages/workflows/[id].vue # detail view
```

The API surface matches `dbos-admin` (axum): `GET /workflows/registered`,
`POST /workflows`, `GET /workflows/{id}`, `GET /workflows/{id}/steps`,
`POST /workflows/{name}/start`, `POST /workflows/{id}/cancel|resume`.
