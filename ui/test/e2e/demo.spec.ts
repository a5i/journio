import { describe, test, expect } from 'vitest'
import { setup, createPage, url } from '@nuxt/test-utils/e2e'

// True end-to-end test: starts the Nuxt dev server, opens a browser, and
// exercises the real UI against the sqlite-demo admin API.
//
// ⚠ This suite is GATED behind `E2E_ENABLED=1` because the browser build path
// (`@nuxt/test-utils` e2e) is sensitive to the exact Vite / @vitejs/plugin-vue
// versions installed. Enable it only in an environment known to be compatible.
//
// Prerequisites:
//   1. The Rust demo backend running:  cargo run -p sqlite-demo   (:3001)
//   2. A playwright browser:            npx playwright-core install chromium
//   3. A compatible Vite/plugin-vue set (see README "Troubleshooting").
//
// Run:  E2E_ENABLED=1 npm run test:e2e
//
// When disabled (default), the suite skips gracefully with documentation.

const API_BASE = 'http://localhost:3001'
const E2E_ENABLED = process.env.E2E_ENABLED === '1'

// Probe the backend at module-eval time (top-level await) so `describe.skipIf`
// below sees the resolved value.
let backendUp = false
if (E2E_ENABLED) {
  try {
    const res = await fetch(`${API_BASE}/journio-healthz`)
    backendUp = res.ok
  } catch {
    backendUp = false
  }
}

// `setup` registers Nuxt's beforeAll/afterAll hooks — must be called at the
// top level. It starts the Nuxt dev server on a random port; the UI reaches
// the Rust backend via runtimeConfig.apiBase.
if (E2E_ENABLED && backendUp) {
  await setup({ browser: true })
}

const e2eReady = E2E_ENABLED && backendUp

describe.skipIf(!e2eReady)('Demo E2E', () => {
  test('dashboard loads and shows registered workflows', async () => {
    const page = await createPage()
    await page.goto(url('/'))

    await page.waitForSelector('text=Journio Console', { timeout: 20_000 })

    // Registered workflows from the demo backend.
    await page.waitForSelector('text=greet')
    await page.waitForSelector('text=checkout')
    await page.waitForSelector('text=flaky_task')
  })

  test('recent-workflows table shows seeded history', async () => {
    const page = await createPage()
    await page.goto(url('/'))
    await page.waitForSelector('text=Recent Workflows')

    // The seeded checkout workflows appear in the table.
    await page.waitForSelector('text=seed-checkout-1')
  })

  test('workflow detail page renders the step timeline', async () => {
    // A seeded checkout workflow has three recorded steps.
    const page = await createPage()
    await page.goto(url('/workflows/seed-checkout-1'))

    await page.waitForSelector('text=validate_order', { timeout: 15_000 })
    await page.waitForSelector('text=charge_card')
    await page.waitForSelector('text=ship_order')

    // And the workflow name + status.
    await page.waitForSelector('text=checkout')
  })

  test('starting a greet workflow navigates to its detail page', async () => {
    const page = await createPage()
    await page.goto(url('/'))
    await page.waitForSelector('text=greet')

    // Open the start modal for "greet".
    await page.click('div:has(> .text-blue-300:text("greet")) button:has-text("Start")')

    await page.waitForSelector('text=Start: greet', { timeout: 5_000 })
    await page.fill('textarea', '"E2E"')
    await page.click('button:has-text("Start workflow")')

    // Should navigate to /workflows/<uuid>.
    await page.waitForURL(/\/workflows\/[0-9a-f-]+/, { timeout: 10_000 })

    // The detail page shows the workflow name.
    await page.waitForSelector('text=greet', { timeout: 5_000 })
  })
})

// Document the skip so `vitest` reports a passing test, not "no tests found".
describe.skipIf(e2eReady)('Demo E2E (skipped)', () => {
  test('documents the skip', () => {
    const reason = !E2E_ENABLED
      ? 'E2E_ENABLED is not set'
      : !backendUp
        ? 'backend not running (start: cargo run -p sqlite-demo)'
        : 'unknown'
    // eslint-disable-next-line no-console
    console.warn(`\n  ℹ E2E tests skipped — ${reason}.\n`)
    expect(true).toBe(true)
  })
})
