import { defineVitestConfig } from '@nuxt/test-utils/config'

// E2E config — spins up the Nuxt server and drives a real browser via
// playwright. Requires the sqlite-demo backend running on :3001 and a
// playwright browser install. Run with `npm run test:e2e`.
export default defineVitestConfig({
  test: {
    // E2E tests use setup() which manages its own environment; we don't want
    // the per-file nuxt environment here.
    environment: 'node',
    include: ['test/e2e/**/*.spec.ts'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
  },
})
