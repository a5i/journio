import { defineVitestConfig } from '@nuxt/test-utils/config'

// Unit + component + page-integration tests run in the Nuxt environment
// (happy-dom under the hood). True browser E2E tests live in test/e2e/** and
// are excluded from the default run — see `npm run test:e2e`.
export default defineVitestConfig({
  test: {
    environment: 'nuxt',
    setupFiles: ['./test/setup.ts'],
    include: ['test/**/*.spec.ts'],
    exclude: ['test/e2e/**', 'node_modules', '.output', '.nuxt', 'dist'],
  },
})
