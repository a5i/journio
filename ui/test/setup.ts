import { vi } from 'vitest'

// Safety net: any test that forgets to mock `$fetch` will fail loudly instead
// of hitting the real admin API (which isn't running during `npm test`).
// Override per-test with `mockApi()` from `./helpers.ts`.
vi.stubGlobal(
  '$fetch',
  vi.fn(() => {
    throw new Error('$fetch was called without a mock — call mockApi() in this test')
  }),
)

// `navigateTo` is a Nuxt global; stub it so page tests that trigger
// navigation don't actually try to drive the router.
vi.stubGlobal('navigateTo', vi.fn())
