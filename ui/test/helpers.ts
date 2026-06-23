import { vi } from 'vitest'

// The configured admin API base (must match nuxt.config.ts runtimeConfig).
const API_BASE = 'http://localhost:3001'

/**
 * Strip the API base + query string from a URL, returning just the path.
 * `/workflows/registered` — used to route mock responses.
 */
export function urlPath(url: string): string {
  return url
    .replace(API_BASE, '')
    .replace(/^https?:\/\/[^/]+/, '')
    .replace(/\?.*$/, '')
}

/**
 * Install a `$fetch` mock backed by a plain handler function. The handler
 * receives the raw URL and the `$fetch` options (`{ method, body }`) and
 * returns the response payload (or throws).
 *
 * Returns the underlying `vi.fn` so tests can assert call counts / args.
 *
 * @example
 * mockApi((url, opts) => {
 *   const path = urlPath(url)
 *   if (path === '/workflows/registered') return [{ name: 'greet' }]
 *   throw new Error(`unmocked ${path}`)
 * })
 */
export function mockApi(
  handler: (url: string, opts: { method: string; body: any }) => any,
) {
  const fn = vi.fn(async (url: string, options: any = {}) => {
    const method = (options.method ?? 'GET').toUpperCase()
    return handler(url, { method, body: options.body })
  })
  vi.stubGlobal('$fetch', fn)
  return fn
}

/**
 * Resolve pending microtasks. The polling composables fire their first fetch
 * inside `onMounted` (fire-and-forget); one flush settles the promises.
 */
export const flushPromises = () => new Promise((resolve) => setTimeout(resolve, 0))

/** Flush a few times to be safe with chained async resolution. */
export async function flush(times = 3) {
  for (let i = 0; i < times; i++) {
    await flushPromises()
  }
}
