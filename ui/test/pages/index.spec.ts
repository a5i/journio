import { describe, it, expect, beforeEach } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import IndexPage from '~/pages/index.vue'
import { mockApi, urlPath, flush } from '../helpers'
import { registeredWorkflows, workflows } from '../fixtures'

// Build a `$fetch` mock serving the dashboard's four endpoints.
function mountDashboard() {
  mockApi((url, opts) => {
    const path = urlPath(url)
    if (path === '/') return { service: 'dbos-admin', app_name: 'sqlite-demo', admin_server_port: 3001 }
    if (path === '/workflows/registered') return registeredWorkflows
    if (path === '/dbos-workflow-queues-metadata')
      return [{ name: 'orders', concurrency: 2 }]
    if (path === '/workflows' && opts.method === 'POST') return workflows
    throw new Error(`unmocked ${opts.method} ${path}`)
  })
  return mountSuspended(IndexPage)
}

describe('Dashboard page (index.vue)', () => {
  beforeEach(() => {
    // Fresh mock per test (mockApi re-stubs $fetch each call).
  })

  it('renders the Registered Workflows heading', async () => {
    const wrapper = await mountDashboard()
    await flush()
    expect(wrapper.text()).toContain('Registered Workflows')
  })

  it('lists registered workflows (excluding the internal debouncer)', async () => {
    const wrapper = await mountDashboard()
    await flush()
    const text = wrapper.text()
    expect(text).toContain('greet')
    expect(text).toContain('checkout')
    expect(text).toContain('flaky_task')
    // The internal workflow must be hidden.
    expect(text).not.toContain('__dbos_internal_debouncer_workflow')
  })

  it('shows summary stat counts', async () => {
    const wrapper = await mountDashboard()
    await flush()
    const text = wrapper.text()
    // Total = 3 (the fixtures), Success = 1, Errors = 1, In flight = 1.
    expect(text).toContain('Total')
    expect(text).toContain('Success')
  })

  it('renders workflow rows in the recent table', async () => {
    const wrapper = await mountDashboard()
    await flush()
    const text = wrapper.text()
    expect(text).toContain('wf-success-1')
    expect(text).toContain('wf-error-1')
    expect(text).toContain('checkout')
    expect(text).toContain('flaky_task')
  })

  it('renders the queue from metadata', async () => {
    const wrapper = await mountDashboard()
    await flush()
    expect(wrapper.text()).toContain('orders')
  })

  it('shows a Start button on each registered workflow card', async () => {
    const wrapper = await mountDashboard()
    await flush()
    // Three visible registered workflows → three Start buttons appear on hover.
    const startButtons = wrapper.findAll('button').filter((b) => b.text().includes('Start'))
    expect(startButtons.length).toBe(3)
  })
})
