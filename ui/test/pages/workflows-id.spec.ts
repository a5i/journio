import { describe, it, expect } from 'vitest'
import { mountSuspended, mockNuxtImport } from '@nuxt/test-utils/runtime'
import WorkflowDetail from '~/pages/workflows/[id].vue'
import { mockApi, urlPath, flush } from '../helpers'
import {
  workflows,
  checkoutSteps,
  failedFlakyWorkflow,
  failedFlakySteps,
} from '../fixtures'

// `useRoute` must return the id of the workflow under test. Since
// `mockNuxtImport` is hoisted/module-scoped, we route it through a mutable
// variable that each test sets before mounting.
let routeId = 'wf-success-1'
mockNuxtImport('useRoute', () => {
  return () => ({ params: { id: routeId }, query: {}, path: '/' })
})

// Helper: mount the detail page with a specific workflow + its steps served.
async function mountDetail(workflowId: string, workflow: any, steps: any[]) {
  routeId = workflowId
  mockApi((url, opts) => {
    const path = urlPath(url)
    if (path === `/workflows/${workflowId}`) return workflow
    if (path === `/workflows/${workflowId}/steps`) return steps
    throw new Error(`unmocked ${opts.method} ${path}`)
  })
  return mountSuspended(WorkflowDetail)
}

describe('Workflow detail page ([id].vue)', () => {
  it('renders the workflow name and status badge', async () => {
    const wrapper = await mountDetail('wf-success-1', workflows[0], checkoutSteps)
    await flush()
    const text = wrapper.text()
    expect(text).toContain('checkout')
    expect(text).toContain('SUCCESS')
  })

  it('renders each step in the timeline with its name', async () => {
    const wrapper = await mountDetail('wf-success-1', workflows[0], checkoutSteps)
    await flush()
    const text = wrapper.text()
    expect(text).toContain('validate_order')
    expect(text).toContain('charge_card')
    expect(text).toContain('ship_order')
  })

  it('renders step outputs', async () => {
    const wrapper = await mountDetail('wf-success-1', workflows[0], checkoutSteps)
    await flush()
    const text = wrapper.text()
    // The output JSON-string is pretty-printed by formatJson.
    expect(text).toContain('validated 2x Widget')
    expect(text).toContain('charged alice')
  })

  it('renders the error banner for a failed workflow', async () => {
    const wrapper = await mountDetail(
      'wf-error-1',
      failedFlakyWorkflow,
      failedFlakySteps,
    )
    await flush()
    const text = wrapper.text()
    expect(text).toContain('Error')
    expect(text).toContain('flaky task failed for seed 3 (odd)')
  })

  it('shows the workflow UUID', async () => {
    const wrapper = await mountDetail('wf-success-1', workflows[0], checkoutSteps)
    await flush()
    expect(wrapper.text()).toContain('wf-success-1')
  })

  it('renders input and output sections', async () => {
    const wrapper = await mountDetail('wf-success-1', workflows[0], checkoutSteps)
    await flush()
    const text = wrapper.text()
    expect(text).toContain('Input')
    expect(text).toContain('Output')
    // Pretty-printed input contains the item.
    expect(text).toContain('Widget')
  })

  it('shows the Cancel action for an active (PENDING) workflow', async () => {
    const pending = workflows[2] // status PENDING
    const wrapper = await mountDetail('wf-pending-1', pending, [])
    await flush()
    const cancel = wrapper.findAll('button').filter((b) => b.text().includes('Cancel'))
    expect(cancel.length).toBe(1)
  })

  it('shows the Resume action for a CANCELLED workflow', async () => {
    const cancelled = { ...workflows[0], status: 'CANCELLED' }
    const wrapper = await mountDetail('wf-success-1', cancelled, [])
    await flush()
    const resume = wrapper.findAll('button').filter((b) => b.text().includes('Resume'))
    expect(resume.length).toBe(1)
  })
})
