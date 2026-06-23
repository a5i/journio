import { describe, it, expect } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import StatusBadge from '~/components/StatusBadge.vue'

describe('StatusBadge', () => {
  it('renders the status uppercased', async () => {
    const wrapper = await mountSuspended(StatusBadge, { props: { status: 'success' } })
    expect(wrapper.text()).toContain('SUCCESS')
  })

  it('applies success-colored classes for SUCCESS', async () => {
    const wrapper = await mountSuspended(StatusBadge, {
      props: { status: 'SUCCESS' },
    })
    const span = wrapper.find('span')
    expect(span.classes().join(' ')).toContain('text-success')
  })

  it('applies error-colored classes for ERROR', async () => {
    const wrapper = await mountSuspended(StatusBadge, {
      props: { status: 'ERROR' },
    })
    expect(wrapper.find('span').classes().join(' ')).toContain('text-error')
  })

  it('renders UNKNOWN for missing status', async () => {
    const wrapper = await mountSuspended(StatusBadge, {
      props: { status: '' },
    })
    expect(wrapper.text()).toContain('UNKNOWN')
  })

  it('renders the status dot indicator', async () => {
    const wrapper = await mountSuspended(StatusBadge, {
      props: { status: 'PENDING' },
    })
    // The inner dot span.
    expect(wrapper.findAll('span').length).toBeGreaterThanOrEqual(2)
  })
})
