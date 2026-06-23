import { describe, it, expect } from 'vitest'
import { formatTime, formatJson, statusClasses } from '~/composables/useApi'

describe('formatTime', () => {
  it('returns — for null/empty', () => {
    expect(formatTime(null)).toBe('—')
    expect(formatTime('')).toBe('—')
  })

  it('returns — for non-numeric input', () => {
    expect(formatTime('not-a-number')).toBe('—')
  })

  it('formats a recent timestamp as seconds ago', () => {
    const tenSecondsAgo = String(Date.now() - 10_000)
    const result = formatTime(tenSecondsAgo)
    expect(result).toMatch(/^\d+s ago$/)
  })

  it('formats an older timestamp as minutes/hours ago', () => {
    const fiveMinAgo = String(Date.now() - 5 * 60_000)
    expect(formatTime(fiveMinAgo)).toMatch(/^\d+m ago$/)

    const twoHoursAgo = String(Date.now() - 2 * 3_600_000)
    expect(formatTime(twoHoursAgo)).toMatch(/^\d+h ago$/)
  })
})

describe('formatJson', () => {
  it('returns empty string for empty input', () => {
    expect(formatJson('')).toBe('')
  })

  it('pretty-prints valid JSON', () => {
    const out = formatJson('{"b":2,"a":1}')
    // JSON.stringify with indent — keys preserved in insertion order.
    expect(out).toContain('"b": 2')
    expect(out).toContain('"a": 1')
    expect(out).toContain('\n')
  })

  it('passes through invalid JSON unchanged', () => {
    expect(formatJson('not json {')).toBe('not json {')
  })
})

describe('statusClasses', () => {
  it('maps known statuses to colored badge classes', () => {
    expect(statusClasses('SUCCESS')).toContain('text-success')
    expect(statusClasses('success')).toContain('text-success') // case-insensitive
    expect(statusClasses('ERROR')).toContain('text-error')
    expect(statusClasses('PENDING')).toContain('text-pending')
    expect(statusClasses('ENQUEUED')).toContain('text-enqueued')
    expect(statusClasses('CANCELLED')).toContain('text-cancelled')
    expect(statusClasses('DELAYED')).toContain('text-delayed')
  })

  it('marks pending workflows as animated (pulse)', () => {
    expect(statusClasses('PENDING')).toContain('animate-pulse-soft')
  })

  it('falls back to neutral slate classes for unknown status', () => {
    const cls = statusClasses('UNKNOWN')
    expect(cls).toContain('text-slate-300')
    expect(cls).not.toContain('animate-pulse-soft')
  })

  it('handles null-ish input', () => {
    const cls = statusClasses(undefined as any)
    expect(cls).toContain('text-slate-300')
  })
})
