import { describe, expect, it } from 'vitest'
import { ApiError } from '@/lib/api'
import { quotaRefreshDisplayError } from './useCredentialsTabData'
import { CREDENTIAL_PAGES_REFRESH_INTERVAL_MS } from './useCredentialPages'
import { buildQuotaCacheAuthIndexesKey, QUOTA_CACHE_REFRESH_INTERVAL_MS } from './useQuotaCache'
import { buildQuotaRefreshSubmissionUpdate, buildQuotaRefreshTaskErrorUpdate } from './useQuotaRefreshTasks'

describe('Credentials polling intervals', () => {
  it('keeps list data on a 5 minute refresh interval', () => {
    expect(CREDENTIAL_PAGES_REFRESH_INTERVAL_MS).toBe(5 * 60 * 1000)
  })

  it('keeps quota cache on a 1 minute refresh interval', () => {
    expect(QUOTA_CACHE_REFRESH_INTERVAL_MS).toBe(60 * 1000)
  })
})

describe('buildQuotaCacheAuthIndexesKey', () => {
  it('keeps equal auth index lists stable across array references', () => {
    expect(buildQuotaCacheAuthIndexesKey(['auth-1', 'auth-2'])).toBe(buildQuotaCacheAuthIndexesKey(['auth-1', 'auth-2']))
  })

  it('changes when auth index contents or order changes', () => {
    expect(buildQuotaCacheAuthIndexesKey(['auth-1', 'auth-2'])).not.toBe(buildQuotaCacheAuthIndexesKey(['auth-2', 'auth-1']))
  })
})

describe('quotaRefreshDisplayError', () => {
  it('turns refresh rejection codes into friendly messages', () => {
    expect(quotaRefreshDisplayError('duplicate')).toBe('Quota refresh is already running for this credential.')
    expect(quotaRefreshDisplayError('duplicate_request')).toBe('This credential was already included in the refresh request.')
    expect(quotaRefreshDisplayError('not_auth_file')).toBe('Quota refresh only supports local auth files.')
    expect(quotaRefreshDisplayError('unsupported')).toBe('Quota refresh is not supported for this credential type.')
  })

  it('keeps backend friendly refresh failures displayable', () => {
    expect(quotaRefreshDisplayError('Quota refresh timed out. Please try again later.')).toBe('Quota refresh timed out. Please try again later.')
  })
})

describe('buildQuotaRefreshSubmissionUpdate', () => {
  it('keeps duplicate refresh rejections in the polling queue', () => {
    const update = buildQuotaRefreshSubmissionUpdate({
      tasks: [{ authIndex: 'auth-1' }],
      rejected: [
        { authIndex: 'auth-2', error: 'duplicate' },
        { authIndex: 'auth-3', error: 'duplicate_request' },
      ],
      accepted: 1,
      skipped: 2,
      limit: 3,
    }, 'batch')

    expect(update.pendingTasks).toEqual([
      { authIndex: 'auth-1', source: 'batch' },
      { authIndex: 'auth-2', source: 'batch' },
    ])
    expect(update.stateUpdates['auth-2']).toEqual({ refreshStatus: 'queued', error: undefined })
    expect(update.stateUpdates['auth-3']).toEqual({ refreshStatus: 'failed', error: 'This credential was already included in the refresh request.' })
  })
})

describe('buildQuotaRefreshTaskErrorUpdate', () => {
  it('settles 401 polling failures and asks the page to re-authenticate', () => {
    let authRequiredCalls = 0

    const update = buildQuotaRefreshTaskErrorUpdate('auth-1', new ApiError('unauthorized', 401), () => {
      authRequiredCalls += 1
    })

    expect(authRequiredCalls).toBe(1)
    expect(update).toEqual({
      authIndex: 'auth-1',
      settled: true,
      stateUpdate: {
        refreshStatus: 'failed',
        error: 'Please sign in again to refresh quota.',
      },
    })
  })
})
