import type { UsageLog } from '@/types'
import { BILLING_MODE_VIDEO, getDisplayBillingMode } from '@/utils/billingMode'
import { resolveUsageRequestType, type UsageRequestTypeLike } from '@/utils/usageRequestType'

type UsageOutputTpsRow = Partial<
  Pick<UsageLog, 'output_tokens' | 'duration_ms' | 'first_token_ms' | 'image_count' | 'image_output_tokens' | 'billing_mode'>
>

/**
 * Average output throughput (tokens/s) over the recorded request duration.
 *
 * Use the same window for streaming and non-streaming records. Output tokens
 * can include reasoning generated before the first visible token, and buffered
 * tool output can arrive only at completion. Subtracting first_token_ms would
 * divide the full output count by an unrelated, potentially tiny window.
 *
 * This includes waiting time and is not a measurement of model generation speed.
 * Image/video requests and records without valid output or duration are excluded.
 */
export const usageOutputTps = (row: UsageOutputTpsRow | null | undefined): number | null => {
  const outputTokens = row?.output_tokens ?? 0
  const durationMs = row?.duration_ms ?? 0
  if (!Number.isFinite(outputTokens) || !Number.isFinite(durationMs) || outputTokens < 2 || durationMs <= 0) {
    return null
  }
  if ((row?.image_count ?? 0) > 0 || (row?.image_output_tokens ?? 0) > 0 || row?.billing_mode === BILLING_MODE_VIDEO) {
    return null
  }
  const tps = outputTokens / (durationMs / 1000)
  return Number.isFinite(tps) ? tps : null
}

/** "30.8 t/s"；100 t/s 及以上取整。不可计算时返回 null，由调用方显示占位符。 */
export const formatUsageOutputTps = (row: UsageOutputTpsRow | null | undefined): string | null => {
  const tps = usageOutputTps(row)
  if (tps == null) return null
  return `${tps >= 100 ? Math.round(tps) : tps.toFixed(1)} t/s`
}

interface UsageTpsRow extends UsageRequestTypeLike {
  output_tokens?: number | null
  duration_ms?: number | null
  first_token_ms?: number | null
  billing_mode?: string | null
  image_count?: number
  image_output_tokens?: number | null
}

/** Average output rate; streaming excludes time to first token, not reasoning tokens. */
export function usageTps(row: UsageTpsRow): number | null {
  const type = resolveUsageRequestType(row)
  const mode = getDisplayBillingMode({ billing_mode: row.billing_mode, image_count: row.image_count ?? 0 })
  if (type === 'probe' || type === 'gwpool_degraded' || mode === 'image' || mode === 'video' ||
      (row.image_count ?? 0) > 0 || (row.image_output_tokens ?? 0) > 0) return null
  const tokens = row.output_tokens
  const duration = row.duration_ms
  if (tokens == null || !Number.isFinite(tokens) || tokens < 0 ||
      duration == null || !Number.isFinite(duration) || duration <= 0) return null

  const streaming = type === 'stream' || type === 'ws_v2' || (type !== 'sync' && row.stream === true)
  let elapsed = duration
  if (streaming) {
    const first = row.first_token_ms
    if (first == null || !Number.isFinite(first) || first < 0 || first >= duration) return null
    elapsed -= first
  }
  const rate = tokens / (elapsed / 1000)
  return Number.isFinite(rate) ? rate : null
}

export function formatUsageTps(row: UsageTpsRow): string {
  return usageTps(row)?.toFixed(2) ?? '—'
}
