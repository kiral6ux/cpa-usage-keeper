import type { UsageTimeRange } from '@/lib/types';

export interface UsageRangeQuery {
  valid: boolean;
  range: UsageTimeRange;
  start?: string;
  end?: string;
}

export const normalizeUsageRange = (value: string): UsageTimeRange => (
  value === '4h' || value === '8h' || value === '12h' || value === '24h' || value === 'today' || value === 'yesterday' || value === '7d' || value === '30d' || value === 'custom'
    ? value
    : '8h'
);

const parseCustomDateParam = (value: string | undefined, endOfDay: boolean): { value: string; timestampMs: number } | undefined => {
  const trimmed = value?.trim();
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimmed ?? '');
  if (!match || !trimmed) return undefined;
  const [, year, month, day] = match;
  const yearNumber = Number(year);
  const monthNumber = Number(month);
  const dayNumber = Number(day);
  const date = endOfDay
    ? new Date(yearNumber, monthNumber - 1, dayNumber, 23, 59, 59, 999)
    : new Date(yearNumber, monthNumber - 1, dayNumber, 0, 0, 0, 0);
  if (Number.isNaN(date.getTime())) return undefined;
  if (date.getFullYear() !== yearNumber || date.getMonth() !== monthNumber - 1 || date.getDate() !== dayNumber) return undefined;
  return { value: trimmed, timestampMs: date.getTime() };
};

export function buildUsageRangeQuery({ range, customStart, customEnd }: { range: string; customStart?: string; customEnd?: string }): UsageRangeQuery {
  const normalizedRange = normalizeUsageRange(range);
  if (normalizedRange !== 'custom') {
    return { valid: true, range: normalizedRange };
  }

  const start = parseCustomDateParam(customStart, false);
  const end = parseCustomDateParam(customEnd, true);
  if (!start || !end || start.timestampMs > end.timestampMs) {
    return { valid: false, range: normalizedRange };
  }
  return { valid: true, range: normalizedRange, start: start.value, end: end.value };
}
