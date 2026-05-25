package quota

import (
	"time"

	"cpa-usage-keeper/internal/repository"
	"cpa-usage-keeper/internal/timeutil"
)

type quotaUsageWindowKey struct {
	start time.Time
	end   *time.Time
}

func (s *Service) attachWindowUsageStats(authIndex string, response CheckResponse, now time.Time) CheckResponse {
	if len(response.Quota) == 0 {
		return response
	}
	// 同一个 quota response 可能同时包含 5h/weekly 或多个同窗口 row；按窗口去重后再查库，避免一行一次 DB 查询。
	statsByWindow := make(map[quotaUsageWindowKey]repository.UsageWindowStats)
	for index := range response.Quota {
		windowStart, windowEnd, ok := quotaRowUsageWindow(response.Quota[index], now)
		if !ok {
			continue
		}
		key := quotaUsageWindowKey{start: windowStart, end: windowEnd}
		stats, ok := statsByWindow[key]
		if !ok {
			var err error
			stats, err = repository.SumUsageWindowStatsByAuthIndex(s.db, authIndex, windowStart, windowEnd)
			if err != nil {
				continue
			}
			statsByWindow[key] = stats
		}
		tokens := stats.Tokens
		cost := stats.Cost
		response.Quota[index].WindowUsageTokens = &tokens
		response.Quota[index].WindowUsageCost = &cost
	}
	return response
}

func quotaRowUsageWindow(row QuotaRow, now time.Time) (time.Time, *time.Time, bool) {
	if row.ResetAt == "" || row.Window == nil || row.Window.Seconds == nil || *row.Window.Seconds <= 0 {
		return time.Time{}, nil, false
	}
	resetAt, err := time.Parse(time.RFC3339, row.ResetAt)
	if err != nil {
		return time.Time{}, nil, false
	}
	resetAt = timeutil.NormalizeStorageTime(resetAt)
	windowStart := resetAt.Add(-time.Duration(*row.Window.Seconds) * time.Second)
	// 当前窗口还没到 reset_at，不设置结束时间，让新写入事件只要晚于 start 都能被统计进当前窗口。
	if timeutil.NormalizeStorageTime(now).Before(resetAt) {
		return windowStart, nil, true
	}
	return windowStart, &resetAt, true
}
