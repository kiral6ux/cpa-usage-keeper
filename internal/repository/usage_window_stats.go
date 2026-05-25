package repository

import (
	"fmt"
	"strings"
	"time"

	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/helper"
	"cpa-usage-keeper/internal/timeutil"

	"gorm.io/gorm"
)

type UsageWindowStats struct {
	Tokens int64
	Cost   float64
}

func SumUsageWindowStatsByAuthIndex(db *gorm.DB, authIndex string, start time.Time, end *time.Time) (UsageWindowStats, error) {
	if db == nil {
		return UsageWindowStats{}, fmt.Errorf("database is nil")
	}
	authIndex = strings.TrimSpace(authIndex)
	if authIndex == "" {
		return UsageWindowStats{}, fmt.Errorf("auth_index is required")
	}
	pricingByModel, err := loadPriceSettingsByModel(db)
	if err != nil {
		return UsageWindowStats{}, err
	}
	query := db.Model(&entities.UsageEvent{}).
		Select(usageEventProjectionColumns).
		Where("auth_index = ? AND timestamp >= ?", authIndex, timeutil.FormatStorageTime(start)).
		Order("timestamp asc")
	if end != nil {
		// 过期 quota 窗口必须使用半开结束时间，避免把新窗口事件累计进旧窗口缓存。
		query = query.Where("timestamp < ?", timeutil.FormatStorageTime(*end))
	}
	var rows []usageEventProjection
	if err := query.Find(&rows).Error; err != nil {
		return UsageWindowStats{}, fmt.Errorf("sum usage window stats by auth index: %w", err)
	}
	stats := UsageWindowStats{}
	for _, row := range rows {
		event := usageEventProjectionToEntity(row)
		stats.Tokens += event.TotalTokens
		pricing := pricingByModel[strings.TrimSpace(event.Model)]
		// quota 窗口展示按“部分成本”语义累计，缺价模型按 0 贡献，不影响其它已配置模型成本。
		stats.Cost += helper.CalculateUsageEventCost(event, pricing)
	}
	return stats, nil
}
