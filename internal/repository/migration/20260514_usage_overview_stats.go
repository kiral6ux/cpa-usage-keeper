package migration

import (
	"fmt"

	"cpa-usage-keeper/internal/entities"

	"gorm.io/gorm"
)

// createUsageOverviewStatsMigration 创建 Overview 查询使用的小时、天、health 和 checkpoint 增量表。
func createUsageOverviewStatsMigration(tx *gorm.DB) error {
	if err := tx.AutoMigrate(
		&entities.UsageOverviewHourlyStat{},
		&entities.UsageOverviewDailyStat{},
		&entities.UsageOverviewHealthStat{},
		&entities.UsageOverviewAggregationCheckpoint{},
	); err != nil {
		return fmt.Errorf("auto migrate usage overview stats: %w", err)
	}
	// 查询侧按时间窗口、API key 和模型组合读取 stats，因此这里显式补齐复合索引。
	for _, stmt := range []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS uniq_usage_overview_hourly_stats_bucket_api_model ON usage_overview_hourly_stats (bucket_start, api_group_key, model)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_hourly_stats_bucket_start ON usage_overview_hourly_stats (bucket_start)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_hourly_stats_api_bucket ON usage_overview_hourly_stats (api_group_key, bucket_start)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_hourly_stats_api_model_bucket ON usage_overview_hourly_stats (api_group_key, model, bucket_start)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uniq_usage_overview_daily_stats_bucket_api_model ON usage_overview_daily_stats (bucket_start, api_group_key, model)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_daily_stats_bucket_start ON usage_overview_daily_stats (bucket_start)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_daily_stats_api_bucket ON usage_overview_daily_stats (api_group_key, bucket_start)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_daily_stats_api_model_bucket ON usage_overview_daily_stats (api_group_key, model, bucket_start)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uniq_usage_overview_health_stats_bucket_span_api ON usage_overview_health_stats (bucket_start, span_seconds, api_group_key)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_health_stats_bucket_start ON usage_overview_health_stats (bucket_start)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_overview_health_stats_api_bucket_span ON usage_overview_health_stats (api_group_key, bucket_start, span_seconds)`,
	} {
		if err := tx.Exec(stmt).Error; err != nil {
			return fmt.Errorf("create usage overview stats index: %w", err)
		}
	}
	return nil
}
