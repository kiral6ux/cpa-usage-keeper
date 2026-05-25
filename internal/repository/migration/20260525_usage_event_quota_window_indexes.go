package migration

import (
	"fmt"

	"gorm.io/gorm"
)

func addUsageEventQuotaWindowIndexesMigration(tx *gorm.DB) error {
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS idx_usage_events_trim_model`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_source`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_auth_index`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_provider`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_auth_type`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_api_group_key`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_model ON usage_events(model)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_source ON usage_events(source)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_auth_index ON usage_events(auth_index)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_provider ON usage_events(provider)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_auth_type ON usage_events(auth_type)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_api_group_key ON usage_events(api_group_key)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_auth_index_timestamp_id ON usage_events(auth_index, timestamp, id)`,
	} {
		// 新迁移只处理索引形态：删掉 TRIM 表达式索引并补普通字段索引，避免窗口统计查询走不到 auth_index+timestamp。
		if err := tx.Exec(stmt).Error; err != nil {
			return fmt.Errorf("update usage event quota window index: %w", err)
		}
	}
	return nil
}
