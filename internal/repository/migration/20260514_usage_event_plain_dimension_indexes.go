package migration

import (
	"fmt"

	"gorm.io/gorm"
)

// addUsageEventPlainDimensionIndexesMigration 为已规范化入库的 usage_events 维度补普通索引。
func addUsageEventPlainDimensionIndexesMigration(tx *gorm.DB) error {
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS idx_usage_events_trim_model`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_source`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_auth_index`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_provider`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_auth_type`,
		`DROP INDEX IF EXISTS idx_usage_events_trim_api_group_key`,
		`DROP INDEX IF EXISTS idx_usage_events_auth_type_source_id`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_api_group_key ON usage_events(api_group_key)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_auth_index ON usage_events(auth_index)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_events_model ON usage_events(model)`,
	} {
		if err := tx.Exec(stmt).Error; err != nil {
			return fmt.Errorf("update usage event plain dimension index: %w", err)
		}
	}
	return nil
}
