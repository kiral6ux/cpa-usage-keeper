package migration

import (
	"fmt"

	"cpa-usage-keeper/internal/entities"

	"gorm.io/gorm"
)

// addUsageEventCacheTokenFieldsMigration 补齐 CPA Redis usage payload 里的 cache token 明细字段。
func addUsageEventCacheTokenFieldsMigration(tx *gorm.DB) error {
	if !tx.Migrator().HasTable(&entities.UsageEvent{}) {
		return nil
	}
	for _, column := range []string{"cache_read_tokens", "cache_creation_tokens"} {
		if tx.Migrator().HasColumn(&entities.UsageEvent{}, column) {
			continue
		}
		if err := tx.Exec(fmt.Sprintf("ALTER TABLE usage_events ADD COLUMN %s INTEGER NOT NULL DEFAULT 0", column)).Error; err != nil {
			return fmt.Errorf("add usage_events.%s column: %w", column, err)
		}
	}
	return nil
}
