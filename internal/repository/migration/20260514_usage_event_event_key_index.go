package migration

import "gorm.io/gorm"

// removeUsageEventEventKeyUniqueIndexMigration 允许同一个 request_id/event_key 对应多条队列事件。
func removeUsageEventEventKeyUniqueIndexMigration(tx *gorm.DB) error {
	if !tx.Migrator().HasTable("usage_events") {
		return nil
	}
	if err := tx.Exec("DROP INDEX IF EXISTS uniq_usage_events_event_key").Error; err != nil {
		return err
	}
	return tx.Exec("CREATE INDEX IF NOT EXISTS idx_usage_events_event_key ON usage_events(event_key)").Error
}
