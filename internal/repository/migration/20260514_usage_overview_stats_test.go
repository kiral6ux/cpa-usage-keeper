package migration

import (
	"path/filepath"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreateUsageOverviewStatsMigrationCreatesTablesAndIndexes(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(testSQLiteDSN(filepath.Join(t.TempDir(), "overview-stats.db"))), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeOpenedDatabase(t, db)

	if err := createUsageOverviewStatsMigration(db); err != nil {
		t.Fatalf("create usage overview stats: %v", err)
	}
	if err := createUsageOverviewStatsMigration(db); err != nil {
		t.Fatalf("create usage overview stats should be idempotent: %v", err)
	}

	for _, table := range []string{
		"usage_overview_hourly_stats",
		"usage_overview_daily_stats",
		"usage_overview_health_stats",
		"usage_overview_aggregation_checkpoints",
	} {
		if !db.Migrator().HasTable(table) {
			t.Fatalf("expected table %s to exist", table)
		}
	}

	for _, column := range []string{
		"id",
		"bucket_start",
		"api_group_key",
		"model",
		"request_count",
		"success_count",
		"failure_count",
		"input_tokens",
		"output_tokens",
		"reasoning_tokens",
		"cached_tokens",
		"cache_read_tokens",
		"cache_creation_tokens",
		"total_tokens",
		"created_at",
		"updated_at",
	} {
		if !db.Migrator().HasColumn("usage_overview_hourly_stats", column) {
			t.Fatalf("expected usage_overview_hourly_stats.%s column to exist", column)
		}
		if !db.Migrator().HasColumn("usage_overview_daily_stats", column) {
			t.Fatalf("expected usage_overview_daily_stats.%s column to exist", column)
		}
	}
	for _, column := range []string{"id", "bucket_start", "span_seconds", "api_group_key", "success_count", "failure_count", "created_at", "updated_at"} {
		if !db.Migrator().HasColumn("usage_overview_health_stats", column) {
			t.Fatalf("expected usage_overview_health_stats.%s column to exist", column)
		}
	}
	for _, column := range []string{"id", "name", "last_aggregated_usage_event_id", "stats_updated_at", "created_at", "updated_at"} {
		if !db.Migrator().HasColumn("usage_overview_aggregation_checkpoints", column) {
			t.Fatalf("expected usage_overview_aggregation_checkpoints.%s column to exist", column)
		}
	}

	for _, index := range []string{
		"uniq_usage_overview_hourly_stats_bucket_api_model",
		"idx_usage_overview_hourly_stats_bucket_start",
		"idx_usage_overview_hourly_stats_api_bucket",
		"idx_usage_overview_hourly_stats_api_model_bucket",
		"uniq_usage_overview_daily_stats_bucket_api_model",
		"idx_usage_overview_daily_stats_bucket_start",
		"idx_usage_overview_daily_stats_api_bucket",
		"idx_usage_overview_daily_stats_api_model_bucket",
		"uniq_usage_overview_health_stats_bucket_span_api",
		"idx_usage_overview_health_stats_bucket_start",
		"idx_usage_overview_health_stats_api_bucket_span",
		"uniq_usage_overview_aggregation_checkpoints_name",
	} {
		if !migrationSQLiteIndexExists(t, db, index) {
			t.Fatalf("expected index %s to exist", index)
		}
	}
}

func migrationSQLiteIndexExists(t *testing.T, db *gorm.DB, indexName string) bool {
	t.Helper()
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", indexName).Scan(&count).Error; err != nil {
		t.Fatalf("check sqlite index %s: %v", indexName, err)
	}
	return count == 1
}
