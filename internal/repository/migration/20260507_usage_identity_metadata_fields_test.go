package migration

import (
	"database/sql"
	"path/filepath"
	"testing"

	"cpa-usage-keeper/internal/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAddUsageIdentityMetadataFieldsMigrationAddsNullableColumns(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(testSQLiteDSN(filepath.Join(t.TempDir(), "legacy.db"))), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeOpenedDatabase(t, db)

	if err := db.Exec(`CREATE TABLE usage_identities (
		id integer PRIMARY KEY AUTOINCREMENT,
		name text,
		auth_type integer,
		auth_type_name text,
		identity text,
		type text,
		provider text,
		lookup_key text,
		total_requests integer,
		success_count integer,
		failure_count integer,
		input_tokens integer,
		output_tokens integer,
		reasoning_tokens integer,
		cached_tokens integer,
		total_tokens integer,
		last_aggregated_usage_event_id integer,
		first_used_at datetime,
		last_used_at datetime,
		stats_updated_at datetime,
		is_deleted numeric,
		created_at datetime,
		updated_at datetime,
		deleted_at datetime
	)`).Error; err != nil {
		t.Fatalf("create legacy usage_identities table: %v", err)
	}
	if err := db.Exec(`INSERT INTO usage_identities (name, auth_type, auth_type_name, identity, type, provider, lookup_key, is_deleted)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, "Legacy", models.UsageIdentityAuthTypeAuthFile, "oauth", "legacy-auth", "codex", "codex", "", false).Error; err != nil {
		t.Fatalf("seed legacy usage identity: %v", err)
	}

	if err := addUsageIdentityMetadataFieldsMigration(db); err != nil {
		t.Fatalf("add usage identity metadata fields: %v", err)
	}

	newColumns := []string{
		"prefix",
		"account_id",
		"active_start",
		"active_until",
		"plan_type",
		"limit_reached",
		"primary_window_used_percent",
		"primary_window_limit_seconds",
		"primary_window_reset_seconds",
		"primary_window_reset_at",
		"secondary_window_used_percent",
		"secondary_window_limit_seconds",
		"secondary_window_reset_seconds",
		"secondary_window_reset_at",
	}
	for _, column := range newColumns {
		if !db.Migrator().HasColumn(&models.UsageIdentity{}, column) {
			t.Fatalf("expected usage_identities.%s column to exist", column)
		}
	}

	var limitReached sql.NullBool
	var primaryUsed sql.NullInt64
	var primaryResetAt sql.NullTime
	err = db.Raw(`
		SELECT limit_reached, primary_window_used_percent, primary_window_reset_at
		FROM usage_identities
		WHERE identity = ?`, "legacy-auth").Row().Scan(&limitReached, &primaryUsed, &primaryResetAt)
	if err != nil {
		t.Fatalf("scan added nullable fields: %v", err)
	}
	if limitReached.Valid || primaryUsed.Valid || primaryResetAt.Valid {
		t.Fatalf("expected reserved fields to default NULL, got limit=%+v used=%+v reset=%+v", limitReached, primaryUsed, primaryResetAt)
	}
}
