package service

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"cpa-usage-keeper/internal/config"
	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/repository"
	"cpa-usage-keeper/internal/repository/dto"
	servicedto "cpa-usage-keeper/internal/service/dto"
	"gorm.io/gorm"
)

func TestUsageServiceGetUsageWithFilterDelegatesToFilteredSnapshot(t *testing.T) {
	db, err := repository.OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-service-filter.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	if _, _, err := repository.InsertUsageEvents(db, []entities.UsageEvent{
		{EventKey: "event-1", APIGroupKey: "provider-a", Model: "claude-sonnet", Timestamp: time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC), TotalTokens: 10},
		{EventKey: "event-2", APIGroupKey: "provider-a", Model: "claude-sonnet", Timestamp: time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC), TotalTokens: 20},
	}); err != nil {
		t.Fatalf("InsertUsageEvents returned error: %v", err)
	}

	start := time.Date(2026, 4, 16, 9, 30, 0, 0, time.UTC)
	end := time.Date(2026, 4, 16, 10, 30, 0, 0, time.UTC)
	provider := NewUsageService(db)
	snapshot, err := provider.GetUsageWithFilter(context.Background(), servicedto.UsageFilter{StartTime: &start, EndTime: &end})
	if err != nil {
		t.Fatalf("GetUsageWithFilter returned error: %v", err)
	}
	if snapshot.TotalRequests != 1 || snapshot.TotalTokens != 20 {
		t.Fatalf("expected service filter to keep only in-range event, got %+v", snapshot)
	}
}

func TestUsageServiceGetUsageOverviewDelegatesToFilteredOverview(t *testing.T) {
	previousLocal := time.Local
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	t.Cleanup(func() { time.Local = previousLocal })
	time.Local = location

	db, err := repository.OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-service-overview.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	if _, err := repository.UpsertModelPriceSetting(db, dto.ModelPriceSettingInput{
		Model:                "claude-sonnet",
		PromptPricePer1M:     3,
		CompletionPricePer1M: 15,
		CachePricePer1M:      0.3,
	}); err != nil {
		t.Fatalf("UpsertModelPriceSetting returned error: %v", err)
	}
	if _, _, err := repository.InsertUsageEvents(db, []entities.UsageEvent{
		{EventKey: "event-1", APIGroupKey: "provider-a", Model: "claude-sonnet", Timestamp: time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC), InputTokens: 1000, OutputTokens: 500, CachedTokens: 100, ReasoningTokens: 50, TotalTokens: 1650},
		{EventKey: "event-2", APIGroupKey: "provider-a", Model: "claude-sonnet", Timestamp: time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC), InputTokens: 500, OutputTokens: 250, CachedTokens: 0, ReasoningTokens: 25, TotalTokens: 775},
	}); err != nil {
		t.Fatalf("InsertUsageEvents returned error: %v", err)
	}
	if err := repository.AggregateUsageOverviewStats(context.Background(), db, time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("AggregateUsageOverviewStats returned error: %v", err)
	}

	start := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 16, 23, 59, 59, 0, time.UTC)
	provider := NewUsageService(db)
	overview, err := provider.GetUsageOverview(context.Background(), servicedto.UsageFilter{Range: "24h", StartTime: &start, EndTime: &end})
	if err != nil {
		t.Fatalf("GetUsageOverview returned error: %v", err)
	}
	if overview.Summary.RequestCount != 2 || overview.Summary.TokenCount != 2425 {
		t.Fatalf("expected overview summary counts, got %+v", overview.Summary)
	}
	if overview.Summary.WindowMinutes != 1440 {
		t.Fatalf("expected 24h overview to use exact 1440 minute window, got %+v", overview.Summary)
	}
	if overview.Series.Requests["2026-04-16T17:00:00+08:00"] != 1 || overview.Series.Requests["2026-04-16T18:00:00+08:00"] != 1 {
		t.Fatalf("expected hourly request series values, got %+v", overview.Series)
	}
	if math.Abs(overview.Series.Cost["2026-04-16T17:00:00+08:00"]-0.01023) > 0.000000001 || math.Abs(overview.Series.Cost["2026-04-16T18:00:00+08:00"]-0.00525) > 0.000000001 {
		t.Fatalf("expected hourly cost series values, got %+v", overview.Series)
	}
}

func TestUsageServiceResolvesAPIKeyIDForUsageQueries(t *testing.T) {
	db, err := repository.OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-service-api-key-filter.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	if err := repository.SyncCPAAPIKeys(db, []string{"sk-target-key", "sk-other-key"}, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("SyncCPAAPIKeys returned error: %v", err)
	}
	activeKeys, err := repository.ListActiveCPAAPIKeys(db)
	if err != nil {
		t.Fatalf("ListActiveCPAAPIKeys returned error: %v", err)
	}
	var targetID string
	for _, key := range activeKeys {
		if key.APIKey == "sk-target-key" {
			targetID = strconv.FormatInt(key.ID, 10)
		}
	}
	if targetID == "" {
		t.Fatalf("expected synced target API key")
	}
	if _, _, err := repository.InsertUsageEvents(db, []entities.UsageEvent{
		{EventKey: "target-1", APIGroupKey: "sk-target-key", Model: "claude-sonnet", Timestamp: time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC), TotalTokens: 10},
		{EventKey: "target-2", APIGroupKey: "sk-target-key", Model: "claude-opus", Timestamp: time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC), TotalTokens: 20},
		{EventKey: "other-1", APIGroupKey: "sk-other-key", Model: "claude-other", Timestamp: time.Date(2026, 4, 16, 11, 0, 0, 0, time.UTC), TotalTokens: 300},
	}); err != nil {
		t.Fatalf("InsertUsageEvents returned error: %v", err)
	}

	if err := repository.AggregateUsageOverviewStats(context.Background(), db, time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("AggregateUsageOverviewStats returned error: %v", err)
	}

	start := time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 16, 11, 0, 0, 0, time.UTC)
	provider := NewUsageService(db)
	overview, err := provider.GetUsageOverview(context.Background(), servicedto.UsageFilter{APIKeyID: targetID, Range: "custom", StartTime: &start, EndTime: &end})
	if err != nil {
		t.Fatalf("GetUsageOverview returned error: %v", err)
	}
	if overview.Summary.RequestCount != 2 || overview.Summary.TokenCount != 30 {
		t.Fatalf("expected overview to use resolved API key, got %+v", overview.Summary)
	}
	analysis, err := provider.GetUsageAnalysis(context.Background(), servicedto.UsageFilter{APIKeyID: targetID})
	if err != nil {
		t.Fatalf("GetUsageAnalysis returned error: %v", err)
	}
	if len(analysis.APIs) != 1 || analysis.APIs[0].APIKey != "sk-target-key" || analysis.APIs[0].TotalRequests != 2 {
		t.Fatalf("expected analysis to use resolved API key, got %+v", analysis.APIs)
	}
	events, err := provider.ListUsageEvents(context.Background(), servicedto.UsageFilter{APIKeyID: targetID, Page: 1, PageSize: 100, Limit: 100})
	if err != nil {
		t.Fatalf("ListUsageEvents returned error: %v", err)
	}
	if events.TotalCount != 2 || len(events.Events) != 2 {
		t.Fatalf("expected events to use resolved API key, got %+v", events)
	}
}

func TestUsageServiceRejectsInvalidAPIKeyID(t *testing.T) {
	db, err := repository.OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-service-invalid-api-key-id.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	provider := NewUsageService(db)

	_, err = provider.ListUsageEvents(context.Background(), servicedto.UsageFilter{APIKeyID: "not-an-id", Page: 1, PageSize: 100, Limit: 100})
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("expected ErrInvalidID, got %v", err)
	}
}

func TestUsageServiceRejectsDeletedAPIKeyID(t *testing.T) {
	db, err := repository.OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-service-deleted-api-key-id.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	if err := repository.SyncCPAAPIKeys(db, []string{"sk-deleted-key"}, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("SyncCPAAPIKeys returned error: %v", err)
	}
	activeKeys, err := repository.ListActiveCPAAPIKeys(db)
	if err != nil {
		t.Fatalf("ListActiveCPAAPIKeys returned error: %v", err)
	}
	if len(activeKeys) != 1 {
		t.Fatalf("expected one active key, got %+v", activeKeys)
	}
	if err := db.Model(&entities.CPAAPIKey{}).Where("id = ?", activeKeys[0].ID).Update("is_deleted", true).Error; err != nil {
		t.Fatalf("mark api key deleted: %v", err)
	}
	provider := NewUsageService(db)

	_, err = provider.GetUsageOverview(context.Background(), servicedto.UsageFilter{APIKeyID: strconv.FormatInt(activeKeys[0].ID, 10)})
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected deleted key to return record not found, got %v", err)
	}
}
