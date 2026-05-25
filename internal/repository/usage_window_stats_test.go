package repository

import (
	"path/filepath"
	"testing"
	"time"

	"cpa-usage-keeper/internal/config"
	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/repository/dto"
)

func TestSumUsageWindowStatsByAuthIndexUsesAuthIndexAndWindow(t *testing.T) {
	db, err := OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-window-stats.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	if _, err := UpsertModelPriceSetting(db, dto.ModelPriceSettingInput{Model: "priced", PromptPricePer1M: 10, CompletionPricePer1M: 20, CachePricePer1M: 1}); err != nil {
		t.Fatalf("UpsertModelPriceSetting returned error: %v", err)
	}
	start := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	events := []entities.UsageEvent{
		{AuthIndex: "auth-1", Model: "priced", Timestamp: start.Add(10 * time.Minute), InputTokens: 1_000_000, OutputTokens: 500_000, CachedTokens: 200_000, TotalTokens: 1_500_000},
		{AuthIndex: "auth-2", Model: "priced", Timestamp: start.Add(20 * time.Minute), TotalTokens: 9_000_000},
		{AuthIndex: "auth-1", Model: "priced", Timestamp: end.Add(time.Minute), TotalTokens: 8_000_000},
	}
	if err := db.Create(&events).Error; err != nil {
		t.Fatalf("seed usage events: %v", err)
	}

	stats, err := SumUsageWindowStatsByAuthIndex(db, "auth-1", start, &end)
	if err != nil {
		t.Fatalf("SumUsageWindowStatsByAuthIndex returned error: %v", err)
	}
	if stats.Tokens != 1_500_000 {
		t.Fatalf("expected 1500000 tokens, got %d", stats.Tokens)
	}
	wantCost := 0.8*10 + 0.5*20 + 0.2*1
	if stats.Cost != wantCost {
		t.Fatalf("expected cost %.2f, got %.2f", wantCost, stats.Cost)
	}
}

func TestSumUsageWindowStatsByAuthIndexTreatsMissingPriceAsZeroCost(t *testing.T) {
	db, err := OpenDatabase(config.Config{SQLitePath: filepath.Join(t.TempDir(), "usage-window-stats-missing-price.db")})
	if err != nil {
		t.Fatalf("OpenDatabase returned error: %v", err)
	}
	closeTestDatabase(t, db)
	start := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	if err := db.Create(&entities.UsageEvent{AuthIndex: "auth-1", Model: "missing", Timestamp: start, InputTokens: 1_000_000, TotalTokens: 1_000_000}).Error; err != nil {
		t.Fatalf("seed usage event: %v", err)
	}
	stats, err := SumUsageWindowStatsByAuthIndex(db, "auth-1", start.Add(-time.Minute), nil)
	if err != nil {
		t.Fatalf("SumUsageWindowStatsByAuthIndex returned error: %v", err)
	}
	if stats.Tokens != 1_000_000 || stats.Cost != 0 {
		t.Fatalf("expected tokens with zero missing-price cost, got %+v", stats)
	}
}
