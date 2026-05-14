package entities

import "time"

// UsageOverviewAggregationCheckpoint 记录 Overview 增量聚合已处理到的 usage_events cursor。
type UsageOverviewAggregationCheckpoint struct {
	ID                         int64      `gorm:"primaryKey"`
	Name                       string     `gorm:"not null;uniqueIndex:uniq_usage_overview_aggregation_checkpoints_name"`
	LastAggregatedUsageEventID int64      `gorm:"not null;default:0"`
	StatsUpdatedAt             *time.Time `gorm:"serializer:storageTime"`
	CreatedAt                  time.Time  `gorm:"serializer:storageTime;not null"`
	UpdatedAt                  time.Time  `gorm:"serializer:storageTime;not null"`
}
