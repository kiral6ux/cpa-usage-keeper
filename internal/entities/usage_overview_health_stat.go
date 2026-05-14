package entities

import "time"

// UsageOverviewHealthStat 是 Overview service health 最近窗口的预聚合统计。
type UsageOverviewHealthStat struct {
	ID           int64     `gorm:"primaryKey"`
	BucketStart  time.Time `gorm:"serializer:storageTime;not null;uniqueIndex:uniq_usage_overview_health_stats_bucket_span_api,priority:1;index:idx_usage_overview_health_stats_bucket_start;index:idx_usage_overview_health_stats_api_bucket_span,priority:2"`
	SpanSeconds  int64     `gorm:"not null;uniqueIndex:uniq_usage_overview_health_stats_bucket_span_api,priority:2;index:idx_usage_overview_health_stats_api_bucket_span,priority:3"`
	APIGroupKey  string    `gorm:"not null;uniqueIndex:uniq_usage_overview_health_stats_bucket_span_api,priority:3;index:idx_usage_overview_health_stats_api_bucket_span,priority:1"`
	SuccessCount int64     `gorm:"not null;default:0"`
	FailureCount int64     `gorm:"not null;default:0"`
	CreatedAt    time.Time `gorm:"serializer:storageTime;not null"`
	UpdatedAt    time.Time `gorm:"serializer:storageTime;not null"`
}
