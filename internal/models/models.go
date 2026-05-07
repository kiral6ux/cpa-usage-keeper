package models

import "time"

type UsageEvent struct {
	ID              uint      `gorm:"primaryKey;index:idx_usage_events_timestamp_id,sort:desc,priority:2;index:idx_usage_events_auth_type_auth_index_id,priority:3;index:idx_usage_events_auth_type_source_id,priority:3"`
	EventKey        string    `gorm:"uniqueIndex:uniq_usage_events_event_key"`
	APIGroupKey     string    `gorm:"index:idx_usage_events_trim_api_group_key,expression:TRIM(api_group_key)"`
	Provider        string    `gorm:"column:provider;index:idx_usage_events_trim_provider,expression:TRIM(provider)"`
	Endpoint        string    `gorm:"column:endpoint"`
	AuthType        string    `gorm:"column:auth_type;index:idx_usage_events_trim_auth_type,expression:TRIM(auth_type);index:idx_usage_events_auth_type_auth_index_id,priority:1;index:idx_usage_events_auth_type_source_id,priority:1"`
	RequestID       string    `gorm:"column:request_id"`
	Model           string    `gorm:"index:idx_usage_events_model;index:idx_usage_events_trim_model,expression:TRIM(model)"`
	Timestamp       time.Time `gorm:"index:idx_usage_events_timestamp_id,sort:desc,priority:1"`
	Source          string    `gorm:"index:idx_usage_events_trim_source,expression:TRIM(source);index:idx_usage_events_auth_type_source_id,priority:2"`
	AuthIndex       string    `gorm:"index:idx_usage_events_trim_auth_index,expression:TRIM(auth_index);index:idx_usage_events_auth_type_auth_index_id,priority:2"`
	Failed          bool      `gorm:"index:idx_usage_events_failed"`
	LatencyMS       int64
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
	CachedTokens    int64
	TotalTokens     int64
	CreatedAt       time.Time
}

type RedisUsageInbox struct {
	ID            uint   `gorm:"primaryKey;index:idx_redis_usage_inboxes_status_id,priority:2"`
	QueueKey      string `gorm:"not null"`
	MessageHash   string `gorm:"not null"`
	RawMessage    string `gorm:"not null"`
	Status        string `gorm:"not null;index:idx_redis_usage_inboxes_status_id,priority:1;index:idx_redis_usage_inboxes_status_processed_at,priority:1;index:idx_redis_usage_inboxes_status_updated_at,priority:1;index:idx_redis_usage_inboxes_status_usage_event_key,priority:1"`
	AttemptCount  int    `gorm:"not null;default:0"`
	LastError     string
	UsageEventKey string     `gorm:"index:idx_redis_usage_inboxes_status_usage_event_key,priority:2"`
	PoppedAt      time.Time  `gorm:"not null"`
	ProcessedAt   *time.Time `gorm:"index:idx_redis_usage_inboxes_status_processed_at,priority:2"`
	CreatedAt     time.Time
	UpdatedAt     time.Time `gorm:"index:idx_redis_usage_inboxes_status_updated_at,priority:2"`
}

type ModelPriceSetting struct {
	ID                   uint   `gorm:"primaryKey"`
	Model                string `gorm:"uniqueIndex:uniq_model_price_settings_model"`
	PromptPricePer1M     float64
	CompletionPricePer1M float64
	CachePricePer1M      float64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type UsageIdentityAuthType int

const (
	UsageIdentityAuthTypeAuthFile   UsageIdentityAuthType = 1
	UsageIdentityAuthTypeAIProvider UsageIdentityAuthType = 2
)

type UsageIdentity struct {
	ID           uint                  `gorm:"primaryKey;index:idx_usage_identities_auth_type_name_id,priority:3"`
	Name         string                `gorm:"index:idx_usage_identities_auth_type_name_id,priority:2"`
	AuthType     UsageIdentityAuthType `gorm:"uniqueIndex:uniq_usage_identities_type_identity;index:idx_usage_identities_auth_type_name_id,priority:1;index:idx_usage_identities_auth_type_type,priority:1"`
	AuthTypeName string
	Identity     string `gorm:"uniqueIndex:uniq_usage_identities_type_identity"`
	Type         string `gorm:"column:type;index:idx_usage_identities_auth_type_type,priority:2"`
	Provider     string
	LookupKey    string
	Prefix       string
	AccountID    *string

	ActiveStart *time.Time
	ActiveUntil *time.Time
	PlanType    *string

	LimitReached                *bool
	PrimaryWindowUsedPercent    *int
	PrimaryWindowLimitSeconds   *int
	PrimaryWindowResetSeconds   *int
	PrimaryWindowResetAt        *time.Time
	SecondaryWindowUsedPercent  *int
	SecondaryWindowLimitSeconds *int
	SecondaryWindowResetSeconds *int
	SecondaryWindowResetAt      *time.Time

	TotalRequests   int64
	SuccessCount    int64
	FailureCount    int64
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
	CachedTokens    int64
	TotalTokens     int64

	LastAggregatedUsageEventID uint
	FirstUsedAt                *time.Time
	LastUsedAt                 *time.Time
	StatsUpdatedAt             *time.Time

	IsDeleted bool
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

func All() []any {
	return []any{
		&UsageEvent{},
		&RedisUsageInbox{},
		&ModelPriceSetting{},
		&UsageIdentity{},
	}
}
