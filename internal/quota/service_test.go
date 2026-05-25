package quota

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cpa-usage-keeper/internal/entities"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type refreshHandlerStub struct {
	mu     sync.Mutex
	calls  []string
	block  <-chan struct{}
	output ProviderOutput
	err    error
}

func (s *refreshHandlerStub) Check(ctx context.Context, input ProviderInput) (ProviderOutput, error) {
	if s.block != nil {
		select {
		case <-ctx.Done():
			return ProviderOutput{}, ctx.Err()
		case <-s.block:
		}
	}
	s.mu.Lock()
	s.calls = append(s.calls, input.Identity.Identity)
	s.mu.Unlock()
	if s.err != nil {
		return ProviderOutput{}, s.err
	}
	return s.output, nil
}

func (s *refreshHandlerStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func TestRefreshCreatesTaskPerAuthIndexAndCachesCompletedQuota(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	handler := &refreshHandlerStub{output: ProviderOutput{Result: ClaudeResult{Usage: &ClaudeUsagePayload{FiveHour: &ClaudeUsageWindow{Utilization: 25}}}}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))

	response, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if response.Accepted != 1 || response.Skipped != 0 || len(response.Tasks) != 1 {
		t.Fatalf("unexpected refresh response: %+v", response)
	}

	task := waitForRefreshTask(t, service, response.Tasks[0].AuthIndex, RefreshTaskStatusCompleted)
	if task.AuthIndex != "auth-1" || task.Quota == nil || task.Quota.ID != "auth-1" || len(task.Quota.Quota) != 1 {
		t.Fatalf("expected completed task to expose cached quota, got %+v", task)
	}
	if task.ExpiresAt != nil {
		t.Fatalf("expected completed quota cache to have no expiry, got %v", task.ExpiresAt)
	}
	service.cleanupExpiredRefreshTasks(time.Now().Add(RefreshTransientTaskTTL * 2))
	cache, err := service.GetCachedQuota(context.Background(), CacheRequest{AuthIndexes: []string{"auth-1"}})
	if err != nil {
		t.Fatalf("GetCachedQuota returned error: %v", err)
	}
	if len(cache.Items) != 1 || cache.Items[0].AuthIndex != "auth-1" || cache.Items[0].Quota == nil || cache.Items[0].Quota.ID != "auth-1" {
		t.Fatalf("expected completed quota cache to survive cleanup, got %+v", cache)
	}
	if handler.callCount() != 1 {
		t.Fatalf("expected one provider call, got %d", handler.callCount())
	}
}

func TestRefreshOverwritesPreviousCompletedTaskForSameAuthIndex(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	handler := &refreshHandlerStub{output: ProviderOutput{Result: ClaudeResult{Usage: &ClaudeUsagePayload{FiveHour: &ClaudeUsageWindow{Utilization: 25}}}}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))

	first, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("first Refresh returned error: %v", err)
	}
	waitForRefreshTask(t, service, first.Tasks[0].AuthIndex, RefreshTaskStatusCompleted)

	handler.output = ProviderOutput{Result: ClaudeResult{Usage: &ClaudeUsagePayload{FiveHour: &ClaudeUsageWindow{Utilization: 60}}}}
	second, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("second Refresh returned error: %v", err)
	}
	waitForRefreshTask(t, service, second.Tasks[0].AuthIndex, RefreshTaskStatusCompleted)
	cache, err := service.GetCachedQuota(context.Background(), CacheRequest{AuthIndexes: []string{"auth-1"}})
	if err != nil {
		t.Fatalf("GetCachedQuota returned error: %v", err)
	}
	if len(cache.Items) != 1 || cache.Items[0].Quota == nil || cache.Items[0].Quota.Quota[0].UsedPercent == nil || *cache.Items[0].Quota.Quota[0].UsedPercent != 60 {
		t.Fatalf("expected cache to expose latest quota, got %+v", cache)
	}
}

func TestRefreshRejectsInvalidEntriesAndIgnoresRunningTask(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "provider-1", Provider: "openai", Type: "openai", AuthType: entities.UsageIdentityAuthTypeAIProvider})
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "deleted-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile, IsDeleted: true})
	block := make(chan struct{})
	handler := &refreshHandlerStub{block: block, output: ProviderOutput{Result: ClaudeResult{Usage: &ClaudeUsagePayload{FiveHour: &ClaudeUsageWindow{Utilization: 25}}}}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))

	response, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1", "auth-1", "provider-1", "deleted-1", "missing"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if response.Accepted != 1 || response.Skipped != 4 || len(response.Tasks) != 1 || len(response.Rejected) != 4 {
		t.Fatalf("unexpected refresh response: %+v", response)
	}
	if !hasRefreshRejection(response.Rejected, "auth-1", "duplicate") || !hasRefreshRejection(response.Rejected, "provider-1", "not_auth_file") || !hasRefreshRejection(response.Rejected, "deleted-1", "not_found") || !hasRefreshRejection(response.Rejected, "missing", "not_found") {
		t.Fatalf("unexpected rejected entries: %+v", response.Rejected)
	}

	firstTaskID := response.Tasks[0].AuthIndex
	waitForRefreshTask(t, service, firstTaskID, RefreshTaskStatusRunning)
	second, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("second Refresh returned error: %v", err)
	}
	if second.Accepted != 0 || second.Skipped != 1 || len(second.Tasks) != 0 || !hasRefreshRejection(second.Rejected, "auth-1", "duplicate") {
		t.Fatalf("expected running task to be ignored as duplicate, got %+v", second)
	}
	close(block)
	waitForRefreshTask(t, service, firstTaskID, RefreshTaskStatusCompleted)
	if handler.callCount() != 1 {
		t.Fatalf("expected duplicate refresh to reuse provider call, got %d", handler.callCount())
	}
}

func TestRefreshQueueUsesConfiguredWorkersTimeoutAndCooldown(t *testing.T) {
	if RefreshWorkerLimit != 10 {
		t.Fatalf("expected refresh worker limit 10, got %d", RefreshWorkerLimit)
	}
	if RefreshTaskTimeout != 20*time.Second {
		t.Fatalf("expected refresh task timeout 20s, got %s", RefreshTaskTimeout)
	}
	if RefreshTaskCooldown != time.Second {
		t.Fatalf("expected refresh task cooldown 1s, got %s", RefreshTaskCooldown)
	}
}

func TestRefreshTaskWaitsForCooldownBeforeReleasingWorker(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	handler := &refreshHandlerStub{output: ProviderOutput{Result: ClaudeResult{Usage: &ClaudeUsagePayload{FiveHour: &ClaudeUsageWindow{Utilization: 25}}}}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))
	cooldownCalls := make(chan time.Duration, 1)
	service.refreshCooldown = func(duration time.Duration) {
		cooldownCalls <- duration
	}

	response, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	waitForRefreshTask(t, service, response.Tasks[0].AuthIndex, RefreshTaskStatusCompleted)

	select {
	case duration := <-cooldownCalls:
		if duration != RefreshTaskCooldown {
			t.Fatalf("expected cooldown %s, got %s", RefreshTaskCooldown, duration)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected refresh task to call cooldown")
	}
}

func TestRefreshTaskFailureReturnsFriendlyMessage(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	handler := &refreshHandlerStub{err: errors.New("upstream exploded")}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))

	response, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	task := waitForRefreshTask(t, service, response.Tasks[0].AuthIndex, RefreshTaskStatusFailed)
	if task.Error != "Quota refresh failed. Please try again later." {
		t.Fatalf("expected friendly error message, got %q", task.Error)
	}
	cache, err := service.GetCachedQuota(context.Background(), CacheRequest{AuthIndexes: []string{"auth-1"}})
	if err != nil {
		t.Fatalf("GetCachedQuota returned error: %v", err)
	}
	if len(cache.Items) != 0 {
		t.Fatalf("expected transient failure to stay out of page cache, got %+v", cache.Items)
	}
}

func TestRefreshTaskCachesConfiguredHTTPError(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	handler := &refreshHandlerStub{err: ProviderHTTPError{StatusCode: 401, Message: "expired token"}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))

	response, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	task := waitForRefreshTask(t, service, response.Tasks[0].AuthIndex, RefreshTaskStatusFailed)
	if task.HTTPStatusCode == nil || *task.HTTPStatusCode != 401 {
		t.Fatalf("expected task to expose HTTP status 401, got %+v", task)
	}
	if task.ExpiresAt == nil || task.ExpiresAt.Sub(*task.CachedAt) != RefreshErrorCacheTTL {
		t.Fatalf("expected 401 cache TTL %s, got cachedAt=%v expiresAt=%v", RefreshErrorCacheTTL, task.CachedAt, task.ExpiresAt)
	}

	cache, err := service.GetCachedQuota(context.Background(), CacheRequest{AuthIndexes: []string{"auth-1"}})
	if err != nil {
		t.Fatalf("GetCachedQuota returned error: %v", err)
	}
	if len(cache.Items) != 1 || cache.Items[0].Status != RefreshTaskStatusFailed || cache.Items[0].HTTPStatusCode == nil || *cache.Items[0].HTTPStatusCode != 401 {
		t.Fatalf("expected cached failed item with HTTP 401, got %+v", cache.Items)
	}
}

func openQuotaTestDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "quota.db")), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open returned error: %v", err)
	}
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
	if err := db.AutoMigrate(entities.All()...); err != nil {
		t.Fatalf("AutoMigrate returned error: %v", err)
	}
	return db
}

func seedUsageIdentity(t *testing.T, db *gorm.DB, identity entities.UsageIdentity) {
	t.Helper()
	identity.Name = identity.Identity
	if err := db.Create(&identity).Error; err != nil {
		t.Fatalf("seed usage identity %q: %v", identity.Identity, err)
	}
}

func waitForRefreshTask(t *testing.T, service *Service, authIndex string, status RefreshTaskStatus) RefreshTaskResponse {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var task RefreshTaskResponse
	var err error
	for time.Now().Before(deadline) {
		task, err = service.GetRefreshTaskByAuthIndex(context.Background(), authIndex)
		if err == nil && task.Status == status {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("auth_index %s did not reach status %s, last task=%+v err=%v", authIndex, status, task, err)
	return RefreshTaskResponse{}
}

func hasRefreshRejection(rejections []RefreshRejectedAuthIndex, authIndex string, code string) bool {
	for _, rejection := range rejections {
		if rejection.AuthIndex == authIndex && rejection.Error == code {
			return true
		}
	}
	return false
}
