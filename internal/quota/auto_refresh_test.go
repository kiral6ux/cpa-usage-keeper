package quota

import (
	"context"
	"errors"
	"testing"
	"time"

	"cpa-usage-keeper/internal/entities"
)

func TestRunAutoRefreshQueuesOnlyActiveAuthFiles(t *testing.T) {
	db := openQuotaTestDatabase(t)
	disabled := true
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "disabled-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile, Disabled: &disabled})
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "deleted-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile, IsDeleted: true})
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "provider-1", Provider: "openai", Type: "openai", AuthType: entities.UsageIdentityAuthTypeAIProvider})
	handler := &refreshHandlerStub{output: ProviderOutput{Result: ClaudeResult{Usage: &ClaudeUsagePayload{FiveHour: &ClaudeUsageWindow{Utilization: 25}}}}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))
	service.refreshCooldown = func(time.Duration) {}

	if err := service.RunAutoRefresh(context.Background()); err != nil {
		t.Fatalf("RunAutoRefresh returned error: %v", err)
	}
	waitForRefreshTask(t, service, "auth-1", RefreshTaskStatusCompleted)
	if handler.callCount() != 1 {
		t.Fatalf("expected only active auth file to refresh, got %d calls", handler.callCount())
	}
	for _, authIndex := range []string{"disabled-1", "deleted-1", "provider-1"} {
		if _, err := service.GetRefreshTaskByAuthIndex(context.Background(), authIndex); !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("expected %s to stay out of auto refresh queue, got %v", authIndex, err)
		}
	}
}

func TestRunAutoRefreshSkipsCachedHTTPFailures(t *testing.T) {
	db := openQuotaTestDatabase(t)
	seedUsageIdentity(t, db, entities.UsageIdentity{Identity: "auth-1", Provider: "claude", Type: "auth-file", AuthType: entities.UsageIdentityAuthTypeAuthFile})
	handler := &refreshHandlerStub{err: ProviderHTTPError{StatusCode: 401, Message: "expired token"}}
	service := NewServiceWithRegistry(db, NewProviderRegistry(map[string]ProviderHandler{"claude": handler}))
	service.refreshCooldown = func(time.Duration) {}

	first, err := service.Refresh(context.Background(), RefreshRequest{AuthIndexes: []string{"auth-1"}, Source: RefreshSourceManual})
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	waitForRefreshTask(t, service, first.Tasks[0].AuthIndex, RefreshTaskStatusFailed)
	if handler.callCount() != 1 {
		t.Fatalf("expected one manual provider call, got %d", handler.callCount())
	}
	if err := service.RunAutoRefresh(context.Background()); err != nil {
		t.Fatalf("RunAutoRefresh returned error: %v", err)
	}
	if handler.callCount() != 1 {
		t.Fatalf("expected auto refresh to skip cached 401, got %d calls", handler.callCount())
	}
}
