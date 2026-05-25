package quota

import (
	"context"
	"time"

	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/timeutil"

	"github.com/sirupsen/logrus"
)

func (s *Service) RunAutoRefresh(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	// 自动刷新每轮开始先清理过期任务，确保 401/402 过期后能重新进入队列，而不是被旧缓存一直拦住。
	s.cleanupExpiredRefreshTasks(time.Now())
	identities, err := s.listAutoRefreshAuthFiles(ctx)
	if err != nil {
		return err
	}
	queued := 0
	skippedCachedError := 0
	skippedRunning := 0
	for _, identity := range identities {
		authIndex := identity.Identity
		if authIndex == "" {
			continue
		}
		if s.shouldSkipAutoRefreshForCachedHTTPError(authIndex, time.Now()) {
			// 这里跳过的是未过期的可缓存 HTTP 错误，避免后台持续打已知不可自动恢复的身份。
			skippedCachedError++
			continue
		}
		if task, created := s.ensureRefreshTask(authIndex, RefreshSourceAuto); created {
			queued++
			go s.runRefreshTask(task.AuthIndex)
		} else if task != nil && task.isActive() {
			// queued/running 已经代表这个 auth_index 在队列里，自动刷新不能重复入队。
			skippedRunning++
		}
	}
	logrus.WithFields(logrus.Fields{
		"scanned":              len(identities),
		"queued":               queued,
		"skipped_cached_error": skippedCachedError,
		"skipped_running":      skippedRunning,
	}).Info("quota auto refresh round completed")
	return nil
}

func (s *Service) StartAutoRefresh(ctx context.Context) error {
	// 启动后立即执行第一轮，避免服务刚启动后的 Auth Files 页面长时间依赖旧缓存。
	if err := s.RunAutoRefresh(ctx); err != nil {
		logrus.Errorf("quota auto refresh failed: %v", err)
	}
	ticker := time.NewTicker(AutoRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.RunAutoRefresh(ctx); err != nil {
				logrus.Errorf("quota auto refresh failed: %v", err)
			}
		}
	}
}

func (s *Service) listAutoRefreshAuthFiles(ctx context.Context) ([]entities.UsageIdentity, error) {
	var identities []entities.UsageIdentity
	// 自动刷新只扫描未删除且未禁用的 Auth Files；AI Provider 和用户停用的 Auth File 都不应产生后台请求。
	err := s.db.WithContext(ctx).
		Select("id, identity, provider, type, auth_type, is_deleted, disabled").
		Where("auth_type = ? AND is_deleted = ? AND (disabled IS NULL OR disabled = ?)", entities.UsageIdentityAuthTypeAuthFile, false, false).
		Order("priority IS NULL ASC").
		Order("priority DESC").
		Order("id ASC").
		Find(&identities).Error
	return identities, err
}

func (s *Service) shouldSkipAutoRefreshForCachedHTTPError(authIndex string, now time.Time) bool {
	now = timeutil.NormalizeStorageTime(now)
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	task, ok := s.refreshTasks[authIndex]
	if !ok || task.Status != RefreshTaskStatusFailed || task.HTTPStatusCode == nil {
		return false
	}
	if _, ok := AutoRefreshHTTPStatusSkipCodes[*task.HTTPStatusCode]; !ok {
		return false
	}
	// 只有未过期的 401/402 等配置错误会拦截自动刷新；过期后下一轮可以重新尝试并覆盖旧错误。
	return task.ExpiresAt.IsZero() || now.Before(task.ExpiresAt)
}
