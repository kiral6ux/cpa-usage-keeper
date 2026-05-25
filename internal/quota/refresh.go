package quota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cpa-usage-keeper/internal/entities"
	"cpa-usage-keeper/internal/repository"
	"cpa-usage-keeper/internal/timeutil"

	"gorm.io/gorm"
)

type RefreshSource string

const (
	RefreshSourceManual        RefreshSource = "manual"
	RefreshSourceAuto          RefreshSource = "auto"
	RefreshSourceScheduled     RefreshSource = "scheduled"
	RefreshSourceCacheBackfill RefreshSource = "cache_backfill"
)

type RefreshTaskStatus string

const (
	RefreshTaskStatusQueued    RefreshTaskStatus = "queued"
	RefreshTaskStatusRunning   RefreshTaskStatus = "running"
	RefreshTaskStatusCompleted RefreshTaskStatus = "completed"
	RefreshTaskStatusFailed    RefreshTaskStatus = "failed"
)

type CacheRequest struct {
	AuthIndexes []string `json:"auth_indexes"`
}

type CacheResponse struct {
	Items []CachedQuotaItem `json:"items"`
}

type CachedQuotaItem struct {
	AuthIndex      string            `json:"auth_index"`
	Status         RefreshTaskStatus `json:"status"`
	Quota          *CheckResponse    `json:"quota,omitempty"`
	Error          string            `json:"error,omitempty"`
	HTTPStatusCode *int              `json:"http_status_code,omitempty"`
	ExpiresAt      *time.Time        `json:"expires_at,omitempty"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

type RefreshRequest struct {
	AuthIndexes []string      `json:"auth_indexes"`
	Source      RefreshSource `json:"source"`
}

type RefreshResponse struct {
	Tasks    []RefreshTaskRef           `json:"tasks"`
	Rejected []RefreshRejectedAuthIndex `json:"rejected"`
	Accepted int                        `json:"accepted"`
	Skipped  int                        `json:"skipped"`
	Limit    int                        `json:"limit"`
}

type RefreshTaskRef struct {
	AuthIndex string `json:"authIndex"`
}

type RefreshRejectedAuthIndex struct {
	AuthIndex string `json:"authIndex"`
	Error     string `json:"error"`
}

type RefreshTaskResponse struct {
	AuthIndex      string            `json:"authIndex"`
	Status         RefreshTaskStatus `json:"status"`
	Quota          *CheckResponse    `json:"quota,omitempty"`
	Error          string            `json:"error,omitempty"`
	HTTPStatusCode *int              `json:"http_status_code,omitempty"`
	CachedAt       *time.Time        `json:"cachedAt,omitempty"`
	ExpiresAt      *time.Time        `json:"expiresAt,omitempty"`
}

type RefreshTaskRecord struct {
	AuthIndex      string
	Status         RefreshTaskStatus
	Quota          *CheckResponse
	Error          string
	HTTPStatusCode *int
	Source         RefreshSource
	CreatedAt      time.Time
	StartedAt      time.Time
	FinishedAt     time.Time
	CachedAt       time.Time
	ExpiresAt      time.Time
}

func (s *Service) GetCachedQuota(ctx context.Context, request CacheRequest) (CacheResponse, error) {
	_ = ctx
	// 缓存读取只返回已完成任务的结果，不触发新的 provider 请求。
	if len(request.AuthIndexes) == 0 {
		return CacheResponse{}, fmt.Errorf("%w: auth_indexes are required", ErrValidation)
	}
	response := CacheResponse{Items: make([]CachedQuotaItem, 0, len(request.AuthIndexes))}
	s.cleanupExpiredRefreshTasks(time.Now())
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	// 按请求顺序去重并读取每个 auth_index 最近一次完成的任务缓存。
	seen := make(map[string]struct{}, len(request.AuthIndexes))
	for _, rawAuthIndex := range request.AuthIndexes {
		authIndex := strings.TrimSpace(rawAuthIndex)
		if authIndex == "" {
			continue
		}
		if _, ok := seen[authIndex]; ok {
			continue
		}
		seen[authIndex] = struct{}{}
		task, ok := s.refreshTasks[authIndex]
		if !ok {
			continue
		}
		// 页面恢复缓存只暴露两类稳定状态：成功 quota 和配置允许持久展示的 HTTP 错误。
		// 普通网络错误/500/超时只给当前轮询读取，不从 cache 接口恢复，避免刷新页面后展示不可长期判断的瞬时失败。
		switch {
		case task.Status == RefreshTaskStatusCompleted && task.Quota != nil:
			quota := *task.Quota
			response.Items = append(response.Items, CachedQuotaItem{AuthIndex: authIndex, Status: RefreshTaskStatusCompleted, Quota: &quota, UpdatedAt: task.CachedAt})
		case task.Status == RefreshTaskStatusFailed && task.HTTPStatusCode != nil && isRefreshCacheableHTTPStatus(*task.HTTPStatusCode):
			expiresAt := task.ExpiresAt
			response.Items = append(response.Items, CachedQuotaItem{AuthIndex: authIndex, Status: RefreshTaskStatusFailed, Error: task.Error, HTTPStatusCode: task.HTTPStatusCode, ExpiresAt: &expiresAt, UpdatedAt: task.FinishedAt})
		}
	}
	return response, nil
}

func (s *Service) Refresh(ctx context.Context, request RefreshRequest) (RefreshResponse, error) {
	// 刷新入口只负责校验、去重、建任务；实际 provider 调用交给后台 worker。
	limit := len(request.AuthIndexes)
	if limit <= 0 {
		return RefreshResponse{}, fmt.Errorf("%w: auth_indexes are required", ErrValidation)
	}
	response := RefreshResponse{Limit: limit}
	seen := make(map[string]struct{}, len(request.AuthIndexes))
	s.cleanupExpiredRefreshTasks(time.Now())

	for _, rawAuthIndex := range request.AuthIndexes {
		// 每个 auth_index 独立生成任务，便于前端逐行轮询和展示错误。
		authIndex := strings.TrimSpace(rawAuthIndex)
		if authIndex == "" {
			response.Rejected = append(response.Rejected, RefreshRejectedAuthIndex{AuthIndex: authIndex, Error: "invalid"})
			continue
		}
		if _, ok := seen[authIndex]; ok {
			response.Rejected = append(response.Rejected, RefreshRejectedAuthIndex{AuthIndex: authIndex, Error: "duplicate"})
			continue
		}
		seen[authIndex] = struct{}{}
		if response.Accepted >= limit {
			response.Rejected = append(response.Rejected, RefreshRejectedAuthIndex{AuthIndex: authIndex, Error: "invalid"})
			continue
		}
		if rejection, err := s.validateRefreshAuthIndex(ctx, authIndex); err != nil {
			return RefreshResponse{}, err
		} else if rejection != "" {
			response.Rejected = append(response.Rejected, RefreshRejectedAuthIndex{AuthIndex: authIndex, Error: rejection})
			continue
		}

		task, created := s.ensureRefreshTask(authIndex, request.Source)
		if !created {
			response.Rejected = append(response.Rejected, RefreshRejectedAuthIndex{AuthIndex: authIndex, Error: "duplicate"})
			continue
		}
		response.Tasks = append(response.Tasks, RefreshTaskRef{AuthIndex: task.AuthIndex})
		response.Accepted++
		go s.runRefreshTask(task.AuthIndex)
	}
	response.Skipped = len(response.Rejected)
	return response, nil
}

func (s *Service) GetRefreshTaskByAuthIndex(ctx context.Context, authIndex string) (RefreshTaskResponse, error) {
	_ = ctx
	authIndex = strings.TrimSpace(authIndex)
	if authIndex == "" {
		return RefreshTaskResponse{}, fmt.Errorf("%w: auth_index is required", ErrValidation)
	}
	s.cleanupExpiredRefreshTasks(time.Now())
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	task, ok := s.refreshTasks[authIndex]
	if !ok {
		return RefreshTaskResponse{}, ErrTaskNotFound
	}
	return task.response(), nil
}

func (s *Service) validateRefreshAuthIndex(ctx context.Context, authIndex string) (string, error) {
	// 先按 auth-file 身份查找；查不到时再区分“非 auth file”和“不存在”。
	identity, err := repository.GetActiveAuthFileUsageIdentityByAuthIndex(ctx, s.db, authIndex)
	if err == nil {
		if _, _, ok := s.resolveQuotaHandler(identity.Provider, identity.Type); !ok {
			return "unsupported", nil
		}
		return "", nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	}

	var active entities.UsageIdentity
	if err := s.db.WithContext(ctx).Select("id, auth_type").Where("identity = ? AND is_deleted = ?", authIndex, false).First(&active).Error; err == nil {
		return "not_auth_file", nil
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		return "not_found", nil
	} else {
		return "", err
	}
}

func (s *Service) ensureRefreshTask(authIndex string, source RefreshSource) (*RefreshTaskRecord, bool) {
	// auth_index 本身就是任务唯一标识；queued/running 时直接拒绝重复入队，避免重复打到上游接口。
	now := timeutil.NormalizeStorageTime(time.Now())
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if task, ok := s.refreshTasks[authIndex]; ok && task.isActive() {
		return task, false
	}
	task := &RefreshTaskRecord{
		AuthIndex: authIndex,
		Status:    RefreshTaskStatusQueued,
		Source:    source,
		CreatedAt: now,
	}
	s.refreshTasks[authIndex] = task
	return task, true
}

func (s *Service) runRefreshTask(authIndex string) {
	// worker token 控制全局并发，防止一次批量刷新同时压垮 CPA/上游接口。
	s.refreshWorkerTokens <- struct{}{}
	defer func() {
		// 冷却必须发生在释放 worker slot 之前，否则队列会立刻补进下一条任务，无法形成“每个 worker 完成后停 1 秒”的节流效果。
		s.refreshCooldown(RefreshTaskCooldown)
		<-s.refreshWorkerTokens
	}()

	authIndex, ok := s.markRefreshTaskRunning(authIndex)
	if !ok {
		return
	}
	// 每个任务独立设置超时；超时或 provider 错误都会沉淀到任务状态里给前端展示。
	ctx, cancel := context.WithTimeout(context.Background(), RefreshTaskTimeout)
	defer cancel()
	response, err := s.Check(ctx, CheckRequest{AuthIndex: authIndex})
	if err != nil {
		s.markRefreshTaskFailed(authIndex, err)
		return
	}
	// provider 成功后立即把窗口内 token/cost 补进同一次缓存，前端读取缓存时不再触发额外统计请求。
	response = s.attachWindowUsageStats(authIndex, response, time.Now())
	s.markRefreshTaskCompleted(authIndex, response)
}

func refreshTaskErrorMessage(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "Quota refresh timed out. Please try again later."
	}
	if errors.Is(err, ErrProviderInput) {
		return ProviderInputErrorMessage(err, "Quota request is missing required parameters.")
	}
	var httpErr ProviderHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Error()
	}
	if strings.HasPrefix(err.Error(), "HTTP ") {
		return err.Error()
	}
	return "Quota refresh failed. Please try again later."
}

func refreshTaskHTTPStatusCode(err error) *int {
	var httpErr ProviderHTTPError
	if !errors.As(err, &httpErr) {
		return nil
	}
	statusCode := httpErr.StatusCode
	return &statusCode
}

func isRefreshCacheableHTTPStatus(statusCode int) bool {
	_, ok := RefreshCacheableHTTPStatusCodes[statusCode]
	return ok
}

func (s *Service) markRefreshTaskRunning(authIndex string) (string, bool) {
	now := timeutil.NormalizeStorageTime(time.Now())
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	task, ok := s.refreshTasks[authIndex]
	if !ok || task.Status != RefreshTaskStatusQueued {
		return "", false
	}
	task.Status = RefreshTaskStatusRunning
	task.StartedAt = now
	return task.AuthIndex, true
}

func (s *Service) markRefreshTaskCompleted(authIndex string, response CheckResponse) {
	now := timeutil.NormalizeStorageTime(time.Now())
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	task, ok := s.refreshTasks[authIndex]
	if !ok {
		return
	}
	task.Status = RefreshTaskStatusCompleted
	task.FinishedAt = now
	task.CachedAt = now
	task.Quota = &response
}

func (s *Service) markRefreshTaskFailed(authIndex string, err error) {
	now := timeutil.NormalizeStorageTime(time.Now())
	message := refreshTaskErrorMessage(err)
	httpStatusCode := refreshTaskHTTPStatusCode(err)
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	task, ok := s.refreshTasks[authIndex]
	if !ok {
		return
	}
	// 失败任务分两类保存：401/402 这类可配置 HTTP 错误要进入页面恢复缓存；其它失败只短期保留给当前轮询。
	task.Status = RefreshTaskStatusFailed
	task.FinishedAt = now
	task.CachedAt = now
	task.Error = message
	task.HTTPStatusCode = httpStatusCode
	if httpStatusCode != nil && isRefreshCacheableHTTPStatus(*httpStatusCode) {
		task.ExpiresAt = now.Add(RefreshErrorCacheTTL)
		return
	}
	task.ExpiresAt = now.Add(s.refreshTaskTTL)
}

func (s *Service) cleanupExpiredRefreshTasks(now time.Time) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	s.cleanupExpiredRefreshTasksLocked(now)
}

func (s *Service) cleanupExpiredRefreshTasksLocked(now time.Time) {
	// refreshTasks 直接以 auth_index 为 key；过期时删除这一条缓存即可，不再维护额外 taskId 索引。
	for authIndex, task := range s.refreshTasks {
		if task.ExpiresAt.IsZero() || now.Before(task.ExpiresAt) {
			continue
		}
		delete(s.refreshTasks, authIndex)
	}
}

func (t *RefreshTaskRecord) isActive() bool {
	return t.Status == RefreshTaskStatusQueued || t.Status == RefreshTaskStatusRunning
}

func (t *RefreshTaskRecord) response() RefreshTaskResponse {
	response := RefreshTaskResponse{
		AuthIndex:      t.AuthIndex,
		Status:         t.Status,
		Error:          t.Error,
		HTTPStatusCode: t.HTTPStatusCode,
	}
	if t.Quota != nil {
		quota := *t.Quota
		response.Quota = &quota
	}
	if !t.CachedAt.IsZero() {
		cachedAt := t.CachedAt
		response.CachedAt = &cachedAt
	}
	if !t.ExpiresAt.IsZero() {
		expiresAt := t.ExpiresAt
		response.ExpiresAt = &expiresAt
	}
	return response
}
