package quota

import "time"

const (
	// RefreshWorkerLimit 控制限额刷新队列的最大并发 worker 数，手动刷新和自动刷新共用同一套队列。
	RefreshWorkerLimit = 10

	// RefreshTaskTimeout 限制单次 provider 限额查询的最长执行时间，避免上游长时间无响应时占住 worker。
	RefreshTaskTimeout = 20 * time.Second

	// RefreshTaskCooldown 是每个 worker 完成一次刷新后的冷却时间，冷却结束后才允许处理下一条任务。
	RefreshTaskCooldown = 1 * time.Second

	// RefreshTransientTaskTTL 是普通失败在内存中的短期保留时间，只用于当前页面轮询读取失败结果。
	RefreshTransientTaskTTL = 20 * time.Minute

	// AutoRefreshInterval 是后台自动刷新 Auth Files 限额的调度间隔。
	AutoRefreshInterval = 10 * time.Minute

	// RefreshErrorCacheTTL 是可恢复展示的 HTTP 错误缓存时间，过期后自动刷新可以重新尝试。
	RefreshErrorCacheTTL = 4 * time.Hour
)

// RefreshCacheableHTTPStatusCodes 定义会写入页面恢复缓存并被自动刷新跳过的 provider HTTP 状态码。
var RefreshCacheableHTTPStatusCodes = map[int]struct{}{
	401: {},
	402: {},
}

// AutoRefreshHTTPStatusSkipCodes 复用可缓存错误码配置，后续新增自动跳过状态码只需要改一处。
var AutoRefreshHTTPStatusSkipCodes = RefreshCacheableHTTPStatusCodes

type APICallConfig struct {
	Method  string
	URL     string
	Headers map[string]string
}

type ProviderConfigs struct {
	Antigravity         []APICallConfig
	Codex               APICallConfig
	GeminiCLI           APICallConfig
	GeminiCLICodeAssist APICallConfig
	ClaudeUsage         APICallConfig
	ClaudeProfile       APICallConfig
	Kimi                APICallConfig
}

func DefaultProviderConfigs() ProviderConfigs {
	return ProviderConfigs{
		Antigravity: []APICallConfig{
			{
				Method: "POST",
				URL:    "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
				Headers: map[string]string{
					"Authorization": "Bearer $TOKEN$",
					"Content-Type":  "application/json",
					"User-Agent":    "antigravity/1.11.5 windows/amd64",
				},
			},
			{
				Method: "POST",
				URL:    "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels",
				Headers: map[string]string{
					"Authorization": "Bearer $TOKEN$",
					"Content-Type":  "application/json",
					"User-Agent":    "antigravity/1.11.5 windows/amd64",
				},
			},
			{
				Method: "POST",
				URL:    "https://cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
				Headers: map[string]string{
					"Authorization": "Bearer $TOKEN$",
					"Content-Type":  "application/json",
					"User-Agent":    "antigravity/1.11.5 windows/amd64",
				},
			},
		},
		Codex: APICallConfig{
			Method: "GET",
			URL:    "https://chatgpt.com/backend-api/wham/usage",
			Headers: map[string]string{
				"Authorization": "Bearer $TOKEN$",
				"Content-Type":  "application/json",
				"User-Agent":    "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal",
			},
		},
		GeminiCLI: APICallConfig{
			Method: "POST",
			URL:    "https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuota",
			Headers: map[string]string{
				"Authorization": "Bearer $TOKEN$",
				"Content-Type":  "application/json",
			},
		},
		GeminiCLICodeAssist: APICallConfig{
			Method: "POST",
			URL:    "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist",
			Headers: map[string]string{
				"Authorization": "Bearer $TOKEN$",
				"Content-Type":  "application/json",
			},
		},
		ClaudeUsage: APICallConfig{
			Method: "GET",
			URL:    "https://api.anthropic.com/api/oauth/usage",
			Headers: map[string]string{
				"Authorization":  "Bearer $TOKEN$",
				"Content-Type":   "application/json",
				"anthropic-beta": "oauth-2025-04-20",
			},
		},
		ClaudeProfile: APICallConfig{
			Method: "GET",
			URL:    "https://api.anthropic.com/api/oauth/profile",
			Headers: map[string]string{
				"Authorization":  "Bearer $TOKEN$",
				"Content-Type":   "application/json",
				"anthropic-beta": "oauth-2025-04-20",
			},
		},
		Kimi: APICallConfig{
			Method: "GET",
			URL:    "https://api.kimi.com/coding/v1/usages",
			Headers: map[string]string{
				"Authorization": "Bearer $TOKEN$",
			},
		},
	}
}

func (c ProviderConfigs) APICallTemplates() []APICallConfig {
	templates := make([]APICallConfig, 0, len(c.Antigravity)+6)
	templates = append(templates, c.Antigravity...)
	templates = append(templates,
		c.Codex,
		c.GeminiCLI,
		c.GeminiCLICodeAssist,
		c.ClaudeUsage,
		c.ClaudeProfile,
		c.Kimi,
	)
	return templates
}
