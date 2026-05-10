package quota

import (
	"context"
	"fmt"

	"cpa-usage-keeper/internal/cpa/dto/apicall"
)

type codexProvider struct {
	caller ManagementAPICaller
	config APICallConfig
}

func NewCodexProvider(caller ManagementAPICaller, config APICallConfig) ProviderHandler {
	return codexProvider{caller: caller, config: config}
}

func (p codexProvider) Check(ctx context.Context, input ProviderInput) (ProviderOutput, error) {
	// Codex quota 依赖 account_id；缺少时直接返回可展示的参数错误，不发起无效请求。
	if input.Identity.AccountID == nil || *input.Identity.AccountID == "" {
		return ProviderOutput{}, fmt.Errorf("%w: missing account_id parameter", ErrProviderInput)
	}
	// 统一调用 CPA api-call，由后端补齐固定 URL/header 和当前账号的动态 header。
	request := apicall.Request{
		AuthIndex: input.Identity.Identity,
		Method:    p.config.Method,
		URL:       p.config.URL,
		Header:    mergeHeaders(p.config.Headers, map[string]string{"Chatgpt-Account-Id": *input.Identity.AccountID}),
	}
	response, err := p.caller.CallManagementAPI(ctx, request)
	if err != nil {
		return ProviderOutput{}, err
	}
	usage, err := parseCodexUsagePayload(response)
	if err != nil {
		return ProviderOutput{}, err
	}
	return ProviderOutput{Provider: "codex", Result: CodexResult{Usage: usage}}, nil
}
