package helper

import (
	"strings"

	"cpa-usage-keeper/internal/entities"
)

func UsageIdentityDisplayName(item entities.UsageIdentity) string {
	name := strings.TrimSpace(item.Name)
	provider := strings.TrimSpace(item.Provider)
	if item.AuthType != entities.UsageIdentityAuthTypeAIProvider {
		if name != "" {
			return name
		}
		return firstNonEmptyString(provider, item.Identity)
	}

	isOpenAICompatible := strings.TrimSpace(item.Type) == "openai"
	if isOpenAICompatible && name != "" && name != "openai" && provider == name {
		return name
	}

	prefix := strings.TrimSpace(item.Prefix)
	baseURL := formatBaseURLDisplay(item.BaseURL)
	qualifiers := displayQualifiers(prefix, baseURL)
	qualifierSeparator := " @ "
	switch {
	case name != "" && len(qualifiers) > 0:
		return name + "(" + strings.Join(qualifiers, qualifierSeparator) + ")"
	case name != "":
		return name
	case prefix != "" && baseURL != "":
		return prefix + "(" + baseURL + ")"
	case prefix != "":
		return prefix
	case provider != "" && baseURL != "":
		return provider + "(" + baseURL + ")"
	case provider != "":
		return provider
	case baseURL != "":
		return baseURL
	default:
		return firstNonEmptyString(provider, item.Identity)
	}
}

func displayQualifiers(values ...string) []string {
	qualifiers := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		qualifiers = append(qualifiers, value)
	}
	return qualifiers
}

func formatBaseURLDisplay(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{"https://", "http://"} {
		if strings.HasPrefix(lower, prefix) {
			trimmed = trimmed[len(prefix):]
			break
		}
	}
	trimmed = strings.TrimRight(trimmed, "/")
	if strings.HasSuffix(strings.ToLower(trimmed), "/v1") {
		trimmed = trimmed[:len(trimmed)-len("/v1")]
	}
	return trimmed
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
