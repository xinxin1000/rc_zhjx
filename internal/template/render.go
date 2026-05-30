package template

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var placeholder = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}`)

func RenderJSON(bodyTemplate any, payload map[string]any) ([]byte, error) {
	rendered := renderValue(bodyTemplate, payload)
	return json.Marshal(rendered)
}

func ExtractFields(payload map[string]any, mapping map[string]string) map[string]any {
	extracted := make(map[string]any, len(mapping))
	for name, path := range mapping {
		value, ok := lookup(payload, path)
		if ok {
			extracted[name] = value
		}
	}
	return extracted
}

func renderValue(value any, payload map[string]any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = renderValue(item, payload)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, renderValue(item, payload))
		}
		return out
	case string:
		return renderString(typed, payload)
	default:
		return typed
	}
}

func renderString(input string, payload map[string]any) any {
	matches := placeholder.FindAllStringSubmatch(input, -1)
	if len(matches) == 1 && strings.TrimSpace(input) == matches[0][0] {
		if value, ok := lookup(payload, matches[0][1]); ok {
			return value
		}
		return nil
	}

	return placeholder.ReplaceAllStringFunc(input, func(token string) string {
		match := placeholder.FindStringSubmatch(token)
		if len(match) != 2 {
			return token
		}
		value, ok := lookup(payload, match[1])
		if !ok || value == nil {
			return ""
		}
		return fmt.Sprint(value)
	})
}

func lookup(root map[string]any, path string) (any, bool) {
	var current any = root
	for _, part := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}
