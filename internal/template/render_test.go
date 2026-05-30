package template

import (
	"encoding/json"
	"testing"
)

func TestRenderJSONAndExtractFields(t *testing.T) {
	payload := map[string]any{
		"user": map[string]any{
			"id": "u_123",
		},
		"campaign": "spring",
		"amount":   float64(99),
	}

	body, err := RenderJSON(map[string]any{
		"user_id":  "{{user.id}}",
		"campaign": "{{campaign}}",
		"amount":   "{{amount}}",
		"message":  "registered from {{campaign}}",
	}, payload)
	if err != nil {
		t.Fatalf("RenderJSON() error = %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal rendered body: %v", err)
	}
	if got["user_id"] != "u_123" {
		t.Fatalf("user_id = %v", got["user_id"])
	}
	if got["amount"] != float64(99) {
		t.Fatalf("amount = %v", got["amount"])
	}
	if got["message"] != "registered from spring" {
		t.Fatalf("message = %v", got["message"])
	}

	extracted := ExtractFields(payload, map[string]string{
		"user_id": "user.id",
		"missing": "user.missing",
	})
	if extracted["user_id"] != "u_123" {
		t.Fatalf("extracted user_id = %v", extracted["user_id"])
	}
	if _, ok := extracted["missing"]; ok {
		t.Fatalf("missing field should not be extracted")
	}
}
