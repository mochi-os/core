// Mochi server: AI prompt API
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Provides mochi.ai.prompt() for Starlark apps to send prompts to AI providers
// (Claude, OpenAI) via the user's connected accounts. API keys are never exposed
// to Starlark — the server handles all provider interactions internally.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_ai = sls.FromStringDict(sl.String("mochi.ai"), sl.StringDict{
	"prompt": sl.NewBuiltin("mochi.ai.prompt", api_ai_prompt),
})

// Default models for each AI provider
var ai_provider_defaults = map[string]string{
	"claude": "claude-haiku-4-5-20251001",
	"openai": "gpt-4o-mini",
}

// ai_result holds the response from an AI provider call
type ai_result struct {
	status int
	text   string
}

// mochi.ai.prompt(prompt, account?) -> dict: Send a prompt to an AI provider
func api_ai_prompt(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <prompt: string>, [account=<int>]")
	}

	if err := require_permission(t, fn, "accounts/ai"); err != nil {
		return sl_encode(map[string]any{"status": 403, "text": ""}), nil
	}

	prompt, ok := sl.AsString(args[0])
	if !ok || prompt == "" {
		return sl_error(fn, "invalid prompt")
	}

	// Parse optional account kwarg
	account_id := ""
	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		if key == "account" {
			id, ok := account_id_arg(kv[1])
			if !ok {
				return sl_error(fn, "invalid account id")
			}
			account_id = id
		}
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")

	var ptype, api_key, model string

	if account_id != "" {
		// Look up specific account
		row, err := db.row("select type, data, enabled from accounts where id=?", account_id)
		if err != nil || row == nil {
			return sl_encode(map[string]any{"status": 0, "text": ""}), nil
		}
		ptype, _ = row["type"].(string)
		if !provider_has_capability(ptype, "ai") {
			return sl_encode(map[string]any{"status": 0, "text": ""}), nil
		}
		enabled, _ := row["enabled"].(int64)
		if enabled != 1 {
			return sl_encode(map[string]any{"status": 0, "text": ""}), nil
		}
		raw, _ := row["data"].(string)
		var data map[string]any
		if raw != "" {
			json.Unmarshal([]byte(raw), &data)
		}
		api_key, _ = data["api_key"].(string)
		model, _ = data["model"].(string)
	} else {
		// Use the designated default AI account
		row, err := db.row("select type, data, enabled from accounts where (',' || \"default\" || ',') like '%,ai,%' and enabled=1")
		if err != nil || row == nil {
			return sl_encode(map[string]any{"status": 0, "text": ""}), nil
		}
		ptype, _ = row["type"].(string)
		if !provider_has_capability(ptype, "ai") {
			return sl_encode(map[string]any{"status": 0, "text": ""}), nil
		}
		raw, _ := row["data"].(string)
		var data map[string]any
		if raw != "" {
			json.Unmarshal([]byte(raw), &data)
		}
		api_key, _ = data["api_key"].(string)
		model, _ = data["model"].(string)
	}

	// Determine model
	if model == "" || model == "default" {
		model = ai_provider_defaults[ptype]
	}

	// Call the provider
	var result ai_result
	switch ptype {
	case "claude":
		result = ai_call_claude(api_key, model, prompt)
	case "openai":
		result = ai_call_openai(api_key, model, prompt)
	default:
		return sl_encode(map[string]any{"status": 0, "text": ""}), nil
	}

	// Model fallback: if model not found and not already using default, retry with default
	if result.status == 404 && model != ai_provider_defaults[ptype] {
		debug("ai: model %q not found for %s, falling back to default %q", model, ptype, ai_provider_defaults[ptype])
		switch ptype {
		case "claude":
			result = ai_call_claude(api_key, ai_provider_defaults[ptype], prompt)
		case "openai":
			result = ai_call_openai(api_key, ai_provider_defaults[ptype], prompt)
		}
	}

	return sl_encode(map[string]any{"status": result.status, "text": result.text}), nil
}

// ai_call_claude sends a prompt to the Claude (Anthropic) API
func ai_call_claude(api_key, model, prompt string) ai_result {
	payload, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": 16384,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})

	req, err := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(payload))
	if err != nil {
		return ai_result{status: 500, text: ""}
	}
	req.Header.Set("x-api-key", api_key)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return ai_result{status: 500, text: ""}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == 200 {
		var data map[string]any
		if json.Unmarshal(body, &data) == nil {
			if content, ok := data["content"].([]any); ok && len(content) > 0 {
				if block, ok := content[0].(map[string]any); ok {
					text, _ := block["text"].(string)
					return ai_result{status: 200, text: text}
				}
			}
		}
		return ai_result{status: 200, text: ""}
	}

	if resp.StatusCode == 401 {
		return ai_result{status: 401, text: ""}
	}
	if resp.StatusCode == 429 {
		return ai_result{status: 429, text: ""}
	}

	// Check for model not found
	var errData map[string]any
	if json.Unmarshal(body, &errData) == nil {
		if error_object, ok := errData["error"].(map[string]any); ok {
			if error_type, _ := error_object["type"].(string); error_type == "not_found_error" {
				return ai_result{status: 404, text: ""}
			}
		}
	}

	return ai_result{status: resp.StatusCode, text: ""}
}

// ai_call_openai sends a prompt to the OpenAI API
func ai_call_openai(api_key, model, prompt string) ai_result {
	payload, _ := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})

	req, err := http.NewRequest("POST", "https://api.openai.com/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return ai_result{status: 500, text: ""}
	}
	req.Header.Set("Authorization", "Bearer "+api_key)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return ai_result{status: 500, text: ""}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == 200 {
		var data map[string]any
		if json.Unmarshal(body, &data) == nil {
			if choices, ok := data["choices"].([]any); ok && len(choices) > 0 {
				if choice, ok := choices[0].(map[string]any); ok {
					if msg, ok := choice["message"].(map[string]any); ok {
						text, _ := msg["content"].(string)
						return ai_result{status: 200, text: text}
					}
				}
			}
		}
		return ai_result{status: 200, text: ""}
	}

	if resp.StatusCode == 401 {
		return ai_result{status: 401, text: ""}
	}
	if resp.StatusCode == 429 {
		return ai_result{status: 429, text: ""}
	}

	// Check for model not found
	var errData map[string]any
	if json.Unmarshal(body, &errData) == nil {
		if error_object, ok := errData["error"].(map[string]any); ok {
			if code, _ := error_object["code"].(string); code == "model_not_found" {
				return ai_result{status: 404, text: ""}
			}
		}
	}

	return ai_result{status: resp.StatusCode, text: ""}
}
