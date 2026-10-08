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
	"strings"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_ai = sls.FromStringDict(sl.String("mochi.ai"), sl.StringDict{
	"prompt": sl.NewBuiltin("mochi.ai.prompt", api_ai_prompt),
})

// ai_prompt_maximum bounds the prompt a single call may carry. The request body
// limit is the only other ceiling, and it is orders of magnitude larger than any
// real prompt; apps fold user-supplied text into prompts (forums scores fifty
// post bodies at a time), so the bound belongs here rather than on each caller.
const ai_prompt_maximum = 65536

// ai_tokens_default and ai_tokens_maximum bound the output a call may ask for.
// The size was fixed at the maximum, so every call paid for a 16k ceiling it
// almost never used and no caller could ask for less.
const (
	ai_tokens_default = 16384
	ai_tokens_maximum = 16384
)

// Default models for each AI provider. An account with no model of its own
// follows these, so moving a default here moves every such account at the next
// release. No provider ID tracks "the latest" model: each one names a fixed
// model, and a new generation can change what a response looks like.
var ai_provider_defaults = map[string]string{
	"claude": "claude-haiku-5-5",
	"openai": "gpt-4o-mini",
}

// ai_claude_effort is the thinking effort core asks of its default Claude
// model. Callers send short tagging, scoring and summary prompts, which Haiku
// 4.5 answered without thinking; low is the nearest setting, and it also
// leaves a small token request room for the answer after any thinking. A
// model the account names runs at that model's own default, since older
// models refuse the parameter.
const ai_claude_effort = "low"

// ai_status_refused is the status a call answers when the provider's safety
// checks decline the prompt. The provider itself answers 200 with no text, which
// a caller checking only the status would read as an empty success.
const ai_status_refused = 422

// ai_result holds the response from an AI provider call
type ai_result struct {
	status int
	text   string
}

// ai_account resolves the account whose key an AI call spends. An id names one
// account; otherwise it is the one the user designated as their default for
// "ai". Both callers have to agree on this: interests_ai_summary used to take
// the first enabled AI-capable account ordered by id, so a user with two
// accounts had their summary billed to whichever they connected first rather
// than the one they chose. An empty key means no usable account.
func ai_account(database *DB, id string) (provider, key, model string) {
	var row map[string]any
	if id != "" {
		row, _ = database.row("select type, data, enabled from accounts where id=?", id)
	} else {
		row, _ = database.row("select type, data, enabled from accounts where (',' || \"default\" || ',') like '%,ai,%' and enabled=1")
	}
	if row == nil {
		return "", "", ""
	}
	if enabled, _ := row["enabled"].(int64); enabled != 1 {
		return "", "", ""
	}
	provider, _ = row["type"].(string)
	if !provider_has_capability(provider, "ai") {
		return "", "", ""
	}
	raw, _ := row["data"].(string)
	var data map[string]any
	if raw != "" {
		json.Unmarshal([]byte(raw), &data)
	}
	key, _ = data["api_key"].(string)
	model, _ = data["model"].(string)
	if model == "" || model == "default" {
		model = ai_provider_defaults[provider]
	}
	return provider, key, model
}

// mochi.ai.prompt(prompt, account?) -> dict: Send a prompt to an AI provider
func api_ai_prompt(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <prompt: string>, [account=<int>]")
	}

	if err := require_permission_acting(t, fn, "accounts/ai"); err != nil {
		return sl_encode(map[string]any{"status": 403, "text": ""}), nil
	}

	prompt, ok := sl.AsString(args[0])
	if !ok || prompt == "" {
		return sl_error(fn, "invalid prompt")
	}
	if len(prompt) > ai_prompt_maximum {
		return sl_error(fn, "prompt too long")
	}

	// Parse optional account and tokens kwargs
	account_id := ""
	tokens := ai_tokens_default
	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		switch key {
		case "account":
			id, ok := account_id_arg(kv[1])
			if !ok {
				return sl_error(fn, "invalid account id")
			}
			account_id = id
		case "tokens":
			n, err := sl.AsInt32(kv[1])
			if err != nil || n < 1 || int(n) > ai_tokens_maximum {
				return sl_error(fn, "invalid tokens")
			}
			tokens = int(n)
		}
	}

	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")

	provider, api_key, model := ai_account(db, account_id)
	if api_key == "" {
		return sl_encode(map[string]any{"status": 0, "text": ""}), nil
	}

	result := ai_call(provider, api_key, model, prompt, tokens)

	// Model fallback: if model not found and not already using default, retry with default
	if result.status == 404 && model != ai_provider_defaults[provider] {
		debug("ai: model %q not found for %s, falling back to default %q", model, provider, ai_provider_defaults[provider])
		result = ai_call(provider, api_key, ai_provider_defaults[provider], prompt, tokens)
	}

	return sl_encode(map[string]any{"status": result.status, "text": result.text}), nil
}

// ai_call sends the prompt to the account's provider. An unknown provider
// answers status 0, the same as a missing account. A variable so a test can
// stand in for the provider without reaching the network.
var ai_call = func(provider, api_key, model, prompt string, tokens int) ai_result {
	switch provider {
	case "claude":
		return ai_call_claude(api_key, model, prompt, tokens)
	case "openai":
		return ai_call_openai(api_key, model, prompt, tokens)
	}
	return ai_result{}
}

// ai_call_claude sends a prompt to the Claude (Anthropic) API
func ai_call_claude(api_key, model, prompt string, tokens int) ai_result {
	payload, _ := json.Marshal(ai_claude_request(model, prompt, tokens))

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
		return ai_claude_answer(body)
	}

	if resp.StatusCode == 401 {
		return ai_result{status: 401, text: ""}
	}
	if resp.StatusCode == 429 {
		return ai_result{status: 429, text: ""}
	}

	// Check for model not found
	var error_data map[string]any
	if json.Unmarshal(body, &error_data) == nil {
		if error_object, ok := error_data["error"].(map[string]any); ok {
			if error_type, _ := error_object["type"].(string); error_type == "not_found_error" {
				return ai_result{status: 404, text: ""}
			}
		}
	}

	return ai_result{status: resp.StatusCode, text: ""}
}

// ai_claude_request builds the body of a Claude call. Only core's default
// model is asked for an effort level; see ai_claude_effort.
func ai_claude_request(model, prompt string, tokens int) map[string]any {
	request := map[string]any{
		"model":      model,
		"max_tokens": tokens,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}
	if model == ai_provider_defaults["claude"] {
		request["output_config"] = map[string]any{"effort": ai_claude_effort}
	}
	return request
}

// ai_claude_answer reads the text of a successful Claude response. The answer
// is every text block joined, chosen by type rather than position: a model
// that thinks returns its thinking blocks first, and those carry no text.
func ai_claude_answer(body []byte) ai_result {
	var data map[string]any
	if json.Unmarshal(body, &data) != nil {
		return ai_result{status: 200, text: ""}
	}
	if reason, _ := data["stop_reason"].(string); reason == "refusal" {
		return ai_result{status: ai_status_refused, text: ""}
	}
	var text strings.Builder
	content, _ := data["content"].([]any)
	for _, item := range content {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if kind, _ := block["type"].(string); kind == "text" {
			part, _ := block["text"].(string)
			text.WriteString(part)
		}
	}
	return ai_result{status: 200, text: text.String()}
}

// ai_call_openai sends a prompt to the OpenAI API
func ai_call_openai(api_key, model, prompt string, tokens int) ai_result {
	payload, _ := json.Marshal(map[string]any{
		"model":                 model,
		"max_completion_tokens": tokens,
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
					if message, ok := choice["message"].(map[string]any); ok {
						text, _ := message["content"].(string)
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
	var error_data map[string]any
	if json.Unmarshal(body, &error_data) == nil {
		if error_object, ok := error_data["error"].(map[string]any); ok {
			if code, _ := error_object["code"].(string); code == "model_not_found" {
				return ai_result{status: 404, text: ""}
			}
		}
	}

	return ai_result{status: resp.StatusCode, text: ""}
}
