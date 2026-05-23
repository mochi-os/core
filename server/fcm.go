// Mochi server: Firebase Cloud Messaging (FCM) delivery
// Copyright Alistair Cunningham 2026
//
// FCM HTTP v1 push delivery. The server admin pastes a Firebase service
// account JSON into the "fcm.service_account" setting; we mint an OAuth2
// access token (RS256-signed JWT exchanged for a Bearer token), cache it
// for ~50 min, and POST the message envelope to
// https://fcm.googleapis.com/v1/projects/<project_id>/messages:send.
//
// Per-server Firebase project: each Mochi server has its own; the client
// learns the public-facing config via the menu app's push/setup action and
// initialises Firebase Messaging against the matching project. Resulting
// token is stored as a per-user account with type="fcm" (set by
// function_push_register_fcm) and looked up by account_deliver_fcm.

package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	fcm_send_url_format     = "https://fcm.googleapis.com/v1/projects/%s/messages:send"
	fcm_oauth_token_url     = "https://oauth2.googleapis.com/token"
	fcm_oauth_scope         = "https://www.googleapis.com/auth/firebase.messaging"
	fcm_access_token_margin = 5 * time.Minute
)

// fcm_service_account is the parsed shape of the service-account JSON. Only
// the fields we need; the JSON file has more.
type fcm_service_account struct {
	Type           string `json:"type"`
	ProjectID      string `json:"project_id"`
	PrivateKeyID   string `json:"private_key_id"`
	PrivateKey     string `json:"private_key"`
	ClientEmail    string `json:"client_email"`
	TokenURI       string `json:"token_uri"`
	UniverseDomain string `json:"universe_domain"`
}

// fcm_access_token_cache memoises the OAuth2 Bearer token to avoid minting
// a new one for every push. The token Google returns is valid for ~1h; we
// renew when within fcm_access_token_margin of expiry.
type fcm_access_token_record struct {
	token   string
	expires time.Time
}

var (
	fcm_access_token_mu    sync.Mutex
	fcm_access_token_cache fcm_access_token_record
	// fcm_access_token_cache_key is the SHA-256 of the service account
	// JSON; on credentials rotation it changes and the cache is dropped.
	fcm_access_token_cache_key string
)

// account_deliver_fcm sends a notification to one user's FCM token. Token
// lives in the account row's data blob (`{"token": "..."}`), set by
// notifications/push/register/fcm.
//
// Returns (success, retire, detail).
//   - retire=true means the token is permanently dead (Google returned
//     404 UNREGISTERED, INVALID_ARGUMENT on token, or the row's data has
//     no token at all) and api_account_notify should delete the row.
//   - detail is a short human-readable failure reason for surfaces like
//     the connected-accounts "Test" button. Empty on success.
func account_deliver_fcm(data map[string]any, title, body, link, tag, app, id string) (success bool, retire bool, detail string) {
	token, _ := data["token"].(string)
	if token == "" {
		return false, true, "Account has no token"
	}

	sa_raw := setting_get("fcm.service_account", "")
	if sa_raw == "" {
		warn("FCM: account_deliver_fcm called but fcm.service_account is empty")
		return false, false, "FCM service account not configured"
	}
	sa, err := fcm_parse_service_account(sa_raw)
	if err != nil {
		warn("FCM: parse service account: %v", err)
		return false, false, fmt.Sprintf("Service account JSON invalid: %v", err)
	}

	access_token, err := fcm_access_token(sa)
	if err != nil {
		warn("FCM: mint access token: %v", err)
		return false, false, fmt.Sprintf("OAuth2 token mint failed: %v", err)
	}

	envelope := map[string]any{
		"message": map[string]any{
			"token": token,
			// Data-only message so the Android side
			// (MochiFirebaseMessagingService) keeps control of channel
			// routing and the pending intent shape. No "notification"
			// field, which would let Android post a default-styled
			// notification on its own.
			"data": map[string]string{
				"title": title,
				"body":  body,
				"link":  link,
				"tag":   tag,
				"app":   app,
				"id":    id,
			},
		},
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		warn("FCM: marshal envelope: %v", err)
		return false, false, fmt.Sprintf("Envelope marshal failed: %v", err)
	}

	url := fmt.Sprintf(fcm_send_url_format, sa.ProjectID)
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		warn("FCM: build request: %v", err)
		return false, false, fmt.Sprintf("Request build failed: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+access_token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		warn("FCM: send: %v", err)
		return false, false, fmt.Sprintf("Network error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return true, false, ""
	}
	body_bytes, _ := io.ReadAll(resp.Body)
	warn("FCM: send returned %d: %s", resp.StatusCode, string(body_bytes))
	// Permanent-failure classification — drop the row so the next
	// register from the phone creates a fresh one rather than the upsert
	// path resurrecting the dead token. 404 UNREGISTERED is the canonical
	// "the app was uninstalled or the token was rotated by Google"
	// signal; INVALID_ARGUMENT on the token field means malformed.
	retire = resp.StatusCode == 404 ||
		(resp.StatusCode == 400 && bytes.Contains(body_bytes, []byte("INVALID_ARGUMENT")))
	return false, retire, fcm_summarise_error(resp.StatusCode, body_bytes)
}

// fcm_summarise_error extracts the most useful identifier from an FCM v1
// error response so the connected-accounts "Test" surface shows
// "UNREGISTERED" rather than the generic "Push notification failed".
// Falls back to the HTTP status when the body doesn't parse.
func fcm_summarise_error(status int, body []byte) string {
	var parsed struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
			Details []struct {
				Type      string `json:"@type"`
				ErrorCode string `json:"errorCode"`
			} `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		for _, d := range parsed.Error.Details {
			if d.ErrorCode != "" {
				return fmt.Sprintf("FCM %d %s", status, d.ErrorCode)
			}
		}
		if parsed.Error.Status != "" {
			return fmt.Sprintf("FCM %d %s", status, parsed.Error.Status)
		}
	}
	return fmt.Sprintf("FCM %d", status)
}

func fcm_parse_service_account(raw string) (*fcm_service_account, error) {
	var sa fcm_service_account
	if err := json.Unmarshal([]byte(raw), &sa); err != nil {
		return nil, err
	}
	if sa.Type != "service_account" {
		return nil, errors.New("not a service_account JSON")
	}
	if sa.ProjectID == "" || sa.ClientEmail == "" || sa.PrivateKey == "" {
		return nil, errors.New("service account JSON missing project_id / client_email / private_key")
	}
	if sa.TokenURI == "" {
		sa.TokenURI = fcm_oauth_token_url
	}
	return &sa, nil
}

// fcm_access_token returns a cached or freshly-minted OAuth2 Bearer token
// authorising sends against sa.ProjectID. Mints by signing a JWT with the
// service account's private key (RS256) and exchanging it at the OAuth2
// token endpoint.
func fcm_access_token(sa *fcm_service_account) (string, error) {
	fcm_access_token_mu.Lock()
	defer fcm_access_token_mu.Unlock()

	hash := sha256.Sum256([]byte(sa.PrivateKey + "|" + sa.ClientEmail))
	cache_key := base64.RawURLEncoding.EncodeToString(hash[:])

	if fcm_access_token_cache_key == cache_key &&
		time.Now().Add(fcm_access_token_margin).Before(fcm_access_token_cache.expires) {
		return fcm_access_token_cache.token, nil
	}

	token, expires, err := fcm_mint_access_token(sa)
	if err != nil {
		return "", err
	}
	fcm_access_token_cache = fcm_access_token_record{token: token, expires: expires}
	fcm_access_token_cache_key = cache_key
	return token, nil
}

func fcm_mint_access_token(sa *fcm_service_account) (string, time.Time, error) {
	private_key, err := fcm_parse_private_key(sa.PrivateKey)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("parse private key: %w", err)
	}

	now := time.Now()
	header := map[string]string{"alg": "RS256", "typ": "JWT", "kid": sa.PrivateKeyID}
	claims := map[string]any{
		"iss":   sa.ClientEmail,
		"scope": fcm_oauth_scope,
		"aud":   sa.TokenURI,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	}

	header_json, _ := json.Marshal(header)
	claims_json, _ := json.Marshal(claims)
	signing_input := base64.RawURLEncoding.EncodeToString(header_json) + "." +
		base64.RawURLEncoding.EncodeToString(claims_json)

	digest := sha256.Sum256([]byte(signing_input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, private_key, crypto.SHA256, digest[:])
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign jwt: %w", err)
	}
	assertion := signing_input + "." + base64.RawURLEncoding.EncodeToString(signature)

	form := strings.NewReader(
		"grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer" +
			"&assertion=" + assertion,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", sa.TokenURI, form)
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", time.Time{}, fmt.Errorf("oauth2 token endpoint returned %d: %s",
			resp.StatusCode, string(body))
	}
	var token_response struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &token_response); err != nil {
		return "", time.Time{}, fmt.Errorf("parse oauth2 response: %w", err)
	}
	if token_response.AccessToken == "" {
		return "", time.Time{}, errors.New("oauth2 response missing access_token")
	}
	expires := time.Now().Add(time.Duration(token_response.ExpiresIn) * time.Second)
	return token_response.AccessToken, expires, nil
}

func fcm_parse_private_key(pem_text string) (*rsa.PrivateKey, error) {
	// Service-account JSON encodes the PEM with literal "\n"; the Unmarshal
	// step already restored them to real newlines, so block decode is direct.
	block, _ := pem.Decode([]byte(pem_text))
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsa_key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return rsa_key, nil
}
