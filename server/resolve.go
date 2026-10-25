// Mochi server: People on other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// An author in feeds, forums and chat is either a Mochi identity or a person's
// own permanent identifier on another network, stored as it is: an ActivityPub
// account URL, a Bluesky DID, a Matrix address, an XMPP address with its
// scheme. The two kinds cannot be confused, since a Mochi identity never
// contains a colon and each of these does. Apps validate an author with
// mochi.text.valid(..., "author") and show one with mochi.account.resolve, which
// turns the identifier into a name, an avatar and a readable address fetched
// from the network and cached. The raw identifier is never displayed.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
)

// The identifier forms, by network.
var resolve_did_re = regexp.MustCompile(`^did:(plc:[a-z2-7]{24}|web:[a-z0-9.-]+(%3A[0-9]+)?(:[a-zA-Z0-9._-]+)*)$`)
var resolve_matrix_re = regexp.MustCompile(`^@[^@:\s/]{1,255}:[a-zA-Z0-9.-]+(:[0-9]+)?$`)
var resolve_xmpp_re = regexp.MustCompile(`^xmpp:[^@\s/]{1,255}@[a-zA-Z0-9.-]+$`)
var resolve_acct_re = regexp.MustCompile(`^@?([^@\s/]{1,255})@([a-zA-Z0-9.-]+)$`)

// author_network says which network an identifier belongs to - "fediverse",
// "bluesky", "matrix" or "xmpp" - or empty for none of them.
func author_network(identifier string) string {
	switch {
	case strings.HasPrefix(identifier, "https://"), url_allow_private && strings.HasPrefix(identifier, "http://"):
		// An http id is a stand-in server on loopback under the development
		// switch that admits private addresses; nowhere else.
		u, err := url.Parse(identifier)
		if err != nil || u.Host == "" || u.User != nil || len(identifier) > 2048 || strings.ContainsAny(identifier, " \t\r\n") {
			return ""
		}
		return "fediverse"
	case resolve_did_re.MatchString(identifier):
		return "bluesky"
	case resolve_matrix_re.MatchString(identifier):
		return "matrix"
	case resolve_xmpp_re.MatchString(identifier):
		return "xmpp"
	}
	return ""
}

// author_valid is the "author" validator: a Mochi identity or a network
// identifier of a known kind.
func author_valid(s string) bool {
	return valid(s, "entity") || author_network(s) != ""
}

// resolved is what the UI shows for a person on another network, and what
// sending to them needs.
type resolved struct {
	Network  string `json:"network"`
	Type     string `json:"type"`     // what the network says it is: person, group, service
	Address  string `json:"address"`  // readable, such as @bob@lemmy.world
	Username string `json:"username"` // the name on its server, without the host
	Host     string `json:"host"`     // the server the identifier names, where it names one
	Server   string `json:"server"`   // where the person's records are read: a Bluesky data server
	Name     string `json:"name"`
	Avatar   string `json:"avatar"`
	URL      string `json:"url"`    // the person's page, where the network has one
	ID       string `json:"id"`     // the person's permanent identifier
	Inbox    string `json:"inbox"`  // where to deliver to the person, on the fediverse
	Shared   string `json:"shared"` // their server's shared inbox, where it has one
	Outbox   string `json:"outbox"` // where the person's posts are read, on the fediverse
	// Template is the server's subscribe template from WebFinger, where it
	// publishes one: the page a visitor is sent to with {uri} to follow or
	// reply to someone from their own account.
	Template string `json:"template"`
	// Locked is a fediverse account that approves its followers one by one,
	// so a Follow waits for its Accept.
	Locked bool `json:"locked"`
	// Summary is the actor's own description, as HTML; Sensitive marks a
	// group whose posts are all taken as sensitive.
	Summary   string `json:"summary"`
	Sensitive bool   `json:"sensitive"`
	// Aliases are the other identifiers the person says are theirs
	// (alsoKnownAs), which a Move from one of them must name; Moved is where
	// the person has moved to, where they have.
	Aliases []string `json:"aliases"`
	Moved   string   `json:"moved"`
}

// resolved_host is the host the host policy judges a resolved person by: the
// one their identifier names, else their data server's, since a did:plc names
// no host of its own.
func resolved_host(r *resolved) string {
	if host := identifier_host(r.ID); host != "" {
		return host
	}
	return identifier_host(r.Server)
}

// identifier_host is the host an identifier names, for the host policy: empty
// where the identifier names none, as a did:plc does.
func identifier_host(identifier string) string {
	host := ""
	switch {
	case strings.HasPrefix(identifier, "https://"), url_allow_private && strings.HasPrefix(identifier, "http://"):
		if u, err := url.Parse(identifier); err == nil {
			host = u.Hostname()
		}
	case strings.HasPrefix(identifier, "xmpp:"):
		if i := strings.LastIndexByte(identifier, '@'); i >= 0 {
			host = identifier[i+1:]
		}
	case strings.HasPrefix(identifier, "did:web:"):
		host = strings.ReplaceAll(strings.SplitN(strings.TrimPrefix(identifier, "did:web:"), ":", 2)[0], "%3A", ":")
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	case resolve_matrix_re.MatchString(identifier):
		host = identifier[strings.IndexByte(identifier, ':')+1:]
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	default:
		if m := resolve_acct_re.FindStringSubmatch(identifier); m != nil {
			host = m[2]
		} else if resolve_handle_form(identifier) {
			host = identifier
		}
	}
	return strings.ToLower(host)
}

// resolve_handle_form says whether a string is shaped as a Bluesky handle and
// as no other identifier: domain labels with dots between, no @, no scheme.
func resolve_handle_form(s string) bool {
	return !strings.ContainsAny(s, "@:/") && bluesky_handle_re.MatchString(strings.ToLower(s))
}

// resolve_network_of says which network a lookup string belongs to: an author
// identifier's network, else the fediverse for a handle such as bob@lemmy.world
// and Bluesky for one such as alice.bsky.social.
func resolve_network_of(identifier string) string {
	network := author_network(identifier)
	switch {
	case network != "":
	case resolve_acct_re.MatchString(identifier) && !strings.HasPrefix(identifier, "xmpp:"):
		network = "fediverse"
	case resolve_handle_form(identifier):
		network = "bluesky"
	}
	return network
}

// resolve_allowed is the host policy for a lookup: the identifier's network is
// on, the owner does not block the person, and their host is blocked by
// neither the server nor the user. The user's own blocks are the caller's;
// the owner's and the limiter apply inside the fetch, to every host it
// reaches.
func resolve_allowed(user *User, identifier string) error {
	network := resolve_network_of(identifier)
	if server_blocked(identifier) {
		return &host_refusal{Host: identifier_host(identifier), Reason: identifier + " is blocked on this server"}
	}
	if host := identifier_host(identifier); host != "" {
		return host_allowed(user, network, host)
	}
	if network != "" && !network_enabled(network) {
		return &host_refusal{Reason: network + " is off", Off: true}
	}
	return nil
}

// The cache: a lookup fetched from the network is kept for a day, a failed one
// for an hour, so a feed of a hundred comments by one person costs one fetch.
// A failure that may pass (resolve_transient: the host down or slow) is kept
// a minute, sparing the host the retries of a burst without answering "not
// found" for an hour after it is back, and while kept answers the same error,
// so a caller still sees it may pass. Bounded by count; when full the whole
// cache is dropped, which costs a refetch and nothing else.
const resolve_cache_success = 24 * time.Hour
const resolve_cache_failure = time.Hour
const resolve_cache_transient = time.Minute
const resolve_cache_maximum = 10000

type resolve_entry struct {
	value   *resolved
	expires time.Time
	failure error
}

// resolve_failure is how long a failed lookup is remembered.
func resolve_failure(err error) time.Duration {
	if resolve_transient(err) {
		return resolve_cache_transient
	}
	return resolve_cache_failure
}

var resolve_cache = map[string]resolve_entry{}
var resolve_lock sync.Mutex

// A resolve_signer signs a fetch as a hosted account, for a server that gives
// its documents to signed requests only. It travels in the context, put there
// by resolve_signing, so every fetch a lookup makes carries it.
type resolve_signer func(r *http.Request)

type resolve_signer_key struct{}

// resolve_signing returns a context whose fetches are signed by sign.
func resolve_signing(ctx context.Context, sign resolve_signer) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, resolve_signer_key{}, sign)
}

// resolve_signed_by says whether a context carries a signer.
func resolve_signed_by(ctx context.Context) resolve_signer {
	if ctx == nil {
		return nil
	}
	sign, _ := ctx.Value(resolve_signer_key{}).(resolve_signer)
	return sign
}

// resolve_fetch does the network request, under the host policy: the owner's
// domain blocks, the limiter per host and the private-address check, signed
// when the context carries a signer. A variable so tests substitute documents
// without a server.
var resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	request, err := http.NewRequestWithContext(ctx, "GET", address, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", accept)
	if sign := resolve_signed_by(ctx); sign != nil {
		sign(request)
	}
	response, err := network_request(ctx, nil, "", request, 5)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, &resolve_refusal{Status: response.StatusCode}
	}
	return io.ReadAll(io.LimitReader(response.Body, resolve_document_maximum))
}

// A resolve_refusal is a document the server would not give: its status.
type resolve_refusal struct {
	Status int
}

func (e *resolve_refusal) Error() string {
	return fmt.Sprintf("status %d", e.Status)
}

// resolve_transient says whether a lookup failed for a reason that may pass -
// our own limiter's refusal, a server error, no answer at all - as against an
// answer that there is no such person.
func resolve_transient(err error) bool {
	var limited *RateLimitError
	var refusal *resolve_refusal
	var network *url.Error
	switch {
	case errors.As(err, &limited):
		return true
	case errors.As(err, &refusal):
		return refusal.Status >= 500 || refusal.Status == 408 || refusal.Status == 429
	case errors.As(err, &network):
		return true
	}
	return false
}

// resolve_signed says whether a fetch failed for want of a signature: a
// server in secure mode answers an unsigned ActivityPub request with 401 or
// 403, as mastodon.social does.
func resolve_signed(err error) bool {
	var refusal *resolve_refusal
	return errors.As(err, &refusal) && (refusal.Status == 401 || refusal.Status == 403)
}

// resolve_document_maximum caps a profile document; actor documents run to a
// few kilobytes.
const resolve_document_maximum = 512 << 10

// author_resolve looks an identifier up, from the cache or the network.
func author_resolve(ctx context.Context, identifier string) (*resolved, error) {
	identifier = strings.TrimSpace(identifier)
	resolve_lock.Lock()
	if entry, found := resolve_cache[identifier]; found && time.Now().Before(entry.expires) {
		// A failure remembered from an unsigned fetch does not bind a signed
		// one: a server in secure mode answers the second where it refused
		// the first.
		if entry.value != nil || resolve_signed_by(ctx) == nil {
			resolve_lock.Unlock()
			if entry.value == nil {
				if entry.failure != nil {
					return nil, entry.failure
				}
				return nil, fmt.Errorf("not resolved")
			}
			return entry.value, nil
		}
	}
	resolve_lock.Unlock()

	value, err := resolve_network(ctx, identifier)

	resolve_lock.Lock()
	if len(resolve_cache) >= resolve_cache_maximum {
		resolve_cache = map[string]resolve_entry{}
	}
	var limited *RateLimitError
	if errors.As(err, &limited) {
		// Refused by our own limiter: nothing was learnt about the person,
		// and the next lookup may go ahead.
	} else if err != nil {
		resolve_cache[identifier] = resolve_entry{nil, time.Now().Add(resolve_failure(err)), err}
	} else {
		resolve_cache[identifier] = resolve_entry{value, time.Now().Add(resolve_cache_success), nil}
	}
	resolve_lock.Unlock()
	return value, err
}

// resolve_network fetches what the identifier's network publishes about the
// person.
func resolve_network(ctx context.Context, identifier string) (*resolved, error) {
	// A fediverse handle, @user@host or user@host, is turned into the account
	// URL by WebFinger first, and a Bluesky handle, alice.bsky.social, into its
	// DID; each is a convenience of lookup, not an author form.
	if m := resolve_acct_re.FindStringSubmatch(identifier); m != nil && !strings.HasPrefix(identifier, "xmpp:") {
		actor, page, template, err := resolve_webfinger(ctx, m[1], strings.ToLower(m[2]))
		if err != nil {
			return nil, err
		}
		r, err := resolve_activitypub(ctx, actor)
		if resolve_signed(err) {
			// A server in secure mode gives its actor documents to signed
			// requests only, which come with the hosted accounts of a later
			// phase. WebFinger has answered the address, the username and the
			// profile page, which is what following the person's web feed
			// needs, so the person resolves from that alone, as a person.
			return &resolved{Network: "fediverse", Type: "person", Address: "@" + m[1] + "@" + strings.ToLower(m[2]), Username: m[1], Host: strings.ToLower(m[2]), Name: m[1], URL: page, ID: actor, Template: template}, nil
		}
		if r != nil {
			r.Template = template
		}
		return r, err
	} else if resolve_handle_form(identifier) {
		did, err := resolve_handle(ctx, identifier)
		if err != nil {
			return nil, err
		}
		identifier = did
	}
	switch author_network(identifier) {
	case "fediverse":
		return resolve_activitypub(ctx, identifier)
	case "bluesky":
		return resolve_bluesky(ctx, identifier)
	case "matrix":
		return resolve_matrix(ctx, identifier)
	case "xmpp":
		return resolve_xmpp(identifier), nil
	}
	return nil, fmt.Errorf("not a network identifier")
}

// resolve_json fetches and decodes a JSON document.
func resolve_json(ctx context.Context, address string, accept string, into any) error {
	body, err := resolve_fetch(ctx, address, accept)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, into)
}

// resolve_webfinger turns user@host into the account URL the host publishes,
// the account's profile page where the host names one, and the host's
// subscribe template where it publishes one.
func resolve_webfinger(ctx context.Context, user string, host string) (string, string, string, error) {
	var document struct {
		Links []struct {
			Rel      string `json:"rel"`
			Type     string `json:"type"`
			Href     string `json:"href"`
			Template string `json:"template"`
		} `json:"links"`
	}
	address := network_base(host) + "/.well-known/webfinger?resource=" + url.QueryEscape("acct:"+user+"@"+host)
	if err := resolve_json(ctx, address, "application/jrd+json, application/json", &document); err != nil {
		return "", "", "", err
	}
	actor, page, template := "", "", ""
	for _, link := range document.Links {
		if link.Rel == activitypub_subscribe && strings.HasPrefix(link.Template, "https://") && strings.Contains(link.Template, "{uri}") {
			template = link.Template
			continue
		}
		if !strings.HasPrefix(link.Href, "https://") && !(url_allow_private && strings.HasPrefix(link.Href, "http://")) {
			continue
		}
		switch {
		case link.Rel == "self" && (strings.Contains(link.Type, "activity+json") || strings.Contains(link.Type, "ld+json")):
			actor = link.Href
		case link.Rel == "http://webfinger.net/rel/profile-page":
			page = link.Href
		}
	}
	if actor == "" {
		return "", "", "", fmt.Errorf("no account at %s@%s", user, host)
	}
	return actor, page, template, nil
}

// resolve_handle turns a Bluesky handle into its DID: the _atproto TXT record
// of the name, else the well-known file on it.
func resolve_handle(ctx context.Context, handle string) (string, error) {
	handle = strings.ToLower(strings.TrimSpace(handle))
	if records, err := bluesky_lookup(ctx, "_atproto."+handle); err == nil {
		for _, record := range records {
			if value, ok := strings.CutPrefix(strings.TrimSpace(record), "did="); ok && resolve_did_re.MatchString(value) {
				return value, nil
			}
		}
	}
	body, err := resolve_fetch(ctx, network_scheme+handle+"/.well-known/atproto-did", "text/plain")
	if err != nil {
		return "", fmt.Errorf("the handle %s does not resolve", handle)
	}
	did := strings.TrimSpace(string(body))
	if !resolve_did_re.MatchString(did) {
		return "", fmt.Errorf("the handle %s names no identity", handle)
	}
	return did, nil
}

// The actor types: a document of any other type is an object, not a person.
var resolve_actor_types = map[string]bool{"person": true, "group": true, "service": true, "application": true, "organization": true}

// resolve_activitypub reads an actor document: the person's preferred username,
// display name and icon.
func resolve_activitypub(ctx context.Context, actor string) (*resolved, error) {
	var document struct {
		ID                string `json:"id"`
		Type              string `json:"type"`
		PreferredUsername string `json:"preferredUsername"`
		Name              string `json:"name"`
		URL               any    `json:"url"`
		Icon              any    `json:"icon"`
		Inbox             string `json:"inbox"`
		Outbox            string `json:"outbox"`
		Endpoints         struct {
			SharedInbox string `json:"sharedInbox"`
		} `json:"endpoints"`
		ManuallyApprovesFollowers bool   `json:"manuallyApprovesFollowers"`
		AlsoKnownAs               any    `json:"alsoKnownAs"`
		MovedTo                   string `json:"movedTo"`
		Summary                   string `json:"summary"`
		Sensitive                 bool   `json:"sensitive"`
	}
	if err := resolve_json(ctx, actor, "application/activity+json, application/ld+json", &document); err != nil {
		return nil, err
	}
	kind := strings.ToLower(document.Type)
	if !resolve_actor_types[kind] {
		return nil, fmt.Errorf("%s is not a person", actor)
	}
	host, hostname := "", ""
	if u, err := url.Parse(actor); err == nil {
		host = strings.ToLower(u.Host)
		hostname = strings.ToLower(u.Hostname())
	}
	// An actor's document must be served by the host its id names, so no
	// server can claim an id on another domain.
	if document.ID != "" && identifier_host(document.ID) != hostname {
		return nil, fmt.Errorf("actor id %s served by %s", document.ID, host)
	}
	id := document.ID
	if id == "" {
		id = actor
	}
	r := &resolved{Network: "fediverse", Type: kind, Username: document.PreferredUsername, Host: host, Name: document.Name, URL: actor, ID: id, Inbox: document.Inbox, Shared: document.Endpoints.SharedInbox, Outbox: document.Outbox, Locked: document.ManuallyApprovesFollowers, Moved: document.MovedTo, Summary: document.Summary, Sensitive: document.Sensitive}
	for _, alias := range object_strings(document.AlsoKnownAs) {
		if activitypub_remote_ok(alias) {
			r.Aliases = append(r.Aliases, alias)
		}
	}
	if document.PreferredUsername != "" {
		r.Address = "@" + document.PreferredUsername + "@" + host
	} else {
		r.Address = actor
	}
	if r.Name == "" {
		r.Name = document.PreferredUsername
	}
	if page := resolve_first_url(document.URL); page != "" {
		r.URL = page
	}
	r.Avatar = resolve_icon(document.Icon)
	return r, nil
}

// resolve_first_url reads an ActivityPub url or attributedTo property, a
// string or a list of strings, Link objects or objects named by id, and
// returns the first https URL in it.
func resolve_first_url(v any) string {
	switch value := v.(type) {
	case string:
		if strings.HasPrefix(value, "https://") {
			return value
		}
	case []any:
		for _, item := range value {
			if found := resolve_first_url(item); found != "" {
				return found
			}
		}
	case map[string]any:
		for _, key := range []string{"href", "id"} {
			if found, _ := value[key].(string); strings.HasPrefix(found, "https://") {
				return found
			}
		}
	}
	return ""
}

// resolve_icon reads an actor's icon, an Image object or a list of them.
func resolve_icon(v any) string {
	switch value := v.(type) {
	case map[string]any:
		if address, _ := value["url"].(string); strings.HasPrefix(address, "https://") {
			return address
		}
	case []any:
		for _, item := range value {
			if found := resolve_icon(item); found != "" {
				return found
			}
		}
	}
	return ""
}

// resolve_bluesky reads the DID document for the account's handle and data
// server, then its profile record there.
func resolve_bluesky(ctx context.Context, did string) (*resolved, error) {
	var document struct {
		AlsoKnownAs []string `json:"alsoKnownAs"`
		Service     []struct {
			ID              string `json:"id"`
			Type            string `json:"type"`
			ServiceEndpoint string `json:"serviceEndpoint"`
		} `json:"service"`
	}
	address := strings.TrimSuffix(bluesky_plc, "/") + "/" + did
	if strings.HasPrefix(did, "did:web:") {
		host := strings.ReplaceAll(strings.TrimPrefix(did, "did:web:"), "%3A", ":")
		address = "https://" + host + "/.well-known/did.json"
	}
	if err := resolve_json(ctx, address, "application/did+ld+json, application/json", &document); err != nil {
		return nil, err
	}
	r := &resolved{Network: "bluesky", Type: "person", Address: did, Host: identifier_host(did), ID: did}
	for _, known := range document.AlsoKnownAs {
		if handle, ok := strings.CutPrefix(known, "at://"); ok {
			r.Address = "@" + handle
			r.Username = handle
			r.URL = "https://bsky.app/profile/" + handle
			break
		}
	}
	// A data server is reached over https, or over http where a development
	// instance may reach private addresses, as a reference fleet's is.
	pds := ""
	for _, service := range document.Service {
		if service.ID == "#atproto_pds" && (strings.HasPrefix(service.ServiceEndpoint, network_scheme) || url_allow_private && strings.HasPrefix(service.ServiceEndpoint, "http://")) {
			pds = strings.TrimSuffix(service.ServiceEndpoint, "/")
		}
	}
	if pds == "" {
		return r, nil
	}
	r.Server = pds
	var record struct {
		Value struct {
			DisplayName string `json:"displayName"`
			Avatar      struct {
				Ref struct {
					Link string `json:"$link"`
				} `json:"ref"`
			} `json:"avatar"`
		} `json:"value"`
	}
	profile := pds + "/xrpc/com.atproto.repo.getRecord?repo=" + url.QueryEscape(did) + "&collection=app.bsky.actor.profile&rkey=self"
	if err := resolve_json(ctx, profile, "application/json", &record); err == nil {
		r.Name = record.Value.DisplayName
		if cid := record.Value.Avatar.Ref.Link; cid != "" {
			r.Avatar = bluesky_blob(pds, did, cid)
		}
	}
	if r.Name == "" {
		r.Name = strings.TrimPrefix(r.Address, "@")
	}
	return r, nil
}

// resolve_matrix finds the home server through its client well-known and reads
// the public profile.
func resolve_matrix(ctx context.Context, address string) (*resolved, error) {
	server := address[strings.IndexByte(address, ':')+1:]
	base := "https://" + server
	var known struct {
		Homeserver struct {
			URL string `json:"base_url"`
		} `json:"m.homeserver"`
	}
	if err := resolve_json(ctx, base+"/.well-known/matrix/client", "application/json", &known); err == nil && strings.HasPrefix(known.Homeserver.URL, "https://") {
		base = strings.TrimSuffix(known.Homeserver.URL, "/")
	}
	var profile struct {
		Name   string `json:"displayname"`
		Avatar string `json:"avatar_url"`
	}
	if err := resolve_json(ctx, base+"/_matrix/client/v3/profile/"+url.PathEscape(address), "application/json", &profile); err != nil {
		return nil, err
	}
	r := &resolved{Network: "matrix", Address: address, Name: profile.Name, URL: "https://matrix.to/#/" + address}
	if r.Name == "" {
		r.Name = address[1:strings.IndexByte(address, ':')]
	}
	// An mxc:// avatar is served by the home server's media endpoint.
	if rest, ok := strings.CutPrefix(profile.Avatar, "mxc://"); ok && strings.Count(rest, "/") == 1 {
		r.Avatar = base + "/_matrix/client/v1/media/download/" + rest
	}
	return r, nil
}

// resolve_xmpp needs no request: an XMPP address is readable as it is, and a
// profile is fetched over a stream, which comes with the XMPP engine.
func resolve_xmpp(identifier string) *resolved {
	address := strings.TrimPrefix(identifier, "xmpp:")
	return &resolved{Network: "xmpp", Address: address, Name: address[:strings.IndexByte(address, '@')], URL: identifier}
}

// mochi.account.resolve(identifier) -> dict | None: What the UI shows for a
// person on another network - network, address, name, avatar and url - with
// the permanent id, the type the network gives (person, group, service), the
// username and the host, fetched from the network and cached; on the
// fediverse also the outbox, the summary as HTML and, for a group, whether it
// is sensitive. The identifier is an author identifier, a fediverse handle
// such as bob@lemmy.world or a Bluesky handle such as alice.bsky.social. None
// when it is none of these or the network did not answer.
func api_account_resolve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <identifier: string>")
	}
	user := principal_caller(t)
	if user == nil {
		// A public action a visitor reaches with no user of their own, such
		// as a feed's follow and reply controls, looks up under the owner's
		// policy: the owner's blocks, the host limiter, nothing of the visitor.
		user, _ = principal_storage(t)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}
	identifier, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid identifier")
	}
	identifier = strings.TrimSpace(identifier)
	if resolve_allowed(user, identifier) != nil {
		return sl.None, nil
	}
	// A user with a hosted fediverse account looks up as it, signed: a
	// server that gives its documents to signed requests only (Mastodon's
	// secure mode, GoToSocial) then answers with the person's name and
	// avatar rather than refusing.
	ctx := thread_context(t)
	for _, row := range accounts_hosted(user, "fediverse") {
		if id, _ := row["id"].(string); id != "" && account_actor(user, id) != "" {
			ctx = activitypub_signing(ctx, user, id)
			break
		}
	}
	r, err := author_resolve(ctx, identifier)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(map[string]any{
		"network":   r.Network,
		"type":      r.Type,
		"id":        r.ID,
		"address":   r.Address,
		"username":  r.Username,
		"host":      r.Host,
		"name":      r.Name,
		"avatar":    r.Avatar,
		"url":       r.URL,
		"subscribe": r.Template,
		"locked":    r.Locked,
		"moved":     r.Moved,
		"aliases":   r.Aliases,
		"outbox":    r.Outbox,
		"summary":   r.Summary,
		"sensitive": r.Sensitive,
	}), nil
}

// fediverse_public_posts reads the first page of a fediverse account's
// outbox, or the page the cursor names: the objects of its Create activities
// that are posts and not replies, newest first, as the account's server
// gives them, and each Announce as {type, actor, object, published} with the
// boosted object's id, for the reader to fetch. The cursor is the next page's
// address, empty at the end. The context's signer, where the caller is a
// hosted account, reaches a server that gives its documents to signed
// requests only.
func fediverse_public_posts(ctx context.Context, actor string, cursor string) (*linked_page, error) {
	address := cursor
	if address == "" {
		r, err := author_resolve(ctx, actor)
		if err != nil {
			return nil, err
		}
		if r.Outbox == "" || identifier_host(r.Outbox) != identifier_host(r.ID) {
			return nil, fmt.Errorf("%s names no outbox", actor)
		}
		var outbox struct {
			First        any   `json:"first"`
			OrderedItems []any `json:"orderedItems"`
			Next         any   `json:"next"`
		}
		if err := resolve_json(ctx, r.Outbox, "application/activity+json, application/ld+json", &outbox); err != nil {
			return nil, err
		}
		// A collection may carry its items itself, or name its first page.
		if len(outbox.OrderedItems) > 0 {
			return fediverse_outbox_page(actor, outbox.OrderedItems, outbox.Next), nil
		}
		if first, ok := outbox.First.(map[string]any); ok {
			if items, ok := first["orderedItems"].([]any); ok {
				return fediverse_outbox_page(actor, items, first["next"]), nil
			}
		}
		address = resolve_first_url(outbox.First)
		if address == "" {
			return &linked_page{Items: []any{}}, nil
		}
	}
	if identifier_host(address) != identifier_host(actor) {
		return nil, fmt.Errorf("outbox page %s is not on %s", address, identifier_host(actor))
	}
	var page struct {
		OrderedItems []any `json:"orderedItems"`
		Next         any   `json:"next"`
	}
	if err := resolve_json(ctx, address, "application/activity+json, application/ld+json", &page); err != nil {
		return nil, err
	}
	return fediverse_outbox_page(actor, page.OrderedItems, page.Next), nil
}

// fediverse_outbox_page shapes an outbox page's activities: the objects of
// the Creates by the actor that are posts and not replies, and the Announces.
func fediverse_outbox_page(actor string, activities []any, next any) *linked_page {
	page := &linked_page{Items: []any{}}
	for _, item := range activities {
		activity, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := activity["type"].(string)
		switch kind {
		case "Create":
			object, ok := activity["object"].(map[string]any)
			if !ok || !quote_types[strings.ToLower(fmt.Sprint(object["type"]))] || object["inReplyTo"] != nil {
				continue
			}
			if by := resolve_first_url(object["attributedTo"]); by != "" && by != actor {
				continue
			}
			page.Items = append(page.Items, object)
		case "Announce":
			if object := activitypub_actor_of(activity["object"]); object != "" {
				page.Items = append(page.Items, map[string]any{"type": "Announce", "actor": actor, "object": object, "published": activity["published"]})
			}
		}
	}
	if address := resolve_first_url(next); address != "" && len(page.Items) > 0 {
		page.Cursor = address
	}
	return page
}

// === Public posts ===

// bluesky_public_posts reads a page of a Bluesky account's own posts from its
// data server, which needs no sign-in: the post records, newest first, each
// shaped as the account's own feed shows a post - the author from the profile,
// the images as the blobs' addresses - so a reader of either sees one form.
func bluesky_public_posts(ctx context.Context, did string, cursor string, limit int) (*linked_page, error) {
	r, err := author_resolve(ctx, did)
	if err != nil {
		return nil, err
	}
	if r.Server == "" {
		return nil, fmt.Errorf("%s names no server", did)
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	address := r.Server + "/xrpc/com.atproto.repo.listRecords?repo=" + url.QueryEscape(did) + "&collection=app.bsky.feed.post&limit=" + strconv.Itoa(limit)
	if cursor != "" {
		address += "&cursor=" + url.QueryEscape(cursor)
	}
	var answer struct {
		Cursor  string `json:"cursor"`
		Records []struct {
			URI   string         `json:"uri"`
			CID   string         `json:"cid"`
			Value map[string]any `json:"value"`
		} `json:"records"`
	}
	if err := resolve_json(ctx, address, "application/json", &answer); err != nil {
		return nil, err
	}
	author := map[string]any{"did": did, "handle": strings.TrimPrefix(r.Address, "@"), "displayName": r.Name, "avatar": r.Avatar}
	page := &linked_page{Items: []any{}}
	for _, record := range answer.Records {
		post := map[string]any{"uri": record.URI, "cid": record.CID, "author": author, "record": record.Value}
		if embed := bluesky_embed_view(r.Server, did, record.Value["embed"]); embed != nil {
			post["embed"] = embed
		}
		page.Items = append(page.Items, map[string]any{"post": post})
	}
	if len(answer.Records) > 0 {
		page.Cursor = answer.Cursor
	}
	return page, nil
}

// bluesky_blob is the address a blob is read at on a data server.
func bluesky_blob(server string, did string, cid string) string {
	return server + "/xrpc/com.atproto.sync.getBlob?did=" + url.QueryEscape(did) + "&cid=" + url.QueryEscape(cid)
}

// bluesky_blob_cid reads a blob reference's content id.
func bluesky_blob_cid(blob any) string {
	b, ok := blob.(map[string]any)
	if !ok {
		return ""
	}
	ref, ok := b["ref"].(map[string]any)
	if !ok {
		return ""
	}
	cid, _ := ref["$link"].(string)
	return cid
}

// bluesky_embed_view shapes a post record's embed as a feed shows it: the
// images with their blobs' addresses and alternative text, an external link
// with its thumbnail. Nothing for a quoted record alone.
func bluesky_embed_view(server string, did string, embed any) map[string]any {
	e, ok := embed.(map[string]any)
	if !ok {
		return nil
	}
	kind, _ := e["$type"].(string)
	switch kind {
	case "app.bsky.embed.recordWithMedia":
		return bluesky_embed_view(server, did, e["media"])
	case "app.bsky.embed.images":
		images := []any{}
		list, _ := e["images"].([]any)
		for _, item := range list {
			image, ok := item.(map[string]any)
			if !ok {
				continue
			}
			cid := bluesky_blob_cid(image["image"])
			if cid == "" {
				continue
			}
			address := bluesky_blob(server, did, cid)
			alt, _ := image["alt"].(string)
			images = append(images, map[string]any{"fullsize": address, "thumb": address, "alt": alt})
		}
		if len(images) == 0 {
			return nil
		}
		return map[string]any{"$type": "app.bsky.embed.images#view", "images": images}
	case "app.bsky.embed.external":
		external, ok := e["external"].(map[string]any)
		if !ok {
			return nil
		}
		view := map[string]any{"uri": external["uri"], "title": external["title"], "description": external["description"]}
		if cid := bluesky_blob_cid(external["thumb"]); cid != "" {
			view["thumb"] = bluesky_blob(server, did, cid)
		}
		return map[string]any{"$type": "app.bsky.embed.external#view", "external": view}
	}
	return nil
}
