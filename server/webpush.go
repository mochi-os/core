// Mochi server: Web Push notifications
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"strings"
	"sync"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Allowed push service base URLs (whitelist)
var webpush_allowed = []string{
	"https://fcm.googleapis.com/",
	"https://updates.push.services.mozilla.com/",
	"https://web.push.apple.com/",
}

// VAPID keys (auto-generated and stored in settings)
var webpush_public string
var webpush_private string
var webpush_once sync.Once

// webpush_ensure initializes VAPID keys on first use
func webpush_ensure() {
	webpush_once.Do(func() {
		webpush_public = setting_get("webpush_public", "")
		webpush_private = setting_get("webpush_private", "")

		if webpush_public == "" || webpush_private == "" {
			var err error
			webpush_private, webpush_public, err = webpush.GenerateVAPIDKeys()
			if err != nil {
				warn("webpush: failed to generate VAPID keys: %v", err)
				return
			}
			setting_set("webpush_public", webpush_public)
			setting_set("webpush_private", webpush_private)
			info("webpush: generated new VAPID keys")
		}
	})
}

// Starlark API
var api_webpush = sls.FromStringDict(sl.String("mochi.webpush"), sl.StringDict{
	"key":  sl.NewBuiltin("mochi.webpush.key", api_webpush_key),
	"send": sl.NewBuiltin("mochi.webpush.send", api_webpush_send),
})

// mochi.webpush.key() -> string: Get VAPID public key for browser subscription
func api_webpush_key(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	webpush_ensure()
	return sl.String(webpush_public), nil
}

// mochi.webpush.send(endpoint, auth, p256dh, payload, event_id="...") -> bool: Send push notification.
//
// `event_id` is an optional caller-supplied stable id for the logical
// notification this send corresponds to. When provided the call dedups
// against a per-user webpush_delivered(endpoint, event_id) row — two
// replicas independently sending the same logical notification produce
// one delivery per subscription instead of two. The cross-replica dedup
// is local-only at this layer: replicating the dedup table tightens the
// window but a small concurrent-emit race remains and is documented as
// acceptable for the user-facing duplicate-push impact.
func api_webpush_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check webpush/send permission
	if err := require_permission(t, fn, "webpush/send"); err != nil {
		return sl_error(fn, "%v", err)
	}

	var endpoint, auth, p256dh, payload, event_id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"endpoint", &endpoint,
		"auth", &auth,
		"p256dh", &p256dh,
		"payload", &payload,
		"event_id?", &event_id,
	); err != nil {
		return nil, err
	}

	webpush_ensure()

	// Check VAPID keys are available
	if webpush_public == "" || webpush_private == "" {
		return sl.Bool(false), nil
	}

	// Validate endpoint is a known push service (whitelist)
	valid := false
	for _, base := range webpush_allowed {
		if strings.HasPrefix(endpoint, base) {
			valid = true
			break
		}
	}
	if !valid {
		return sl.Bool(false), nil
	}

	// Dedup gate. We only consult the table when event_id is supplied
	// and a user is on the thread; otherwise behave as before.
	user, _ := t.Local("user").(*User)
	if event_id != "" && user != nil {
		if webpush_already_delivered(user, endpoint, event_id) {
			debug("webpush dedup: event_id=%q endpoint=%q already delivered", event_id, endpoint)
			return sl.Bool(true), nil
		}
	}

	sub := webpush.Subscription{
		Endpoint: endpoint,
		Keys: webpush.Keys{
			Auth:   auth,
			P256dh: p256dh,
		},
	}

	resp, err := webpush.SendNotification([]byte(payload), &sub, &webpush.Options{
		Subscriber:      "mailto:webpush@localhost",
		VAPIDPublicKey:  webpush_public,
		VAPIDPrivateKey: webpush_private,
		TTL:             86400,
	})

	if err != nil {
		return sl.Bool(false), nil
	}
	resp.Body.Close()

	ok := resp.StatusCode == 201
	if ok && event_id != "" && user != nil {
		webpush_mark_delivered(user, endpoint, event_id)
	}

	// 201 = success, 404/410 = subscription expired
	return sl.Bool(ok), nil
}

// webpush_already_delivered consults the per-user webpush_delivered
// table. Returns true when an earlier call already recorded a delivery
// for this (endpoint, event_id) in the TTL window.
func webpush_already_delivered(u *User, endpoint, event_id string) bool {
	db := webpush_dedup_db(u)
	exists, _ := db.exists("select 1 from webpush_delivered where endpoint=? and event_id=? and ts > ?", endpoint, event_id, now()-webpush_dedup_ttl)
	return exists
}

// webpush_mark_delivered records (endpoint, event_id) as delivered now,
// and opportunistically prunes stale rows so the dedup table doesn't
// grow without bound.
func webpush_mark_delivered(u *User, endpoint, event_id string) {
	db := webpush_dedup_db(u)
	db.exec("insert or ignore into webpush_delivered (endpoint, event_id, ts) values (?, ?, ?)", endpoint, event_id, now())
	db.exec("delete from webpush_delivered where ts < ?", now()-webpush_dedup_ttl)
}

// webpush_dedup_db opens the per-user notifications DB and lazily
// creates the webpush_delivered table.
func webpush_dedup_db(u *User) *DB {
	db := db_user(u, "notifications")
	db.exec("create table if not exists webpush_delivered (endpoint text not null, event_id text not null, ts integer not null, primary key (endpoint, event_id))")
	db.exec("create index if not exists webpush_delivered_ts on webpush_delivered(ts)")
	return db
}

// Dedup TTL — slightly longer than the typical replication window so
// two replicas processing the same logical event always see each other's
// previous delivery, but short enough that a stale subscription that
// happens to reuse an event_id 24 hours later isn't blocked.
const webpush_dedup_ttl int64 = 24 * 3600

// webpush_service_worker serves the push notification service worker
func webpush_service_worker(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "application/javascript")
	c.Writer.Header().Set("Service-Worker-Allowed", "/")
	c.String(200, serviceWorkerJS)
}

const serviceWorkerJS = `// Mochi Push Notification Service Worker

self.addEventListener('push', function(event) {
  if (!event.data) return;

  const data = event.data.json();
  const options = {
    body: data.body,
    tag: data.tag || 'mochi',
    icon: '/icon-192.png',
    data: { link: data.link }
  };

  event.waitUntil(
    self.registration.showNotification(data.title || 'Mochi', options)
  );
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const link = event.notification.data?.link || '/';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {
      for (const client of clientList) {
        if (client.url.includes(self.location.origin) && 'focus' in client) {
          client.focus();
          if (link !== '/') client.navigate(link);
          return;
        }
      }
      return clients.openWindow(link);
    })
  );
});
`
