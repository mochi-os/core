// Mochi server: Web Push notifications
// Copyright Alistair Cunningham 2025

package main

import (
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	webpush "github.com/SherClockHolmes/webpush-go"
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

// mochi.webpush.send(endpoint, auth, p256dh, payload) -> bool: Send push notification
func api_webpush_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var endpoint, auth, p256dh, payload string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"endpoint", &endpoint,
		"auth", &auth,
		"p256dh", &p256dh,
		"payload", &payload,
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

	// 201 = success, 404/410 = subscription expired
	return sl.Bool(resp.StatusCode == 201), nil
}

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
