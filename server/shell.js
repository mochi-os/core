// Mochi Shell: postMessage relay, URL sync, localStorage proxy
// This runs in the top-level shell page. No UI rendering — that's the menu app's job.

(function() {
    'use strict';

    var iframe = document.getElementById('app-frame');
    var staleIframe = null; // old iframe kept visible during transition
    var menuEl = document.getElementById('menu');
    var config = window.__mochi_shell || {};
    var currentAppId = config.appId || '';
    var currentAppPath = getAppNameFromPath(window.location.pathname);
    var tokenRefreshTimer = null;
    var navigating = false; // true during cross-app navigation (blocks storage requests)

    // --- Sidebar state ---
    // Persisted across app switches so the sidebar stays collapsed/expanded.
    var sidebarOpen = localStorage.getItem('sidebar_state') !== 'false';

    function setSidebarState(open) {
        sidebarOpen = open;
        try { localStorage.setItem('sidebar_state', String(open)); } catch(e) {}
        // Update menu element so the menu app can observe changes
        if (menuEl) menuEl.setAttribute('data-sidebar', open ? 'expanded' : 'collapsed');
    }

    // Set initial state
    setSidebarState(sidebarOpen);

    // Replace the iframe with a new one, keeping the old visible until the new
    // one sends its ready message. This avoids both history pollution (creating
    // a new element instead of setting .src) and white flashes during transitions.
    function swapIframe(newSrc) {
        var container = iframe.parentNode;

        // Clean up any previous stale iframe
        if (staleIframe && staleIframe.parentNode) {
            staleIframe.parentNode.removeChild(staleIframe);
        }

        // Dim and disable the current iframe while the new one loads
        staleIframe = iframe;
        staleIframe.style.opacity = '0.6';
        staleIframe.style.pointerEvents = 'none';
        staleIframe.removeAttribute('id');

        // Create the new iframe hidden behind the old one
        var next = document.createElement('iframe');
        next.id = 'app-frame';
        next.setAttribute('sandbox', 'allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads');
        next.style.visibility = 'hidden';
        next.src = newSrc;
        container.insertBefore(next, staleIframe);

        iframe = next;
    }

    // Called when the new iframe sends ready — complete the visual transition
    function completeTransition() {
        iframe.style.visibility = '';
        if (staleIframe && staleIframe.parentNode) {
            staleIframe.parentNode.removeChild(staleIframe);
        }
        staleIframe = null;
    }

    // --- Favicon ---
    // Update the tab favicon to match the current app.
    // Each app serves its favicon at /<path>/images/favicon.svg via the images action.
    var faviconLink = document.querySelector('link[rel="icon"]');

    function updateFavicon(appPath) {
        if (!faviconLink) return;
        var base = appPath ? '/' + appPath : '';
        faviconLink.href = base + '/images/favicon.svg';
    }

    // Set initial favicon
    updateFavicon(currentAppPath);

    // --- Token management ---

    function fetchToken(appName) {
        return fetch('/_/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        ,   body: JSON.stringify({ app: appName })
        }).then(function(r) {
            if (!r.ok) throw new Error('Token fetch failed');
            return r.json();
        }).then(function(data) {
            if (data.app) {
                currentAppId = data.app;
                config.appId = data.app;
            }
            return data.token || '';
        });
    }

    function scheduleTokenRefresh(appName) {
        if (tokenRefreshTimer) clearTimeout(tokenRefreshTimer);
        // Refresh 10 minutes before expiry. JWT tokens are long-lived (1 year),
        // but we refresh periodically to handle session invalidation gracefully.
        tokenRefreshTimer = setTimeout(function() {
            fetchToken(appName).then(function(token) {
                postToIframe({ type: 'token-refresh', token: token });
                scheduleTokenRefresh(appName);
            }).catch(function() {
                // Token refresh failed — session may be expired
            });
        }, 10 * 60 * 1000);
    }

    // --- postMessage helpers ---

    function postToIframe(msg) {
        if (iframe && iframe.contentWindow) {
            iframe.contentWindow.postMessage(msg, '*');
        }
    }

    // --- localStorage proxy (namespaced by app ID) ---

    var storagePrefix = 'app:' + currentAppId + ':';

    function handleStorageGet(data) {
        if (navigating) return;
        var value = null;
        try {
            value = localStorage.getItem(storagePrefix + data.key);
        } catch(e) { /* ignore */ }
        postToIframe({
            type: 'storage.result',
            id: data.id,
            value: value
        });
    }

    function handleStorageSet(data) {
        if (navigating) return;
        try {
            localStorage.setItem(storagePrefix + data.key, data.value);
        } catch(e) { /* ignore */ }
    }

    function handleStorageRemove(data) {
        if (navigating) return;
        try {
            localStorage.removeItem(storagePrefix + data.key);
        } catch(e) { /* ignore */ }
    }

    // --- Clipboard proxy ---
    // Sandboxed iframes can't access navigator.clipboard (opaque origin).
    // The shell proxies clipboard writes on behalf of the app.

    function handleClipboardWrite(data) {
        if (navigating) return;
        var id = data.id;
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(data.text).then(function() {
                postToIframe({ type: 'clipboard.result', id: id, ok: true });
            }).catch(function() {
                postToIframe({ type: 'clipboard.result', id: id, ok: false });
            });
        } else {
            postToIframe({ type: 'clipboard.result', id: id, ok: false });
        }
    }

    // --- URL sync ---

    function getAppNameFromPath(path) {
        var match = path.match(/^\/([^/]+)/);
        return match ? match[1] : '';
    }

    var lastNavigatedPath = window.location.pathname + window.location.search + window.location.hash;

    function handleNavigate(data) {
        if (!data.path) return;
        // Reject navigate messages for paths outside the current app (anti-spoofing)
        var currentApp = getAppNameFromPath(window.location.pathname);
        var navApp = getAppNameFromPath(data.path);
        if (navApp && navApp !== currentApp) return;
        // Only push a new history entry when the path actually changed
        if (data.path !== lastNavigatedPath) {
            history.pushState(null, '', data.path);
            lastNavigatedPath = data.path;
        }
    }

    function handleNavigateExternal(data) {
        if (!data.url) return;
        var newApp = getAppNameFromPath(data.url);

        if (newApp !== currentAppPath) {
            // Cross-app navigation: update URL, fetch new token, swap iframe
            navigating = true;
            currentAppPath = newApp;
            updateFavicon(newApp);
            document.title = 'Mochi';
            history.pushState(null, '', data.url);

            fetchToken(newApp).then(function(token) {
                currentAppId = newApp;
                storagePrefix = 'app:' + currentAppId + ':';
                swapIframe(data.url);
                scheduleTokenRefresh(newApp);
            }).catch(function() {
                currentAppId = newApp;
                storagePrefix = 'app:' + currentAppId + ':';
                swapIframe(data.url);
            });
        } else {
            // Same app — just update iframe location
            history.pushState(null, '', data.url);
            postToIframe({ type: 'popstate', path: data.url });
        }
    }

    // --- popstate (back/forward) ---

    window.addEventListener('popstate', function() {
        var path = window.location.pathname + window.location.search + window.location.hash;
        lastNavigatedPath = path;
        var newApp = getAppNameFromPath(path);

        if (newApp !== currentAppPath) {
            // Different app — swap iframe and fetch new token
            navigating = true;
            currentAppPath = newApp;
            updateFavicon(newApp);
            document.title = 'Mochi';
            fetchToken(newApp).then(function() {
                currentAppId = newApp;
                storagePrefix = 'app:' + currentAppId + ':';
                swapIframe(path);
                scheduleTokenRefresh(newApp);
            }).catch(function() {
                currentAppId = newApp;
                storagePrefix = 'app:' + currentAppId + ':';
                swapIframe(path);
            });
        } else {
            // Same app — reload iframe at the new path
            // (pushState/replaceState don't work in sandboxed iframes with opaque origins)
            swapIframe(path);
        }
    });

    // --- Message handler ---

    window.addEventListener('message', function(event) {
        // Validate: must come from the sandboxed iframe (opaque origin = "null")
        if (event.source !== iframe.contentWindow) return;

        var data = event.data;
        if (!data || typeof data !== 'object' || !data.type) return;

        switch (data.type) {
            case 'ready':
                // App is ready — complete transition and send init with token
                completeTransition();
                navigating = false;
                var appName = getAppNameFromPath(window.location.pathname);
                var theme = document.documentElement.classList.contains('dark') ? 'dark' : 'light';
                fetchToken(appName).then(function(token) {
                    postToIframe({
                        type: 'init',
                        token: token,
                        theme: theme,
                        user: { name: config.userName },
                        inShell: true,
                        sidebarOpen: sidebarOpen
                    });
                    scheduleTokenRefresh(appName);
                }).catch(function() {
                    postToIframe({
                        type: 'init',
                        token: '',
                        theme: theme,
                        user: { name: config.userName },
                        inShell: true,
                        sidebarOpen: sidebarOpen
                    });
                });
                break;

            case 'navigate':
                handleNavigate(data);
                break;

            case 'navigate-external':
                handleNavigateExternal(data);
                break;

            case 'title':
                if (data.title) document.title = data.title;
                break;

            case 'storage.get':
                handleStorageGet(data);
                break;

            case 'storage.set':
                handleStorageSet(data);
                break;

            case 'storage.remove':
                handleStorageRemove(data);
                break;

            case 'clipboard.write':
                handleClipboardWrite(data);
                break;

            case 'sidebar-state':
                setSidebarState(!!data.open);
                break;
        }
    });

    // --- Theme sync ---
    // Listen for theme changes from the menu app and forward to iframe
    var observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.attributeName === 'class') {
                var isDark = document.documentElement.classList.contains('dark');
                postToIframe({ type: 'theme-change', theme: isDark ? 'dark' : 'light' });
            }
        });
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });

    // --- Service worker registration ---
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/sw.js').catch(function() {});
    }
})();
