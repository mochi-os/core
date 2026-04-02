// Mochi Shell: postMessage relay, URL sync, localStorage proxy
// This runs in the top-level shell page. No UI rendering — that's the menu app's job.

(function() {
    'use strict';

    var staleIframe = null; // old iframe kept visible during transition
    var menuEl = document.getElementById('menu');
    var shellConfig = null; // populated by /_/shell fetch
    var currentAppPath = getAppNameFromPath(window.location.pathname);
    var currentAppId = currentAppPath;

    // Create the initial iframe — derive src from current URL
    var initialSrc = window.location.pathname + window.location.search + window.location.hash;
    initialSrc += (initialSrc.indexOf('?') >= 0 ? '&' : '?') + '_shell=1';
    var container = document.getElementById('app-container');
    var iframe = document.createElement('iframe');
    iframe.id = 'app-frame';
    iframe.setAttribute('sandbox', 'allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads');
    iframe.src = initialSrc;
    container.appendChild(iframe);
    var tokenRefreshTimer = null;
    var navigating = false; // true during cross-app navigation (blocks storage requests)
    var progressBar = document.getElementById('shell-progress');

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

    var progressInterval = null;
    var progressWidth = 0;

    function showProgress() {
        if (!progressBar) return;
        clearInterval(progressInterval);
        progressWidth = 0;
        progressBar.style.transition = 'none';
        progressBar.style.width = '0%';
        progressBar.style.opacity = '1';
        void progressBar.offsetHeight;
        progressBar.style.transition = 'width 0.3s ease-out';
        progressInterval = setInterval(function() {
            var remaining = 95 - progressWidth;
            progressWidth = Math.min(95, progressWidth + Math.max(0.5, remaining * 0.1));
            progressBar.style.width = progressWidth + '%';
        }, 100);
    }

    function hideProgress() {
        if (!progressBar) return;
        clearInterval(progressInterval);
        progressInterval = null;
        progressBar.style.transition = 'width 0.2s ease-out, opacity 0.3s ease 0.2s';
        progressBar.style.width = '100%';
        progressBar.style.opacity = '0';
    }

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
        hideProgress();
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

    // --- Shell config (menuToken, domain) — fetched once on load ---

    var shellConfigReady = fetch('/_/shell', {
        method: 'POST',
        credentials: 'same-origin'
    }).then(function(r) {
        if (!r.ok) return {};
        return r.json();
    }).then(function(data) {
        shellConfig = data || {};
        return shellConfig;
    }).catch(function() {
        shellConfig = {};
    });

    // Expose promise for menu app (runs in shell page, needs menuToken before rendering)
    window.__mochi_shell_ready = shellConfigReady.then(function() {
        return { menuToken: (shellConfig && shellConfig.menuToken) || '' };
    });

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
                // Expose for menu app (e.g. subscribe-notifications needs entity ID)
                if (window.__mochi_shell) window.__mochi_shell.appId = data.app;
            }
            return data;
        });
    }

    function scheduleTokenRefresh(appName) {
        if (tokenRefreshTimer) clearTimeout(tokenRefreshTimer);
        // Refresh 10 minutes before expiry. JWT tokens are long-lived (1 year),
        // but we refresh periodically to handle session invalidation gracefully.
        tokenRefreshTimer = setTimeout(function() {
            fetchToken(appName).then(function(data) {
                postToIframe({ type: 'token-refresh', token: data.token || '' });
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

            // Show progress bar and dim current iframe immediately (before token fetch)
            showProgress();
            iframe.style.opacity = '0.6';
            iframe.style.pointerEvents = 'none';

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

            // Show progress bar and dim current iframe immediately (before token fetch)
            showProgress();
            iframe.style.opacity = '0.6';
            iframe.style.pointerEvents = 'none';

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
            showProgress();
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
                // App is ready — fetch token and shell config, then send init.
                navigating = false;
                var appName = getAppNameFromPath(window.location.pathname);
                Promise.all([fetchToken(appName), shellConfigReady]).then(function(results) {
                    var tokenData = results[0];
                    var sc = shellConfig || {};
                    var theme = document.documentElement.classList.contains('dark') ? 'dark' : 'light';
                    postToIframe({
                        type: 'init',
                        token: tokenData.token || '',
                        theme: theme,
                        inShell: true,
                        sidebarOpen: sidebarOpen,
                        domain: sc.domain || null
                    });
                    scheduleTokenRefresh(appName);
                    completeTransition();
                }).catch(function() {
                    var theme = document.documentElement.classList.contains('dark') ? 'dark' : 'light';
                    postToIframe({
                        type: 'init',
                        token: '',
                        theme: theme,
                        inShell: true,
                        sidebarOpen: sidebarOpen,
                        domain: null
                    });
                    completeTransition();
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

            case 'theme-set':
                // App changed theme — update shell class (preference persisted server-side by the app)
                var newTheme = data.theme;
                if (newTheme === 'dark' || newTheme === 'light' || newTheme === 'auto' || newTheme === 'system') {
                    var resolved = newTheme;
                    if (newTheme === 'auto' || newTheme === 'system') {
                        resolved = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
                    }
                    document.documentElement.classList.remove('light', 'dark');
                    document.documentElement.classList.add(resolved);
                }
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

    // --- Initial load progress ---
    showProgress();

    // --- Service worker registration ---
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/sw.js').catch(function() {});
    }
})();
