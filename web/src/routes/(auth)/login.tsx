import { z } from 'zod'
import { createFileRoute, redirect } from '@tanstack/react-router'
import { useAuthStore } from '@/stores/auth-store'
import { SignIn } from '@/features/auth/sign-in'

const searchSchema = z.object({
  redirect: z.string().optional(),
})

export const Route = createFileRoute('/(auth)/login')({
  beforeLoad: ({ search }) => {
    const store = useAuthStore.getState()

    // Sync from cookies if not initialized (handles page refresh)
    if (!store.isInitialized) {
      store.syncFromCookie()
    }

    // If already authenticated, redirect away from login page
    if (store.isAuthenticated) {
      if (!store.hasIdentity) {
        throw redirect({
          to: '/identity',
          search: {
            redirect: search.redirect,
          },
        })
      }

      // Use redirect param if provided and valid
      const redirectUrl = search.redirect
      if (redirectUrl && typeof redirectUrl === 'string') {
        // Validate it's a relative URL (security: prevent open redirect)
        try {
          const url = new URL(redirectUrl, window.location.origin)
          // Only allow same-origin redirects
          if (url.origin === window.location.origin) {
            // Use window.location for cross-app navigation
            window.location.href = url.pathname + url.search + url.hash
            // Return early to prevent route from loading
            return
          }
        } catch {
          // Invalid URL, fall through to default
        }
      }

      // Default: redirect to default app (cross-app navigation)
      window.location.href = import.meta.env.VITE_DEFAULT_APP_URL
      // Return early to prevent route from loading
      return
    }

    // Not authenticated, allow login page to render
  },
  component: SignIn,
  validateSearch: searchSchema,
})
