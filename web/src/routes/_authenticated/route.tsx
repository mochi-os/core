// feat(auth): implement login-header based auth flow
import { createFileRoute, redirect } from '@tanstack/react-router'
import { AuthenticatedLayout } from '@/components/layout/authenticated-layout'
import { useAuthStore } from '@/stores/auth-store'

/**
 * Protected Route Guard
 * 
 * This guard runs before any /_authenticated/* route loads.
 * It checks for authentication and redirects to login if not authenticated.
 * 
 * Authentication Strategy:
 * 1. Check store for credentials (rawLogin or accessToken)
 * 2. Sync from cookies if needed (handles page refresh)
 * 3. Redirect to login if no credentials found
 * 4. Save return URL for redirect after login
 */
export const Route = createFileRoute('/_authenticated')({
  beforeLoad: ({ location }) => {
    // Get auth state from store
    const store = useAuthStore.getState()
    
    // Sync from cookies if not initialized (handles page refresh)
    if (!store.isInitialized) {
      store.syncFromCookie()
    }
    
    // Check if authenticated (has login OR token)
    const isAuthenticated = store.isAuthenticated
    
    // If not authenticated, redirect to login
    if (!isAuthenticated) {
      throw redirect({
        to: '/sign-in',
        search: {
          redirect: location.href, // Save original destination
        },
      })
    }
    
    // Authenticated, allow navigation
    return
  },
  component: AuthenticatedLayout,
})
