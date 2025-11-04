import { useAuthStore, type AuthUser } from '@/stores/auth-store'

// feat(auth): implement login-header based auth flow
/**
 * Hook to access authentication state and actions
 * 
 * Provides convenient access to:
 * - Authentication state (user, credentials, loading, etc.)
 * - Authentication actions (setAuth, logout, etc.)
 * 
 * Usage:
 * ```tsx
 * const { user, isAuthenticated, isLoading, logout } = useAuth()
 * 
 * if (isLoading) return <Loading />
 * if (!isAuthenticated) return <Login />
 * 
 * return <div>Welcome {user?.email}</div>
 * ```
 */
export function useAuth() {
  const user = useAuthStore((state) => state.user)
  const rawLogin = useAuthStore((state) => state.rawLogin)
  const accessToken = useAuthStore((state) => state.accessToken)
  const isLoading = useAuthStore((state) => state.isLoading)
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const isInitialized = useAuthStore((state) => state.isInitialized)

  const setAuth = useAuthStore((state) => state.setAuth)
  const setUser = useAuthStore((state) => state.setUser)
  const setRawLogin = useAuthStore((state) => state.setRawLogin)
  const setAccessToken = useAuthStore((state) => state.setAccessToken)
  const setLoading = useAuthStore((state) => state.setLoading)
  const syncFromCookie = useAuthStore((state) => state.syncFromCookie)
  const clearAuth = useAuthStore((state) => state.clearAuth)

  return {
    // State
    user,
    rawLogin,
    accessToken,
    isLoading,
    isAuthenticated,
    isInitialized,

    // Actions
    setAuth,
    setUser,
    setRawLogin,
    setAccessToken,
    setLoading,
    syncFromCookie,
    logout: clearAuth,
  }
}

/**
 * Type-safe user getter
 * Returns user or null
 */
export function useUser(): AuthUser | null {
  return useAuthStore((state) => state.user)
}

/**
 * Check if user is authenticated
 */
export function useIsAuthenticated(): boolean {
  return useAuthStore((state) => state.isAuthenticated)
}

/**
 * Check if auth is loading
 */
export function useIsAuthLoading(): boolean {
  return useAuthStore((state) => state.isLoading)
}

