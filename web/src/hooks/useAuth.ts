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
  const login = useAuthStore((state) => state.login)
  const isLoading = useAuthStore((state) => state.isLoading)
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const isInitialized = useAuthStore((state) => state.isInitialized)
  const hasIdentity = useAuthStore((state) => state.hasIdentity)
  const identityName = useAuthStore((state) => state.identityName)
  const identityPrivacy = useAuthStore((state) => state.identityPrivacy)

  const setAuth = useAuthStore((state) => state.setAuth)
  const setUser = useAuthStore((state) => state.setUser)
  const setLogin = useAuthStore((state) => state.setLogin)
  const setLoading = useAuthStore((state) => state.setLoading)
  const syncFromCookie = useAuthStore((state) => state.syncFromCookie)
  const clearAuth = useAuthStore((state) => state.clearAuth)
  const setIdentity = useAuthStore((state) => state.setIdentity)
  const clearIdentity = useAuthStore((state) => state.clearIdentity)

  return {
    // State
    user,
    login,
    isLoading,
    isAuthenticated,
    isInitialized,
    hasIdentity,
    identityName,
    identityPrivacy,

    // Actions
    setAuth,
    setUser,
    setLogin,
    setLoading,
    syncFromCookie,
    logout: clearAuth,
    setIdentity,
    clearIdentity,
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

