import { useAuthStore, type AuthUser } from '@/stores/auth-store'

export function useAuth() {
  const user = useAuthStore((state) => state.user)
  const token = useAuthStore((state) => state.token)
  const isLoading = useAuthStore((state) => state.isLoading)
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const isInitialized = useAuthStore((state) => state.isInitialized)
  const hasIdentity = useAuthStore((state) => state.hasIdentity)
  const identityName = useAuthStore((state) => state.identityName)
  const identityPrivacy = useAuthStore((state) => state.identityPrivacy)

  const setAuth = useAuthStore((state) => state.setAuth)
  const setUser = useAuthStore((state) => state.setUser)
  const setToken = useAuthStore((state) => state.setToken)
  const setLoading = useAuthStore((state) => state.setLoading)
  const syncFromCookie = useAuthStore((state) => state.syncFromCookie)
  const clearAuth = useAuthStore((state) => state.clearAuth)
  const setIdentity = useAuthStore((state) => state.setIdentity)
  const clearIdentity = useAuthStore((state) => state.clearIdentity)

  return {
    // State
    user,
    token,
    isLoading,
    isAuthenticated,
    isInitialized,
    hasIdentity,
    identityName,
    identityPrivacy,

    // Actions
    setAuth,
    setUser,
    setToken,
    setLoading,
    syncFromCookie,
    logout: clearAuth,
    setIdentity,
    clearIdentity,
  }
}

export function useUser(): AuthUser | null {
  return useAuthStore((state) => state.user)
}

export function useIsAuthenticated(): boolean {
  return useAuthStore((state) => state.isAuthenticated)
}

export function useIsAuthLoading(): boolean {
  return useAuthStore((state) => state.isLoading)
}
