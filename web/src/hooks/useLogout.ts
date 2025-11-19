import { useCallback } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useAuth } from './useAuth'
import { toast } from 'sonner'

/**
 * Hook to handle logout functionality
 * 
 * Usage:
 * ```tsx
 * function LogoutButton() {
 *   const { logout, isLoggingOut } = useLogout()
 *   
 *   return (
 *     <button onClick={logout} disabled={isLoggingOut}>
 *       {isLoggingOut ? 'Logging out...' : 'Logout'}
 *     </button>
 *   )
 * }
 * ```
 */
export function useLogout() {
  const { logout: clearAuth, setLoading, isLoading } = useAuth()
  const navigate = useNavigate()

  const logout = useCallback(async () => {
    try {
      setLoading(true)

      // Optional: Call backend logout endpoint
      // This would invalidate the token on the server
      // await authApi.logout()

      // Clear all auth state (cookie + store)
      clearAuth()

      // Show success message
      toast.success('Logged out successfully')

      // Redirect to login page
      navigate({
        to: '/login',
        replace: true,
      })
    } catch (_error) {
      // Even if backend call fails, clear local auth
      clearAuth()
      
      toast.error('Logged out (with errors)')
      
      navigate({
        to: '/login',
        replace: true,
      })
    } finally {
      setLoading(false)
    }
  }, [clearAuth, setLoading, navigate])

  return {
    logout,
    isLoggingOut: isLoading,
  }
}

