import { useCallback } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { toast } from 'sonner'
import { useAuth } from './useAuth'

export function useLogout() {
  const { logout: clearAuth, setLoading, isLoading } = useAuth()
  const navigate = useNavigate()

  const logout = useCallback(async () => {
    try {
      setLoading(true)

      clearAuth()

      toast.success('Logged out successfully')

      navigate({
        to: import.meta.env.VITE_AUTH_LOGIN_URL,
        replace: true,
      })
    } catch (_error) {
      clearAuth()

      toast.error('Logged out (with errors)')

      navigate({
        to: import.meta.env.VITE_AUTH_LOGIN_URL,
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
