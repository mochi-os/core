import { Outlet } from '@tanstack/react-router'
import { useEffect } from 'react'
import { useAuthStore } from '@/stores/auth-store'
import { loadUserProfile } from '@/services/auth-service'

type AuthenticatedLayoutProps = {
  children?: React.ReactNode
}

export function AuthenticatedLayout({ children }: AuthenticatedLayoutProps) {
  const { login, user } = useAuthStore()

  // Optional: Load user profile if authenticated but user data not loaded
  useEffect(() => {
    if (login && !user) {
      // Load user profile from /me endpoint (when implemented)
      loadUserProfile()
    }
  }, [login, user])

  // Minimal layout - just render the outlet
  // The authenticated index route will redirect to the default app
  return <>{children ?? <Outlet />}</>
}
