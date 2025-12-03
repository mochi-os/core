import { Outlet } from '@tanstack/react-router'
import { useEffect } from 'react'
import { useAuthStore } from '@/stores/auth-store'
import { loadUserProfile } from '@/services/auth-service'

type AuthenticatedLayoutProps = {
  children?: React.ReactNode
}

export function AuthenticatedLayout({ children }: AuthenticatedLayoutProps) {
  const { token, user } = useAuthStore()

  useEffect(() => {
    if (token && !user) {
      loadUserProfile()
    }
  }, [token, user])

  return <>{children ?? <Outlet />}</>
}
