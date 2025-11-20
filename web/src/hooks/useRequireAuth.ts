import { useEffect } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useAuth } from './useAuth'

/**
 * Hook to enforce authentication in components
 * Redirects to login if user is not authenticated
 * 
 * Usage:
 * ```tsx
 * function ProtectedComponent() {
 *   const { isLoading } = useRequireAuth()
 *   
 *   if (isLoading) return <Loading />
 *   
 *   // User is guaranteed to be authenticated here
 *   return <div>Protected content</div>
 * }
 * ```
 * 
 * @param redirectTo - Optional custom redirect path (default: '/login')
 */
export function useRequireAuth(redirectTo: string = '/login') {
  const { isAuthenticated, isInitialized, isLoading, hasIdentity } = useAuth()
  const navigate = useNavigate()

  useEffect(() => {
    // Only redirect once initialization is complete
    if (isInitialized && !isAuthenticated && !isLoading) {
      // Save current location for redirect after login
      const currentPath = window.location.pathname + window.location.search
      
      navigate({
        to: redirectTo,
        search: {
          redirect: currentPath,
        },
        replace: true,
      })
      return
    }

    if (isInitialized && isAuthenticated && !hasIdentity && !isLoading) {
      const currentPath = window.location.pathname + window.location.search
      navigate({
        to: '/identity',
        search: {
          redirect: currentPath,
        },
        replace: true,
      })
    }
  }, [isAuthenticated, isInitialized, isLoading, hasIdentity, navigate, redirectTo])

  return {
    isLoading: !isInitialized || isLoading,
    isAuthenticated,
  }
}

