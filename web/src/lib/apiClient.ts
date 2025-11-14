// feat(auth): implement login-header based auth flow
import axios, { type AxiosError, type InternalAxiosRequestConfig } from 'axios'
import { toast } from 'sonner'
import { useAuthStore } from '@/stores/auth-store'
import { getCookie } from '@/lib/cookies'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL

const devConsole = globalThis.console

/**
 * Log errors in development mode only
 */
const logDevError = (message: string, error: unknown) => {
  if (import.meta.env.DEV) {
    devConsole?.error?.(message, error)
  }
}

/**
 * Create Axios instance with base configuration
 */
export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000, // 30 seconds
  headers: {
    'Content-Type': 'application/json',
  },
})

/**
 * Request Interceptor
 *
 * Authentication Strategy:
 * 1. Use `login` cookie value with "Bearer" scheme (Authorization: Bearer <login>)
 * 2. Store values are checked first (most up-to-date), then cookies
 */
apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    // Get auth values from store (most up-to-date)
    const storeLogin = useAuthStore.getState().login

    // Get auth values from cookies (fallback/persistence)
    const cookieLogin = getCookie('login')

    // Determine which credentials to use (store takes precedence)
    const login = storeLogin || cookieLogin

    // Set Authorization header with Bearer scheme
    config.headers = config.headers ?? {}
    if (login) {
      // Add Bearer prefix if not already present
      ;(config.headers as Record<string, string>).Authorization =
        login.startsWith('Bearer ') ? login : `Bearer ${login}`

      // Log auth method in development for debugging
      if (import.meta.env.DEV) {
        devConsole?.log?.(
          `[API Auth] Using Bearer scheme with login credential`
        )
      }
    }

    // Log request in development
    if (import.meta.env.DEV) {
      devConsole?.log?.(`[API] ${config.method?.toUpperCase()} ${config.url}`)
    }

    return config
  },
  (error) => {
    logDevError('[API] Request error', error)
    return Promise.reject(error)
  }
)

/**
 * Response Interceptor
 *
 * Handles common response scenarios:
 * - 401: Unauthorized → Clear auth and optionally redirect
 * - 403: Forbidden → Show error message
 * - 404: Not Found → Let components handle
 * - 500+: Server errors → Show error message
 * - Network errors → Show error message
 */
apiClient.interceptors.response.use(
  (response) => {
    // Log successful response in development
    if (import.meta.env.DEV) {
      devConsole?.log?.(
        `[API] ${response.config.method?.toUpperCase()} ${response.config.url} → ${response.status}`
      )
    }

    return response
  },
  async (error: AxiosError) => {
    const status = error.response?.status

    // Handle different error scenarios
    switch (status) {
      case 401: {
        // Unauthorized - session expired or invalid credentials
        logDevError('[API] 401 Unauthorized', error)

        const isAuthEndpoint =
          error.config?.url?.includes('/login') ||
          error.config?.url?.includes('/auth') ||
          error.config?.url?.includes('/verify')

        if (!isAuthEndpoint) {
          // Clear auth state and redirect to login
          useAuthStore.getState().clearAuth()

          // Redirect to login (mochi-core's own route)
          if (import.meta.env.PROD) {
            toast.error('Session expired', {
              description: 'Please log in again to continue.',
            })
          }

          // Redirect to login page
          window.location.href = import.meta.env.VITE_AUTH_SIGN_IN_URL
        }
        break
      }

      case 403: {
        // Forbidden - user doesn't have permission
        logDevError('[API] 403 Forbidden', error)
        toast.error('Access denied', {
          description: "You don't have permission to perform this action.",
        })
        break
      }

      case 404: {
        // Not found - resource doesn't exist
        logDevError('[API] 404 Not Found', error)
        // Don't show toast for 404s - let components handle it
        break
      }

      case 500:
      case 502:
      case 503: {
        // Server errors
        logDevError('[API] Server error', error)
        toast.error('Server error', {
          description:
            'Something went wrong on our end. Please try again later.',
        })
        break
      }

      default: {
        // Network errors or other issues
        if (!error.response) {
          logDevError('[API] Network error', error)
          toast.error('Network error', {
            description: 'Please check your internet connection and try again.',
          })
        } else {
          logDevError('[API] Response error', error)
        }
      }
    }

    return Promise.reject(error)
  }
)

/**
 * Helper to check if error is an authentication error
 */
export function isAuthError(error: unknown): boolean {
  return (
    error instanceof Error &&
    'response' in error &&
    (error as AxiosError).response?.status === 401
  )
}

/**
 * Helper to check if error is a forbidden error
 */
export function isForbiddenError(error: unknown): boolean {
  return (
    error instanceof Error &&
    'response' in error &&
    (error as AxiosError).response?.status === 403
  )
}

/**
 * Helper to check if error is a network error
 */
export function isNetworkError(error: unknown): boolean {
  return (
    error instanceof Error &&
    'response' in error &&
    !(error as AxiosError).response
  )
}

export default apiClient
