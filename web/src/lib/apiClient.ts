import axios, { type AxiosError, type InternalAxiosRequestConfig } from 'axios'
import { toast } from 'sonner'
import { useAuthStore } from '@/stores/auth-store'
import { getCookie, removeCookie } from '@/lib/cookies'

const devConsole = globalThis.console

const logDevError = (message: string, error: unknown) => {
  if (import.meta.env.DEV) {
    devConsole?.error?.(message, error)
  }
}

export const apiClient = axios.create({
  baseURL: '/',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
})

apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const storeToken = useAuthStore.getState().token
    const cookieToken = getCookie('token')
    const token = storeToken || cookieToken

    config.headers = config.headers ?? {}
    if (token) {
      ;(config.headers as Record<string, string>).Authorization =
        token.startsWith('Bearer ') ? token : `Bearer ${token}`

      if (import.meta.env.DEV) {
        devConsole?.log?.(`[API Auth] Using Bearer scheme with token`)
      }
    }

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

apiClient.interceptors.response.use(
  (response) => {
    const responseData = response.data as unknown
    if (
      responseData &&
      typeof responseData === 'object' &&
      'error' in responseData &&
      'status' in responseData
    ) {
      const errorData = responseData as { error?: string; status?: number }
      if (errorData.error && errorData.status && errorData.status >= 400) {
        toast.error(errorData.error || 'An error occurred')

        if (import.meta.env.DEV) {
          devConsole?.error?.(
            `[API] Application error: ${errorData.error} (status: ${errorData.status})`
          )
        }
      }
    } else {
      if (import.meta.env.DEV) {
        devConsole?.log?.(
          `[API] ${response.config.method?.toUpperCase()} ${response.config.url} → ${response.status}`
        )
      }
    }

    return response
  },
  async (error: AxiosError) => {
    const status = error.response?.status

    switch (status) {
      case 401: {
        logDevError('[API] 401 Unauthorized', error)

        if (import.meta.env.PROD) {
          const isAuthEndpoint =
            error.config?.url?.includes('/login') ||
            error.config?.url?.includes('/auth') ||
            error.config?.url?.includes('/verify')

          if (!isAuthEndpoint) {
            removeCookie('token')
            removeCookie('mochi_me')

            useAuthStore.getState().clearAuth()

            toast.error('Session expired', {
              description: 'Please log in again to continue.',
            })

            const currentUrl = window.location.href
            window.location.href = `${import.meta.env.VITE_AUTH_LOGIN_URL}?redirect=${encodeURIComponent(currentUrl)}`
          }
        }
        break
      }

      case 403: {
        logDevError('[API] 403 Forbidden', error)
        toast.error('Access denied', {
          description: "You don't have permission to perform this action.",
        })
        break
      }

      case 404: {
        logDevError('[API] 404 Not Found', error)
        break
      }

      case 500:
      case 502:
      case 503: {
        logDevError('[API] Server error', error)
        toast.error('Server error', {
          description:
            'Something went wrong on our end. Please try again later.',
        })
        break
      }

      default: {
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

export function isAuthError(error: unknown): boolean {
  return (
    error instanceof Error &&
    'response' in error &&
    (error as AxiosError).response?.status === 401
  )
}

export function isForbiddenError(error: unknown): boolean {
  return (
    error instanceof Error &&
    'response' in error &&
    (error as AxiosError).response?.status === 403
  )
}

export function isNetworkError(error: unknown): boolean {
  return (
    error instanceof Error &&
    'response' in error &&
    !(error as AxiosError).response
  )
}

export default apiClient
