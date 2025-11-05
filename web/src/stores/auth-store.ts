// feat(auth): implement login-header based auth flow
import { create } from 'zustand'
import { getCookie, setCookie, removeCookie } from '@/lib/cookies'

/**
 * Cookie names for authentication
 * - login: Primary credential (raw value used as Authorization header)
 * - token: Optional fallback credential (used with Bearer prefix)
 * - email: User email for display purposes (persists across reloads)
 */
const LOGIN_COOKIE = 'login'
const TOKEN_COOKIE = 'token'
const EMAIL_COOKIE = 'user_email'

/**
 * Extract name from email (part before @)
 * Capitalizes and formats nicely for display
 */
const extractNameFromEmail = (email: string): string => {
  const name = email.split('@')[0]
  // Capitalize first letter and replace dots/underscores with spaces
  return name
    .split(/[._-]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

/**
 * User information interface
 * Contains user profile data for UI display
 */
export interface AuthUser {
  email: string // User's email address
  name?: string // Display name (extracted from email or provided by backend)
  accountNo?: string // Account number/ID
  role?: string[] // User roles/permissions
  exp?: number // Token expiration timestamp
  avatar?: string // User avatar URL
}

/**
 * Authentication state interface
 *
 * Authentication Strategy:
 * - rawLogin: Primary credential (raw value from backend, used as-is in Authorization header)
 * - accessToken: Optional fallback credential (JWT token, used with Bearer prefix)
 * - user: User profile data for UI display (optional, can be loaded separately)
 * - isAuthenticated: Computed from presence of rawLogin OR accessToken
 */
interface AuthState {
  // State
  user: AuthUser | null
  rawLogin: string // Primary credential (raw login value)
  accessToken: string // Optional fallback credential (JWT token)
  isLoading: boolean
  isInitialized: boolean

  // Computed
  isAuthenticated: boolean

  // Actions
  setAuth: (
    user: AuthUser | null,
    rawLogin: string,
    accessToken?: string
  ) => void
  setUser: (user: AuthUser | null) => void
  setRawLogin: (rawLogin: string) => void
  setAccessToken: (accessToken: string) => void
  setLoading: (isLoading: boolean) => void
  syncFromCookie: () => void
  clearAuth: () => void
  initialize: () => void
}

/**
 * Parse token from cookie value
 * Handles both plain strings and JSON-encoded strings
 */
const parseTokenCookie = (value?: string): string => {
  if (!value) {
    return ''
  }

  try {
    const parsed = JSON.parse(value)
    return typeof parsed === 'string' ? parsed : value
  } catch (_error) {
    return value
  }
}

/**
 * Decode JWT token to extract user information
 * Note: This is NOT validation - backend must validate tokens
 * This is only for extracting display information from the token payload
 */
const decodeJWT = (token: string): Partial<AuthUser> | null => {
  try {
    const base64Url = token.split('.')[1]
    if (!base64Url) return null

    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/')
    const jsonPayload = decodeURIComponent(
      atob(base64)
        .split('')
        .map((c) => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2))
        .join('')
    )

    return JSON.parse(jsonPayload)
  } catch (_error) {
    return null
  }
}

/**
 * Auth Store using Zustand
 *
 * This store manages authentication state with the following strategy:
 * 1. Primary credential: rawLogin (stored in 'login' cookie)
 * 2. Fallback credential: accessToken (stored in 'token' cookie)
 * 3. User data: Optional profile information for UI display
 * 4. Cookies are the source of truth (survive page refresh)
 * 5. Store provides fast in-memory access
 *
 * Authentication Flow:
 * - On login: Backend returns 'login' value → store in cookie + state
 * - On requests: Use 'login' as Authorization header (or token as fallback)
 * - On page load: Sync state from cookies
 * - On logout: Clear both cookies and state
 */
export const useAuthStore = create<AuthState>()((set, get) => {
  // Initialize from cookies on store creation
  const initialLogin = getCookie(LOGIN_COOKIE) || ''
  const initialToken = parseTokenCookie(getCookie(TOKEN_COOKIE))
  const initialEmail = getCookie(EMAIL_COOKIE) || ''

  // Try to extract user info from token if available (for display purposes)
  const initialUser = initialToken
    ? (() => {
        const decoded = decodeJWT(initialToken)
        if (decoded?.email) {
          return {
            email: decoded.email,
            name: decoded.name || extractNameFromEmail(decoded.email),
            ...decoded,
          } as AuthUser
        }
        return null
      })()
    : initialEmail
      ? {
          email: initialEmail,
          name: extractNameFromEmail(initialEmail),
        }
      : null

  return {
    // Initial state from cookies
    user: initialUser,
    rawLogin: initialLogin,
    accessToken: initialToken,
    isLoading: false,
    isInitialized: false,
    // Authenticated if we have login OR token
    isAuthenticated: Boolean(initialLogin || initialToken),

    /**
     * Set authentication state (typically after login)
     *
     * @param user - User profile data (optional, for UI display)
     * @param rawLogin - Primary credential from backend (required)
     * @param accessToken - Optional fallback token
     */
    setAuth: (user, rawLogin, accessToken = '') => {
      // Store credentials in cookies for persistence
      if (rawLogin) {
        setCookie(LOGIN_COOKIE, rawLogin)
      } else {
        removeCookie(LOGIN_COOKIE)
      }

      if (accessToken) {
        setCookie(TOKEN_COOKIE, accessToken)
      } else {
        removeCookie(TOKEN_COOKIE)
      }

      // Store email for persistence across reloads
      if (user?.email) {
        setCookie(EMAIL_COOKIE, user.email)
      } else {
        removeCookie(EMAIL_COOKIE)
      }

      set({
        user,
        rawLogin,
        accessToken,
        // Authenticated if we have login OR token
        isAuthenticated: Boolean(rawLogin || accessToken),
        isInitialized: true,
      })
    },

    /**
     * Update user information only (for profile updates)
     */
    setUser: (user) => {
      // Store email in cookie for persistence
      if (user?.email) {
        setCookie(EMAIL_COOKIE, user.email)
      } else {
        removeCookie(EMAIL_COOKIE)
      }

      set({
        user,
        // Keep authentication status based on credentials
        isAuthenticated: Boolean(get().rawLogin || get().accessToken),
      })
    },

    /**
     * Update raw login credential only
     */
    setRawLogin: (rawLogin) => {
      if (rawLogin) {
        setCookie(LOGIN_COOKIE, rawLogin)
      } else {
        removeCookie(LOGIN_COOKIE)
      }

      set({
        rawLogin,
        isAuthenticated: Boolean(rawLogin || get().accessToken),
      })
    },

    /**
     * Update access token only
     * Optionally extracts user data from JWT if user is not set
     */
    setAccessToken: (accessToken) => {
      if (accessToken) {
        setCookie(TOKEN_COOKIE, accessToken)

        // Try to extract user from token if user is not already set
        if (!get().user) {
          const decoded = decodeJWT(accessToken)
          if (decoded?.email) {
            set({
              user: {
                email: decoded.email,
                name: decoded.name || extractNameFromEmail(decoded.email),
                ...decoded,
              } as AuthUser,
              accessToken,
              isAuthenticated: true,
            })
            return
          }
        }
      } else {
        removeCookie(TOKEN_COOKIE)
      }

      set({
        accessToken,
        isAuthenticated: Boolean(get().rawLogin || accessToken),
      })
    },

    /**
     * Set loading state
     */
    setLoading: (isLoading) => {
      set({ isLoading })
    },

    /**
     * Sync store state from cookies
     * Call this on route navigation or app mount to ensure consistency
     */
    syncFromCookie: () => {
      const cookieLogin = getCookie(LOGIN_COOKIE) || ''
      const cookieToken = parseTokenCookie(getCookie(TOKEN_COOKIE))
      const cookieEmail = getCookie(EMAIL_COOKIE) || ''
      const storeLogin = get().rawLogin
      const storeToken = get().accessToken
      const storeEmail = get().user?.email

      // If cookies differ from store, sync to store (cookies are source of truth)
      if (
        cookieLogin !== storeLogin ||
        cookieToken !== storeToken ||
        cookieEmail !== storeEmail
      ) {
        // Try to extract user from token if available
        const decoded = cookieToken ? decodeJWT(cookieToken) : null

        const user: AuthUser | null = decoded?.email
          ? ({
              email: decoded.email,
              name: decoded.name || extractNameFromEmail(decoded.email),
              ...decoded,
            } as AuthUser)
          : cookieEmail // Use stored email if no token
            ? {
                email: cookieEmail,
                name: extractNameFromEmail(cookieEmail),
              }
            : get().user // Keep existing user if no token or email

        set({
          rawLogin: cookieLogin,
          accessToken: cookieToken,
          user,
          isAuthenticated: Boolean(cookieLogin || cookieToken),
          isInitialized: true,
        })
      } else {
        set({ isInitialized: true })
      }
    },

    /**
     * Clear all authentication state
     * Call this on logout or when session is invalidated
     */
    clearAuth: () => {
      removeCookie(LOGIN_COOKIE)
      removeCookie(TOKEN_COOKIE)
      removeCookie(EMAIL_COOKIE)

      set({
        user: null,
        rawLogin: '',
        accessToken: '',
        isAuthenticated: false,
        isLoading: false,
        isInitialized: true,
      })
    },

    /**
     * Initialize auth state from cookies
     * Call this once on app mount
     */
    initialize: () => {
      get().syncFromCookie()
    },
  }
})
