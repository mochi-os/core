// feat(auth): implement login-header based auth flow
import { create } from 'zustand'
import { getCookie, setCookie, removeCookie } from '@/lib/cookies'

/**
 * Cookie names for authentication
 * - login: Primary credential (raw value used as Authorization header)
 * - user_email: User email for display purposes (persists across reloads)
 */
const LOGIN_COOKIE = 'login'
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
 * - login: Primary credential (raw value from backend, used as-is in Authorization header)
 * - user: User profile data for UI display (optional, can be loaded separately)
 * - isAuthenticated: Computed from presence of login
 */
interface AuthState {
  // State
  user: AuthUser | null
  login: string // Primary credential (raw login value)
  isLoading: boolean
  isInitialized: boolean

  // Computed
  isAuthenticated: boolean

  // Actions
  setAuth: (user: AuthUser | null, login: string) => void
  setUser: (user: AuthUser | null) => void
  setLogin: (login: string) => void
  setLoading: (isLoading: boolean) => void
  syncFromCookie: () => void
  clearAuth: () => void
  initialize: () => void
}

/**
 * Auth Store using Zustand
 *
 * This store manages authentication state with the following strategy:
 * 1. Primary credential: login (stored in 'login' cookie)
 * 2. User data: Optional profile information for UI display
 * 3. Cookies are the source of truth (survive page refresh)
 * 4. Store provides fast in-memory access
 *
 * Authentication Flow:
 * - On login: Backend returns 'login' value → store in cookie + state
 * - On requests: Use 'login' as Authorization header
 * - On page load: Sync state from cookies
 * - On logout: Clear both cookies and state
 */
export const useAuthStore = create<AuthState>()((set, get) => {
  // Initialize from cookies on store creation
  const initialLogin = getCookie(LOGIN_COOKIE) || ''
  const initialEmail = getCookie(EMAIL_COOKIE) || ''

  // Create user object from email if available
  const initialUser = initialEmail
    ? {
        email: initialEmail,
        name: extractNameFromEmail(initialEmail),
      }
    : null

  return {
    // Initial state from cookies
    user: initialUser,
    login: initialLogin,
    isLoading: false,
    isInitialized: false,
    // Authenticated if we have login
    isAuthenticated: Boolean(initialLogin),

    /**
     * Set authentication state (typically after login)
     *
     * @param user - User profile data (optional, for UI display)
     * @param login - Primary credential from backend (required)
     */
    setAuth: (user, login) => {
      // Store credentials in cookies for persistence with secure defaults
      const cookieOptions = {
        maxAge: 60 * 60 * 24 * 7, // 7 days in seconds
        path: '/',
        sameSite: 'strict' as const,
        secure: window.location.protocol === 'https:',
      }

      if (login) {
        setCookie(LOGIN_COOKIE, login, cookieOptions)
      } else {
        removeCookie(LOGIN_COOKIE)
      }

      // Store email for persistence across reloads
      if (user?.email) {
        setCookie(EMAIL_COOKIE, user.email, cookieOptions)
      } else {
        removeCookie(EMAIL_COOKIE)
      }

      set({
        user,
        login,
        // Authenticated if we have login
        isAuthenticated: Boolean(login),
        isInitialized: true,
      })
    },

    /**
     * Update user information only (for profile updates)
     */
    setUser: (user) => {
      // Store email in cookie for persistence with secure defaults
      const cookieOptions = {
        maxAge: 60 * 60 * 24 * 7, // 7 days in seconds
        path: '/',
        sameSite: 'strict' as const,
        secure: window.location.protocol === 'https:',
      }

      if (user?.email) {
        setCookie(EMAIL_COOKIE, user.email, cookieOptions)
      } else {
        removeCookie(EMAIL_COOKIE)
      }

      set({
        user,
        // Keep authentication status based on login
        isAuthenticated: Boolean(get().login),
      })
    },

    /**
     * Update login credential only
     */
    setLogin: (login) => {
      const cookieOptions = {
        maxAge: 60 * 60 * 24 * 7, // 7 days in seconds
        path: '/',
        sameSite: 'strict' as const,
        secure: window.location.protocol === 'https:',
      }

      if (login) {
        setCookie(LOGIN_COOKIE, login, cookieOptions)
      } else {
        removeCookie(LOGIN_COOKIE)
      }

      set({
        login,
        isAuthenticated: Boolean(login),
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
      const cookieEmail = getCookie(EMAIL_COOKIE) || ''
      const storeLogin = get().login
      const storeEmail = get().user?.email

      // If cookies differ from store, sync to store (cookies are source of truth)
      if (cookieLogin !== storeLogin || cookieEmail !== storeEmail) {
        const user: AuthUser | null = cookieEmail
          ? {
              email: cookieEmail,
              name: extractNameFromEmail(cookieEmail),
            }
          : get().user // Keep existing user if no email

        set({
          login: cookieLogin,
          user,
          isAuthenticated: Boolean(cookieLogin),
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
      removeCookie(LOGIN_COOKIE, '/')
      removeCookie(EMAIL_COOKIE, '/')

      set({
        user: null,
        login: '',
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
