// feat(auth): implement login-header based auth flow
import { create } from 'zustand'
import { getCookie, setCookie, removeCookie } from '@/lib/cookies'
import {
  clearProfileCookie,
  mergeProfileCookie,
  readProfileCookie,
  type IdentityPrivacy,
} from '@/lib/profile-cookie'

/**
 * Cookie names for authentication
 * - login: Primary credential (raw value used as Authorization header)
 * - mochi_me: Combined profile data (email/name/privacy)
 */
const LOGIN_COOKIE = 'login'

/**
 * User information interface
 * Contains user profile data for UI display
 */
export interface AuthUser {
  email: string // User's email address
  name?: string // Display name (provided by backend/cookie)
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
  identityName: string
  identityPrivacy: IdentityPrivacy | ''

  // Computed
  isAuthenticated: boolean
  hasIdentity: boolean

  // Actions
  setAuth: (user: AuthUser | null, login: string) => void
  setUser: (user: AuthUser | null) => void
  setLogin: (login: string) => void
  setLoading: (isLoading: boolean) => void
  syncFromCookie: () => void
  clearAuth: () => void
  initialize: () => void
  setIdentity: (name: string, privacy: IdentityPrivacy) => void
  clearIdentity: () => void
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
  const profile = readProfileCookie()
  const initialEmail = profile.email || ''
  const initialIdentityName = profile.name || ''
  const initialIdentityPrivacy: IdentityPrivacy | '' = profile.privacy || ''

  // Create user object from email if available
  const initialUser =
    initialEmail !== ''
      ? {
          email: initialEmail,
          ...(profile.name ? { name: profile.name } : {}),
        }
      : null

  return {
    // Initial state from cookies
    user: initialUser,
    login: initialLogin,
    isLoading: false,
    isInitialized: false,
    identityName: initialIdentityName,
    identityPrivacy: initialIdentityPrivacy,
    // Authenticated if we have login
    isAuthenticated: Boolean(initialLogin),
    hasIdentity: Boolean(initialIdentityName),

    /**
     * Set authentication state (typically after login)
     *
     * @param user - User profile data (optional, for UI display)
     * @param login - Primary credential from backend (required)
     */
    setAuth: (user, login) => {
      if (login) {
        setCookie(LOGIN_COOKIE, login, {
          maxAge: 60 * 60 * 24 * 7,
          path: '/',
          sameSite: 'strict',
          secure: window.location.protocol === 'https:',
        })
      } else {
        removeCookie(LOGIN_COOKIE)
      }

      // Preserve existing profile cookie data (email, name, privacy)
      // Only update email if provided, don't overwrite name/privacy if they exist
      const currentProfile = readProfileCookie()
      const mergedProfile = mergeProfileCookie({
        email: user?.email ?? currentProfile.email ?? null,
        // Don't overwrite name/privacy if they already exist (from identity form)
        // Only set name if it's explicitly provided and we don't have one
        name: currentProfile.name || user?.name || undefined,
      })

      set({
        user,
        login,
        // Authenticated if we have login
        isAuthenticated: Boolean(login),
        identityName: mergedProfile.name || '',
        identityPrivacy: mergedProfile.privacy || '',
        hasIdentity: Boolean(mergedProfile.name),
        isInitialized: true,
      })
    },

    /**
     * Update user information only (for profile updates)
     */
    setUser: (user) => {
      // Preserve existing profile cookie data (don't overwrite name/privacy if they exist)
      const currentProfile = readProfileCookie()
      mergeProfileCookie({
        email: user?.email ?? currentProfile.email ?? null,
        // Don't overwrite name/privacy if they already exist (from identity form)
        name: currentProfile.name || user?.name || undefined,
      })

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
      if (login) {
        setCookie(LOGIN_COOKIE, login, {
          maxAge: 60 * 60 * 24 * 7,
          path: '/',
          sameSite: 'strict',
          secure: window.location.protocol === 'https:',
        })
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
      const profile = readProfileCookie()
      const cookieEmail = profile.email || ''
      const cookieIdentityName = profile.name || ''
      const cookieIdentityPrivacy: IdentityPrivacy | '' = profile.privacy || ''
      const storeLogin = get().login
      const storeEmail = get().user?.email
      const storeIdentityName = get().identityName
      const storeIdentityPrivacy = get().identityPrivacy

      // If cookies differ from store, sync to store (cookies are source of truth)
      if (
        cookieLogin !== storeLogin ||
        cookieEmail !== storeEmail ||
        cookieIdentityName !== storeIdentityName ||
        cookieIdentityPrivacy !== storeIdentityPrivacy
      ) {
        const user: AuthUser | null =
          cookieEmail !== ''
            ? {
                email: cookieEmail,
                ...(profile.name ? { name: profile.name } : {}),
              }
            : get().user // Keep existing user if no email

        set({
          login: cookieLogin,
          user,
          isAuthenticated: Boolean(cookieLogin),
          identityName: cookieIdentityName,
          identityPrivacy: cookieIdentityPrivacy,
          hasIdentity: Boolean(cookieIdentityName),
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
      clearProfileCookie()

      set({
        user: null,
        login: '',
        identityName: '',
        identityPrivacy: '',
        isAuthenticated: false,
        hasIdentity: false,
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

    setIdentity: (name, privacy) => {
      // Merge name and privacy into existing profile cookie (preserves email)
      const profile = mergeProfileCookie({ name, privacy })

      set({
        identityName: profile.name || '',
        identityPrivacy: profile.privacy || '',
        hasIdentity: Boolean(profile.name),
      })
    },

    clearIdentity: () => {
      mergeProfileCookie({ name: null, privacy: null })
      set({
        identityName: '',
        identityPrivacy: '',
        hasIdentity: false,
      })
    },
  }
})
