import { create } from 'zustand'
import { getCookie, setCookie, removeCookie } from '@/lib/cookies'
import {
  clearProfileCookie,
  mergeProfileCookie,
  readProfileCookie,
  type IdentityPrivacy,
} from '@/lib/profile-cookie'

const TOKEN_COOKIE = 'token'

export interface AuthUser {
  email: string
  name?: string
}

interface AuthState {
  user: AuthUser | null
  token: string
  isLoading: boolean
  isInitialized: boolean
  identityName: string
  identityPrivacy: IdentityPrivacy | ''
  isAuthenticated: boolean
  hasIdentity: boolean

  setAuth: (user: AuthUser | null, token: string) => void
  setUser: (user: AuthUser | null) => void
  setToken: (token: string) => void
  setLoading: (isLoading: boolean) => void
  syncFromCookie: () => void
  clearAuth: () => void
  initialize: () => void
  setIdentity: (name: string, privacy: IdentityPrivacy) => void
  clearIdentity: () => void
}

export const useAuthStore = create<AuthState>()((set, get) => {
  const initialToken = getCookie(TOKEN_COOKIE) || ''
  const profile = readProfileCookie()
  const initialEmail = profile.email || ''
  const initialIdentityName = profile.name || ''
  const initialIdentityPrivacy: IdentityPrivacy | '' = profile.privacy || ''

  const initialUser: AuthUser | null =
    initialEmail !== ''
      ? {
          email: initialEmail,
          ...(profile.name ? { name: profile.name } : {}),
        }
      : null

  return {
    user: initialUser,
    token: initialToken,
    isLoading: false,
    isInitialized: false,
    identityName: initialIdentityName,
    identityPrivacy: initialIdentityPrivacy,
    isAuthenticated: Boolean(initialToken),
    hasIdentity: Boolean(initialIdentityName),

    setAuth: (user, token) => {
      if (token) {
        setCookie(TOKEN_COOKIE, token, {
          maxAge: 60 * 60 * 24 * 7,
          path: '/',
          sameSite: 'strict',
          secure: window.location.protocol === 'https:',
        })
      }

      const currentProfile = readProfileCookie()
      const mergedProfile = mergeProfileCookie({
        email: user?.email ?? currentProfile.email ?? null,
        name: currentProfile.name || user?.name || undefined,
      })

      set({
        user,
        token,
        isAuthenticated: Boolean(token),
        identityName: mergedProfile.name || '',
        identityPrivacy: mergedProfile.privacy || '',
        hasIdentity: Boolean(mergedProfile.name),
        isInitialized: true,
      })
    },

    setUser: (user) => {
      const currentProfile = readProfileCookie()
      mergeProfileCookie({
        email: user?.email ?? currentProfile.email ?? null,
        name: currentProfile.name || user?.name || undefined,
      })

      set({
        user,
        isAuthenticated: Boolean(get().token),
      })
    },

    setToken: (token) => {
      if (token) {
        setCookie(TOKEN_COOKIE, token, {
          maxAge: 60 * 60 * 24 * 7,
          path: '/',
          sameSite: 'strict',
          secure: window.location.protocol === 'https:',
        })
      } else {
        removeCookie(TOKEN_COOKIE)
      }

      set({
        token,
        isAuthenticated: Boolean(token),
      })
    },

    setLoading: (isLoading) => {
      set({ isLoading })
    },

    syncFromCookie: () => {
      const cookieToken = getCookie(TOKEN_COOKIE) || ''
      const profile = readProfileCookie()
      const cookieEmail = profile.email || ''
      const cookieIdentityName = profile.name || ''
      const cookieIdentityPrivacy: IdentityPrivacy | '' = profile.privacy || ''
      const storeToken = get().token
      const storeEmail = get().user?.email
      const storeIdentityName = get().identityName
      const storeIdentityPrivacy = get().identityPrivacy

      if (
        cookieToken !== storeToken ||
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
            : get().user

        set({
          user,
          token: cookieToken,
          isAuthenticated: Boolean(cookieToken),
          identityName: cookieIdentityName,
          identityPrivacy: cookieIdentityPrivacy,
          hasIdentity: Boolean(cookieIdentityName),
          isInitialized: true,
        })
      } else {
        set({ isInitialized: true })
      }
    },

    clearAuth: () => {
      removeCookie(TOKEN_COOKIE, '/')
      clearProfileCookie()

      set({
        user: null,
        token: '',
        identityName: '',
        identityPrivacy: '',
        isAuthenticated: false,
        hasIdentity: false,
        isLoading: false,
        isInitialized: true,
      })
    },

    initialize: () => {
      get().syncFromCookie()
    },

    setIdentity: (name, privacy) => {
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
