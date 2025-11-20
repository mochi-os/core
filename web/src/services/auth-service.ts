// feat(auth): implement login-header based auth flow
import authApi, {
  type AuthUser,
  type RequestCodeResponse,
  type VerifyCodeResponse,
} from '@/api/auth'
import { useAuthStore } from '@/stores/auth-store'
import {
  mergeProfileCookie,
  readProfileCookie,
} from '@/lib/profile-cookie'

const devConsole = globalThis.console

/**
 * Log errors in development mode only
 */
const logError = (context: string, error: unknown) => {
  if (import.meta.env.DEV) {
    devConsole?.error?.(`[Auth Service] ${context}`, error)
  }
}

/**
 * Request verification code for email
 *
 * @param email - User's email address
 * @returns Response with verification code (in dev) or success message
 */
export const requestCode = async (
  email: string
): Promise<RequestCodeResponse> => {
  try {
    const response = await authApi.requestCode({ email })

    // Store email in mochi_me profile cookie immediately
    // This ensures the cookie exists even if identity page is skipped
    if (response.data?.email) {
      mergeProfileCookie({
        email: response.data.email,
        // Don't store extracted name yet - only store if identity form is shown
      })

      const currentUser = useAuthStore.getState().user
      useAuthStore.getState().setUser({
        ...currentUser,
        email: response.data.email,
      })
    }

    return response
  } catch (error) {
    logError('Failed to request login code', error)
    throw error
  }
}

/**
 * Verify code and authenticate user
 *
 * Authentication Flow:
 * 1. Call backend to verify the code
 * 2. Extract `login` field (primary credential) from response (ignore `token` field)
 * 3. Get user email from profile cookie (set during requestCode)
 * 4. Store `login` in cookie and Zustand store via setAuth()
 *
 * The `login` value is the primary credential and will be used as-is
 * in the Authorization header.
 *
 * @param code - Verification code from email
 * @returns Response with login and user info
 */
export const verifyCode = async (
  code: string
): Promise<VerifyCodeResponse & { success: boolean }> => {
  try {
    const response = await authApi.verifyCode({ code })

    // Extract login from response (ignore token field completely)
    const login = response.login || ''

    // Get email from profile cookie (set during requestCode) or from response
    const profile = readProfileCookie()
    const emailFromCookie = profile.email
    const email = response.user?.email || emailFromCookie

    const nameFromResponse = response.name || response.user?.name

    // Determine success based on presence of login
    const isSuccess =
      response.success !== undefined
        ? Boolean(response.success)
        : Boolean(login)

    // Store credentials and user data if successful
    if (isSuccess && login) {
      // Ensure email persists in profile cookie (it was set during requestCode)
      if (email && !profile.email) {
        mergeProfileCookie({ email })
      }

      // Create user object - rely solely on stored identity data
      const user: AuthUser | null = email
        ? {
            email,
            ...(nameFromResponse
              ? { name: nameFromResponse }
              : profile.name
              ? { name: profile.name }
              : {}),
            accountNo: response.user?.accountNo,
            role: response.user?.role,
            exp: response.user?.exp,
          }
        : null

      // Store in auth store (which will preserve existing profile cookie data)
      useAuthStore.getState().setAuth(user, login)
    }

    return {
      ...response,
      success: isSuccess,
    }
  } catch (error) {
    logError('Failed to verify login code', error)
    throw error
  }
}

/**
 * Validate current session by fetching user info
 *
 * This function checks if the current session is valid by:
 * 1. Checking for credentials (login) in store
 * 2. Optionally calling /me endpoint to validate with backend
 * 3. Updating user data if successful
 * 4. Clearing auth if validation fails
 *
 * @returns User info if session is valid, null otherwise
 */
export const validateSession = async (): Promise<AuthUser | null> => {
  try {
    // Check if we have credentials
    const { login, user } = useAuthStore.getState()

    if (!login) {
      return null
    }

    // TODO: Uncomment when backend implements /me endpoint
    // try {
    //   const response: MeResponse = await authApi.me()
    //   useAuthStore.getState().setUser(response.user)
    //   return response.user
    // } catch (meError) {
    //   logError('Failed to fetch user profile from /me', meError)
    //   // If /me fails, clear auth
    //   useAuthStore.getState().clearAuth()
    //   return null
    // }

    // For now, just return the current user from store
    // This assumes the credentials are valid if they exist
    return user
  } catch (error) {
    logError('Failed to validate session', error)
    // Clear auth on validation failure
    useAuthStore.getState().clearAuth()
    return null
  }
}

/**
 * Logout user
 *
 * This function:
 * 1. Optionally calls backend logout endpoint
 * 2. Clears all authentication state (cookies + store)
 * 3. Always succeeds (clears local state even if backend call fails)
 */
export const logout = async (): Promise<void> => {
  try {
    // TODO: Uncomment when backend implements logout endpoint
    // await authApi.logout()

    // Clear auth state (removes cookies and clears store)
    useAuthStore.getState().clearAuth()
  } catch (error) {
    logError('Logout failed', error)
    // Clear auth even if backend call fails
    useAuthStore.getState().clearAuth()
  }
}

/**
 * Load user profile from /me endpoint
 *
 * This function:
 * 1. Checks for credentials in store
 * 2. Calls /me endpoint to get user profile
 * 3. Updates store with user data
 * 4. Returns user info or null
 *
 * Call this after successful authentication to populate user data for UI.
 *
 * @returns User info if successful, null otherwise
 */
export const loadUserProfile = async (): Promise<AuthUser | null> => {
  try {
    // Check if we have credentials first
    const { login } = useAuthStore.getState()

    if (!login) {
      return null
    }

    // TODO: Uncomment when backend implements /me endpoint
    // try {
    //   const response: MeResponse = await authApi.me()
    //   useAuthStore.getState().setUser(response.user)
    //   return response.user
    // } catch (meError) {
    //   logError('Failed to fetch user profile from /me', meError)
    //   // Fall through to return current user from store
    // }

    // For now, return current user from store
    // (might be from JWT decode or from login response)
    return useAuthStore.getState().user
  } catch (error) {
    logError('Failed to load user profile', error)
    return null
  }
}

// Alias for backward compatibility
export const sendVerificationCode = requestCode

type IdentityPayload = {
  name: string
  privacy: 'public' | 'private'
}

/**
 * Submit identity details to /login/identity
 *
 * Sends an x-www-form-urlencoded payload with name + privacy. On success,
 * identity data is persisted to cookies + auth store.
 */
export const submitIdentity = async ({
  name,
  privacy,
}: IdentityPayload): Promise<void> => {
  try {
    const body = new URLSearchParams()
    body.set('name', name)
    body.set('privacy', privacy)

    const response = await fetch(`${window.location.origin}/login/identity`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      credentials: 'include', // ensure login cookie is sent
      body,
    })

    if (!response.ok) {
      throw new Error(`Identity request failed with status ${response.status}`)
    }

    useAuthStore.getState().setIdentity(name, privacy)
  } catch (error) {
    logError('Failed to submit identity', error)
    throw error
  }
}

export type { AuthUser, RequestCodeResponse, VerifyCodeResponse }
