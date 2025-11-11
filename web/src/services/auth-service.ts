// feat(auth): implement login-header based auth flow
import authApi, {
  type AuthUser,
  type RequestCodeResponse,
  type VerifyCodeResponse,
} from '@/api/auth'
import { useAuthStore } from '@/stores/auth-store'
import { setCookie, getCookie } from '@/lib/cookies'

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

    // Store email in cookie and auth store
    // This allows us to show user info even before verification completes
    if (response.data?.email) {
      setCookie('user_email', response.data.email, {
        maxAge: 60 * 60 * 24 * 7, // 7 days in seconds
        path: '/',
        sameSite: 'strict',
        secure: window.location.protocol === 'https:',
      })

      const currentUser = useAuthStore.getState().user
      useAuthStore.getState().setUser({
        ...currentUser,
        email: response.data.email,
        name: extractNameFromEmail(response.data.email),
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
 * 3. Get user email from `user_email` cookie (set during requestCode)
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

    // Get email from cookie (set during requestCode) or from response
    const emailFromCookie = getCookie('user_email')
    const email = response.user?.email || emailFromCookie

    // Determine success based on presence of login
    const isSuccess =
      response.success !== undefined
        ? Boolean(response.success)
        : Boolean(login)

    // Store credentials and user data if successful
    if (isSuccess && login) {
      // Create user object if we have email
      const user: AuthUser | null = email
        ? {
            email,
            name: extractNameFromEmail(email),
            // Add other fields if available from response
            accountNo: response.user?.accountNo,
            role: response.user?.role,
            exp: response.user?.exp,
          }
        : null

      // Store in auth store (which will also persist to cookies)
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

export type { AuthUser, RequestCodeResponse, VerifyCodeResponse }
