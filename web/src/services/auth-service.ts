import authApi, {
  type AuthUser,
  type RequestCodeResponse,
  type VerifyCodeResponse,
} from '@/api/auth'
import { useAuthStore } from '@/stores/auth-store'
import { mergeProfileCookie, readProfileCookie } from '@/lib/profile-cookie'

export const requestCode = async (
  email: string
): Promise<RequestCodeResponse> => {
  try {
    const response = await authApi.requestCode({ email })

    const responseStatus = response.status?.toLowerCase()
    const devCode = response.data?.code
    const isSuccess = responseStatus === 'ok' || Boolean(devCode)

    if (!isSuccess) {
      throw new Error(response.message || 'Failed to request login code')
    }

    mergeProfileCookie({
      email,
    })

    const currentUser = useAuthStore.getState().user
    useAuthStore.getState().setUser({
      ...currentUser,
      email,
    })

    return response
  } catch (error) {
    console.error('Failed to request login code', error)
    throw error
  }
}

export const verifyCode = async (
  code: string
): Promise<VerifyCodeResponse & { success: boolean }> => {
  try {
    const response = await authApi.verifyCode({ code })

    const login = response.token || ''

    const profile = readProfileCookie()
    const emailFromCookie = profile.email
    const email = response.user?.email || emailFromCookie

    const nameFromResponse = response.name || response.user?.name

    const isSuccess =
      response.success !== undefined
        ? Boolean(response.success)
        : Boolean(login)

    if (isSuccess && login) {
      if (email && !profile.email) {
        mergeProfileCookie({ email })
      }

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

      useAuthStore.getState().setAuth(user, login)
    }

    return {
      ...response,
      success: isSuccess,
    }
  } catch (error) {
    console.error('Failed to verify login code', error)
    throw error
  }
}

export const logout = async (): Promise<void> => {
  try {
    useAuthStore.getState().clearAuth()
  } catch (error) {
    console.error('Logout failed', error)
  }
}

export const loadUserProfile = async (): Promise<AuthUser | null> => {
  try {
    const { token } = useAuthStore.getState()

    if (!token) {
      return null
    }

    return useAuthStore.getState().user
  } catch (error) {
    console.error('Failed to load user profile', error)
    return null
  }
}

type IdentityPayload = {
  name: string
  privacy: 'public' | 'private'
}

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
      credentials: 'include',
      body,
    })

    if (!response.ok) {
      throw new Error(`Identity request failed with status ${response.status}`)
    }

    useAuthStore.getState().setIdentity(name, privacy)
  } catch (error) {
    console.error('Failed to submit identity', error)
    throw error
  }
}

export type { AuthUser, RequestCodeResponse, VerifyCodeResponse }
