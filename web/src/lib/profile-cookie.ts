import {
  AUTH_COOKIES,
  getCookie,
  setCookie,
  removeCookie,
  type CookieOptions,
} from '@/lib/cookies'

export type IdentityPrivacy = 'public' | 'private'

export interface ProfileCookieData {
  email?: string
  name?: string
  privacy?: IdentityPrivacy
}

export interface ProfileCookiePatch {
  email?: string | null
  name?: string | null
  privacy?: IdentityPrivacy | null
}

const defaultCookieOptions = (): CookieOptions => ({
  maxAge: 60 * 60 * 24 * 7,
  path: '/',
  sameSite: 'strict',
  secure:
    typeof window !== 'undefined'
      ? window.location.protocol === 'https:'
      : true,
})

const isIdentityPrivacy = (value: unknown): value is IdentityPrivacy =>
  value === 'public' || value === 'private'

const sanitizeProfile = (
  profile: ProfileCookieData
): ProfileCookieData => {
  const sanitized: ProfileCookieData = {}

  if (typeof profile.email === 'string' && profile.email.length > 0) {
    sanitized.email = profile.email
  }

  if (typeof profile.name === 'string' && profile.name.length > 0) {
    sanitized.name = profile.name
  }

  if (isIdentityPrivacy(profile.privacy)) {
    sanitized.privacy = profile.privacy
  }

  return sanitized
}

export const readProfileCookie = (): ProfileCookieData => {
  const raw = getCookie(AUTH_COOKIES.PROFILE)
  if (!raw) {
    return {}
  }

  try {
    const parsed = JSON.parse(raw) as ProfileCookieData
    return sanitizeProfile(parsed)
  } catch {
    removeCookie(AUTH_COOKIES.PROFILE, '/')
    return {}
  }
}

const writeProfileCookie = (
  profile: ProfileCookieData,
  options?: CookieOptions
): ProfileCookieData => {
  const sanitized = sanitizeProfile(profile)

  if (Object.keys(sanitized).length === 0) {
    removeCookie(AUTH_COOKIES.PROFILE, '/')
    return {}
  }

  setCookie(
    AUTH_COOKIES.PROFILE,
    JSON.stringify(sanitized),
    options ?? defaultCookieOptions()
  )

  return sanitized
}

export const mergeProfileCookie = (
  partial: ProfileCookiePatch,
  options?: CookieOptions
): ProfileCookieData => {
  const current = readProfileCookie()

  return writeProfileCookie(
    {
      email:
        partial.email === null ? undefined : partial.email ?? current.email,
      name:
        partial.name === null ? undefined : partial.name ?? current.name,
      privacy:
        partial.privacy === null
          ? undefined
          : partial.privacy ?? current.privacy,
    },
    options
  )
}

export const clearProfileCookie = (): void => {
  removeCookie(AUTH_COOKIES.PROFILE, '/')
}

