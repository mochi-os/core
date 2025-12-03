import Cookies from 'js-cookie'

const devConsole = globalThis.console

const DEFAULT_MAX_AGE = 60 * 60 * 24 * 7 // 7 days in seconds

export const AUTH_COOKIES = {
  TOKEN: 'token',
  PROFILE: 'mochi_me',
} as const

export interface CookieOptions {
  maxAge?: number
  httpOnly?: boolean
  secure?: boolean
  sameSite?: 'strict' | 'lax' | 'none'
  path?: string
}

export function getCookie(name: string): string | undefined {
  return Cookies.get(name)
}

export function setCookie(
  name: string,
  value: string,
  options: CookieOptions = {}
): void {
  const {
    maxAge = DEFAULT_MAX_AGE,
    httpOnly = false,
    secure = window.location.protocol === 'https:',
    sameSite = 'strict',
    path = '/',
  } = options

  const expires = maxAge / (60 * 60 * 24)

  if (httpOnly && import.meta.env.DEV) {
    devConsole?.warn?.(
      `[Cookies] HttpOnly flag requested for "${name}" but cannot be set via JavaScript. ` +
        `HttpOnly cookies must be set server-side using Set-Cookie header.`
    )
  }

  Cookies.set(name, value, {
    expires,
    path,
    sameSite,
    secure,
  })
}

export function removeCookie(name: string, path: string = '/'): void {
  Cookies.remove(name, { path })
}
