// feat(auth): implement login-header based auth flow
/**
 * Cookie utility functions using js-cookie library
 * Provides enhanced security and features compared to manual document.cookie approach
 * 
 * Cookie Names for Authentication:
 * - 'login': Primary credential (raw value used as Authorization header)
 * - 'token': Optional fallback credential (JWT used with Bearer prefix)
 * 
 * Cookie Configuration:
 * - Secure: true in production (HTTPS only)
 * - SameSite: 'strict' (CSRF protection)
 * - Path: '/' (available to all routes)
 * - Expires: 7 days by default
 */

import Cookies from 'js-cookie'

const DEFAULT_MAX_AGE = 60 * 60 * 24 * 7 // 7 days in seconds

/**
 * Authentication cookie names
 * These are used by the auth store and API client
 */
export const AUTH_COOKIES = {
  LOGIN: 'login',      // Primary credential
  TOKEN: 'token',      // Fallback credential
} as const

/**
 * Cookie options interface
 * Extends js-cookie options with additional security settings
 */
export interface CookieOptions {
  maxAge?: number      // Max age in seconds (default: 7 days)
  httpOnly?: boolean   // HttpOnly flag (requires server-side setting)
  secure?: boolean     // Secure flag (default: auto-detect from protocol)
  sameSite?: 'strict' | 'lax' | 'none' // SameSite policy (default: 'strict')
  path?: string        // Cookie path (default: '/')
}

/**
 * Get a cookie value by name
 * 
 * Note: HttpOnly cookies cannot be read via JavaScript.
 * If you need HttpOnly cookies, they must be set server-side
 * and will be automatically sent with requests.
 */
export function getCookie(name: string): string | undefined {
  return Cookies.get(name)
}

/**
 * Set a cookie with name, value, and optional configuration
 * 
 * @param name - Cookie name
 * @param value - Cookie value
 * @param options - Cookie configuration options
 * 
 * Security Notes:
 * - HttpOnly: If true, cookie cannot be accessed via JavaScript (more secure)
 *   However, js-cookie cannot set HttpOnly cookies - this must be done server-side.
 *   This option is documented for future server-side implementation.
 * - Secure: Automatically enabled in production (HTTPS only)
 * - SameSite: Defaults to 'strict' for CSRF protection
 * 
 * @example
 * ```ts
 * // Standard cookie (readable by JS)
 * setCookie('token', 'abc123')
 * 
 * // Custom expiration
 * setCookie('token', 'abc123', { maxAge: 60 * 60 * 24 * 30 }) // 30 days
 * 
 * // For HttpOnly cookies, use server-side Set-Cookie header:
 * // Set-Cookie: token=abc123; HttpOnly; Secure; SameSite=Strict; Path=/
 * ```
 */
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

  // Convert maxAge from seconds to days for js-cookie
  const expires = maxAge / (60 * 60 * 24)

  // Log warning if HttpOnly is requested (cannot be set via JS)
  if (httpOnly && import.meta.env.DEV) {
    console.warn(
      `[Cookies] HttpOnly flag requested for "${name}" but cannot be set via JavaScript. ` +
      `HttpOnly cookies must be set server-side using Set-Cookie header.`
    )
  }

  Cookies.set(name, value, {
    expires,
    path,
    sameSite,
    secure,
    // Note: HttpOnly cannot be set via js-cookie (requires server-side)
  })
}

/**
 * Remove a cookie by name
 * 
 * @param name - Cookie name to remove
 * @param path - Cookie path (default: '/')
 * 
 * Note: If the cookie was set with a specific path, you must
 * provide the same path to remove it successfully.
 */
export function removeCookie(name: string, path: string = '/'): void {
  Cookies.remove(name, { path })
}
