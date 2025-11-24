export interface AuthUser {
  email: string
  name?: string
  accountNo?: string
  role?: string[]
  exp?: number
  avatar?: string
}

export interface RequestCodeRequest {
  email: string
}

export interface RequestCodeResponse {
  status: string
  message?: string
  data?: {
    code?: string
  }
}

export interface SignupRequest {
  email: string
}

export interface SignupResponse {
  success: boolean
  message?: string
}

export interface VerifyCodeRequest {
  code: string
}

export interface VerifyCodeResponse {
  success: boolean
  token?: string // Backend returns this but we ignore it
  login?: string
  user?: AuthUser
  name?: string
  message?: string
  expiresIn?: number
  expires_in?: number
}

export interface RefreshTokenRequest {
  refreshToken: string
}

export interface RefreshTokenResponse {
  token: string
  refreshToken?: string
  login?: string
  user?: AuthUser
  expiresIn?: number
  expires_in?: number
}

export interface MeResponse {
  user: AuthUser
}
