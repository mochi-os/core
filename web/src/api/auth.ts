import endpoints from '@/api/endpoints'
import {
  type AuthUser,
  type MeResponse,
  type RequestCodeRequest,
  type RequestCodeResponse,
  type SignupRequest,
  type SignupResponse,
  type VerifyCodeRequest,
  type VerifyCodeResponse,
} from '@/api/types/auth'
import { requestHelpers } from '@/lib/request'

const requestCode = (payload: RequestCodeRequest) =>
  requestHelpers.post<RequestCodeResponse>(endpoints.auth.login, payload)

const signup = (payload: SignupRequest) =>
  requestHelpers.post<SignupResponse>(endpoints.auth.signup, payload)

const verifyCode = (payload: VerifyCodeRequest) =>
  requestHelpers.post<VerifyCodeResponse>(endpoints.auth.verify, payload)

const me = () =>
  requestHelpers.get<MeResponse>(endpoints.auth.me)

export const authApi = {
  requestCode,
  signup,
  verifyCode,
  me,
}

export type {
  AuthUser,
  MeResponse,
  RequestCodeRequest,
  RequestCodeResponse,
  SignupRequest,
  SignupResponse,
  VerifyCodeRequest,
  VerifyCodeResponse,
}

export default authApi
