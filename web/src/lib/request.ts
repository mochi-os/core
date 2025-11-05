import {
  isAxiosError,
  type AxiosError,
  type AxiosRequestConfig,
  type AxiosResponse,
} from 'axios'
import apiClient from '@/lib/apiClient'

const devConsole = globalThis.console

export interface ApiErrorParams {
  message: string
  status?: number
  data?: unknown
  cause?: unknown
}

export class ApiError extends Error {
  readonly status?: number
  readonly data?: unknown
  readonly cause?: unknown

  constructor({ message, status, data, cause }: ApiErrorParams) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.data = data
    if (cause !== undefined) {
      this.cause = cause
    }
  }
}

const buildApiError = (
  error: unknown,
  fallbackMessage: string,
  requestConfig: AxiosRequestConfig
): ApiError => {
  if (isAxiosError(error)) {
    const axiosError = error as AxiosError<{ message?: string }>
    const status = axiosError.response?.status
    const responseData = axiosError.response?.data
    const message =
      responseData?.message ??
      axiosError.message ??
      `${requestConfig.method ?? 'request'} ${requestConfig.url} failed`

    return new ApiError({
      message,
      status,
      data: responseData,
      cause: error,
    })
  }

  if (error instanceof ApiError) {
    return error
  }

  if (error instanceof Error) {
    return new ApiError({
      message: error.message || fallbackMessage,
      cause: error,
    })
  }

  return new ApiError({ message: fallbackMessage, data: error })
}

const logRequestError = (
  error: ApiError,
  requestConfig: AxiosRequestConfig
): void => {
  if (!import.meta.env.DEV) {
    return
  }

  const method = requestConfig.method?.toUpperCase() ?? 'REQUEST'
  const url = requestConfig.url ?? '<unknown>'
  devConsole?.error?.(
    `[API] ${method} ${url} failed`,
    error.cause ?? error.data ?? error.message
  )
}

export async function request<TResponse>(
  config: AxiosRequestConfig
): Promise<TResponse> {
  const requestConfig: AxiosRequestConfig = {
    ...config,
  }

  try {
    const response: AxiosResponse<TResponse> = await apiClient.request<
      TResponse
    >(requestConfig)
    return response.data
  } catch (unknownError) {
    const apiError = buildApiError(
      unknownError,
      'Unexpected API error',
      requestConfig
    )
    logRequestError(apiError, requestConfig)
    throw apiError
  }
}

export const requestHelpers = {
  get: <TResponse>(
    url: string,
    config?: Omit<AxiosRequestConfig, 'url' | 'method'>
  ): Promise<TResponse> =>
    request<TResponse>({
      method: 'GET',
      url,
      ...config,
    }),
  post: <TResponse, TBody = unknown>(
    url: string,
    data?: TBody,
    config?: Omit<AxiosRequestConfig<TBody>, 'url' | 'method' | 'data'>
  ): Promise<TResponse> =>
    request<TResponse>({
      method: 'POST',
      url,
      data,
      ...config,
    }),
  put: <TResponse, TBody = unknown>(
    url: string,
    data?: TBody,
    config?: Omit<AxiosRequestConfig<TBody>, 'url' | 'method' | 'data'>
  ): Promise<TResponse> =>
    request<TResponse>({
      method: 'PUT',
      url,
      data,
      ...config,
    }),
  patch: <TResponse, TBody = unknown>(
    url: string,
    data?: TBody,
    config?: Omit<AxiosRequestConfig<TBody>, 'url' | 'method' | 'data'>
  ): Promise<TResponse> =>
    request<TResponse>({
      method: 'PATCH',
      url,
      data,
      ...config,
    }),
  delete: <TResponse>(
    url: string,
    config?: Omit<AxiosRequestConfig, 'url' | 'method'>
  ): Promise<TResponse> =>
    request<TResponse>({
      method: 'DELETE',
      url,
      ...config,
    }),
}

export default request
