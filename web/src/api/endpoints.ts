const endpoints = {
  auth: {
    login: '/login',
    signup: '/signup',
    verify: '/login/auth',
    me: '/me', // Optional: Load user profile for UI
  },
} as const

export type Endpoints = typeof endpoints

export default endpoints
