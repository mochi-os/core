const endpoints = {
  : {
    login: '/login',
    signup: '/signup',
    verify: '/login/auth',
    logout: '/logout',
    me: '/me', // Optional: Load user profile for UI
  },auth
} as const

export type Endpoints = typeof endpoints

export default endpoints
