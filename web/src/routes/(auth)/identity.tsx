import { useEffect } from 'react'
import { z } from 'zod'
import { createFileRoute, redirect } from '@tanstack/react-router'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { AuthLayout } from '@/features/auth/auth-layout'
import { IdentityForm } from '@/features/auth/identity-form'
import { useAuthStore } from '@/stores/auth-store'

const searchSchema = z.object({
  redirect: z.string().optional(),
})

export const Route = createFileRoute('/(auth)/identity')({
  validateSearch: searchSchema,
  beforeLoad: ({ search, location }) => {
    const store = useAuthStore.getState()

    if (!store.isInitialized) {
      store.syncFromCookie()
    }

    if (!store.isAuthenticated) {
      throw redirect({
        to: '/login',
        search: {
          redirect: search.redirect ?? location.href,
        },
      })
    }
  },
  component: IdentityRouteComponent,
})

function IdentityRouteComponent() {
  const searchParams = Route.useSearch()
  const redirectTo =
    'redirect' in searchParams ? searchParams.redirect : undefined
  const hasIdentity = useAuthStore((state) => state.hasIdentity)

  useEffect(() => {
    if (hasIdentity) {
      const fallback = import.meta.env.VITE_DEFAULT_APP_URL || '/'
      window.location.href = redirectTo || fallback
    }
  }, [hasIdentity, redirectTo])

  if (hasIdentity) {
    return null
  }

  return (
    <AuthLayout>
      <Card>
        <CardHeader>
          <CardTitle className="text-lg tracking-tight">
            Finish setting up your profile
          </CardTitle>
        </CardHeader>
        <CardContent>
          <IdentityForm redirectTo={redirectTo} />
        </CardContent>
      </Card>
    </AuthLayout>
  )
}

