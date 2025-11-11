import { createFileRoute } from '@tanstack/react-router'
import { useEffect } from 'react'

export const Route = createFileRoute('/_authenticated/')({
  component: () => {
    useEffect(() => {
      // Redirect to default app after successful authentication
      window.location.href = import.meta.env.VITE_DEFAULT_APP_URL
    }, [])
  },
})
