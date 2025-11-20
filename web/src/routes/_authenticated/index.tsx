import { createFileRoute } from '@tanstack/react-router'
import { useEffect } from 'react'

function RedirectToDefaultApp() {
  useEffect(() => {
    window.location.href = import.meta.env.VITE_DEFAULT_APP_URL
  }, [])

  return null
}

export const Route = createFileRoute('/_authenticated/')({
  component: RedirectToDefaultApp,
})
