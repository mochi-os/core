import { createFileRoute } from '@tanstack/react-router'
import { useEffect } from 'react'

export const Route = createFileRoute('/_authenticated/')({
  component: () => {
    useEffect(() => {
      // Redirect to chat app after successful authentication
      window.location.href = '/apps/chat/'
    }, [])
    
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <h1 className="text-2xl font-bold mb-2">Logged in successfully</h1>
          <p className="text-muted-foreground">Redirecting to chat app...</p>
        </div>
      </div>
    )
  },
})
