import { useLogout } from '@/hooks/useLogout'
import { ConfirmDialog } from '@/components/confirm-dialog'

interface SignOutDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function SignOutDialog({ open, onOpenChange }: SignOutDialogProps) {
  const { logout, isLoggingOut } = useLogout()

  const handleSignOut = async () => {
    await logout()
    onOpenChange(false)
  }

  return (
    <ConfirmDialog
      open={open}
      onOpenChange={onOpenChange}
      title='Sign out'
      desc='Are you sure you want to sign out? You will need to sign in again to access your account.'
      confirmText={isLoggingOut ? 'Signing out...' : 'Sign out'}
      handleConfirm={handleSignOut}
      className='sm:max-w-sm'
      disabled={isLoggingOut}
    />
  )
}
