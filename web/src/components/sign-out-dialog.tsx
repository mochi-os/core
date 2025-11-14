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
      title='Log out'
      desc='Are you sure you want to log out? You will need to log in again to access your account.'
      confirmText={isLoggingOut ? 'Logging out...' : 'Log out'}
      className='sm:max-w-sm'
      disabled={isLoggingOut}
      handleConfirm={handleSignOut}
    />
  )
}
