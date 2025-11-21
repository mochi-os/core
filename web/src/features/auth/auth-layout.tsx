import { ThemeSwitch } from '@/components/theme-switch'
import { Logo } from '@/assets/logo'

type AuthLayoutProps = {
  children: React.ReactNode
}

export function AuthLayout({ children }: AuthLayoutProps) {
  return (
    <div className='container relative grid h-svh max-w-none items-center justify-center'>
      <div className='absolute right-4 top-4'>
        <ThemeSwitch />
      </div>
      <div className='mx-auto flex w-full flex-col justify-center space-y-2 py-8 sm:w-[480px] sm:p-8'>
        <div className='mb-4 flex items-center justify-center'>
          <Logo className='me-2' />
          <h1 className='text-xl font-medium'>Mochi OS</h1>
        </div>
        {children}
      </div>
    </div>
  )
}
