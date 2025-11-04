import { useState } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useNavigate } from '@tanstack/react-router'
import { Loader2, Mail, ArrowLeft, Copy } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { sendVerificationCode, verifyCode } from '@/services/auth-service'
import { Button } from '@/components/ui/button'
import {
  Form,
  FormField,
  FormItem,
  FormMessage,
  FormControl,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'

const emailSchema = z.object({
  email: z.string().email('Please enter a valid email'),
})

const verificationSchema = z.object({
  code: z.string().min(1, 'Please enter a login token'),
})

interface UserAuthFormProps extends React.HTMLAttributes<HTMLFormElement> {
  redirectTo?: string
}

export function UserAuthForm({
  className,
  redirectTo,
  ...props
}: UserAuthFormProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [step, setStep] = useState<'email' | 'verification'>('email')
  const [userEmail, setUserEmail] = useState('')
  const navigate = useNavigate()

  const emailForm = useForm<z.infer<typeof emailSchema>>({
    resolver: zodResolver(emailSchema),
    defaultValues: { email: '' },
  })

  const verificationForm = useForm<z.infer<typeof verificationSchema>>({
    resolver: zodResolver(verificationSchema),
    defaultValues: { code: '' },
  })

  async function onSubmitEmail(data: z.infer<typeof emailSchema>) {
    setIsLoading(true)
    setUserEmail(data.email)

    try {
      const result = await sendVerificationCode(data.email)
      
      if (result.data && result.data.email) {
        toast.success('Verification code sent!', {
          description: result.data.code ? (
            <div className="flex items-center gap-2">
              <span>Your code is: {result.data.code}</span>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 w-6 p-0"
                onClick={() => {
                  navigator.clipboard.writeText(result.data.code!)
                  toast.success('Code copied to clipboard!')
                }}
              >
                <Copy className="h-3 w-3" />
              </Button>
            </div>
          ) : 'Check your email for the verification code.',
        })
        setStep('verification')
      } else {
        toast.error('Failed to send verification code', {
          description: 'Please try again or contact support.',
        })
      }
    } catch (_error) {
      toast.error('Failed to send verification code', {
        description: 'Please try again or contact support.',
      })
    } finally {
      setIsLoading(false)
    }
  }

  async function onSubmitVerification(data: z.infer<typeof verificationSchema>) {
    setIsLoading(true)

    try {
      const result = await verifyCode(data.code)
      
      // verifyCode handles setting auth in the store
      // Just check if it was successful
      if (result.success && (result.accessToken || result.token)) {
        toast.success('Welcome back!', {
          description: `Successfully signed in`,
        })

        // Small delay to ensure store state is updated and cookies are synced
        await new Promise(resolve => setTimeout(resolve, 250))

        // Redirect to the stored location or default to home
        const targetPath = redirectTo || '/'
        navigate({ to: targetPath, replace: true })
      } else {
        toast.error('Invalid verification code', {
          description: result.message || 'Please check your email and try again.',
        })
      }
    } catch (_error) {
      toast.error('Verification failed', {
        description: 'Please try again or contact support.',
      })
    } finally {
      setIsLoading(false)
    }
  }

  function goBackToEmail() {
    setStep('email')
    verificationForm.reset()
  }

  if (step === 'verification') {
    return (
      <div className={cn('grid gap-4', className)}>
        <div className="text-center space-y-2">
          <h3 className="text-lg font-semibold">Enter Login Token</h3>
          <p className="text-sm text-muted-foreground">
            Paste your login token from the external platform
          </p>
          <p className="text-sm font-medium">{userEmail}</p>
        </div>

        <Form {...verificationForm}>
          <form
            onSubmit={verificationForm.handleSubmit(onSubmitVerification)}
            className="grid gap-4"
          >
            <FormField
              control={verificationForm.control}
              name="code"
              render={({ field }) => (
                <FormItem>
                  <FormControl>
                    <Input 
                      placeholder="Login Token"
                      className="text-center font-mono tracking-wider"
                      {...field} 
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            
            <div className="space-y-2">
              <Button 
                type="submit" 
                className="w-full" 
                disabled={isLoading}
              >
                {isLoading ? <Loader2 className="animate-spin" /> : <Mail />}
                Authenticate with Token
              </Button>
              
              <Button
                type="button"
                variant="ghost"
                onClick={goBackToEmail}
                className="w-full"
              >
                <ArrowLeft className="mr-2 h-4 w-4" />
                Back to email
              </Button>
            </div>
          </form>
        </Form>
      </div>
    )
  }

  return (
    <Form {...emailForm}>
      <form
        onSubmit={emailForm.handleSubmit(onSubmitEmail)}
        className={cn('grid gap-3', className)}
        {...props}
      >
        <FormField
          control={emailForm.control}
          name="email"
          render={({ field }) => (
            <FormItem>
              <FormControl>
                <Input placeholder="Email" {...field} />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        
        <Button className="mt-2" disabled={isLoading}>
          {isLoading ? <Loader2 className="animate-spin" /> : <Mail />}
          Send Verification Code
        </Button>
      </form>
    </Form>
  )
}
