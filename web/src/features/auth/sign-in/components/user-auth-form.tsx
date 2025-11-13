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
  step?: 'email' | 'verification'
  setStep?: (step: 'email' | 'verification') => void
}

export function UserAuthForm({
  className,
  redirectTo,
  step: externalStep,
  setStep: externalSetStep,
  ...props
}: UserAuthFormProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [internalStep, setInternalStep] = useState<'email' | 'verification'>('email')
  const [userEmail, setUserEmail] = useState('')
  const navigate = useNavigate()

  // Use external step/setStep if provided, otherwise use internal state
  const step = externalStep ?? internalStep
  const setStep = externalSetStep ?? setInternalStep

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
                onClick={async (e) => {
                  e.preventDefault()
                  e.stopPropagation()
                  const code = result.data.code!
                  
                  try {
                    // Try modern clipboard API first
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                      await navigator.clipboard.writeText(code)
                      toast.success('Code copied to clipboard!')
                    } else {
                      // Fallback for older browsers
                      const textArea = document.createElement('textarea')
                      textArea.value = code
                      textArea.style.position = 'fixed'
                      textArea.style.left = '-999999px'
                      textArea.style.top = '-999999px'
                      document.body.appendChild(textArea)
                      textArea.focus()
                      textArea.select()
                      
                      try {
                        const successful = document.execCommand('copy')
                        if (successful) {
                          toast.success('Code copied to clipboard!')
                        } else {
                          throw new Error('Copy command failed')
                        }
                      } finally {
                        document.body.removeChild(textArea)
                      }
                    }
                  } catch (error) {
                    console.error('Failed to copy code:', error)
                    toast.error('Failed to copy code', {
                      description: 'Please copy manually: ' + code,
                    })
                  }
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
      if (result.success && result.login) {
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
          <h3 className="text-lg font-semibold">Enter Login Code</h3>
          <p className="text-sm text-muted-foreground">
            Paste your login code
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
                      placeholder="Code"
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
                Authenticate
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
