import { useState } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { toast } from 'sonner'
import { submitIdentity } from '@/services/auth-service'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import {
  RadioGroup,
  RadioGroupItem,
} from '@/components/ui/radio-group'

const identitySchema = z.object({
  name: z.string().min(2, 'Please enter your name'),
  privacy: z.enum(['public', 'private']),
})

type IdentityFormValues = z.infer<typeof identitySchema>

interface IdentityFormProps {
  redirectTo?: string
}

export function IdentityForm({ redirectTo }: IdentityFormProps) {
  const [isSubmitting, setIsSubmitting] = useState(false)

  const form = useForm<IdentityFormValues>({
    resolver: zodResolver(identitySchema),
    defaultValues: {
      name: '',
      privacy: 'private',
    },
  })

  const handleRedirect = (target?: string) => {
    const fallback = import.meta.env.VITE_DEFAULT_APP_URL || '/'
    const destination = target && target.length > 0 ? target : fallback
    window.location.href = destination
  }

  const onSubmit = async (values: IdentityFormValues) => {
    setIsSubmitting(true)
    try {
      await submitIdentity(values)
      toast.success('Identity saved', {
        description: 'Redirecting you to the dashboard…',
      })
      handleRedirect(redirectTo)
    } catch (error) {
      toast.error('Failed to save identity', {
        description:
          error instanceof Error ? error.message : 'Please try again.',
      })
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <Form {...form}>
      <form className="mt-6 space-y-6" onSubmit={form.handleSubmit(onSubmit)}>
        <FormField
          control={form.control}
          name="name"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Display name</FormLabel>
              <FormControl>
                <Input
                  placeholder="Jane Doe"
                  autoComplete="off"
                  {...field}
                />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />

        <FormField
          control={form.control}
          name="privacy"
          render={({ field }) => (
            <FormItem className="space-y-3">
              <FormLabel>Privacy</FormLabel>
              <FormControl>
                <RadioGroup
                  onValueChange={field.onChange}
                  value={field.value}
                  className="grid gap-4 sm:grid-cols-2"
                >
                  <FormItem className="space-y-1 rounded-md border p-3">
                    <FormControl>
                      <RadioGroupItem value="public" />
                    </FormControl>
                    <FormLabel className="text-sm font-medium">
                      Public
                    </FormLabel>
                    <p className="text-sm text-muted-foreground">
                      Other users can find your profile.
                    </p>
                  </FormItem>

                  <FormItem className="space-y-1 rounded-md border p-3">
                    <FormControl>
                      <RadioGroupItem value="private" />
                    </FormControl>
                    <FormLabel className="text-sm font-medium">
                      Private
                    </FormLabel>
                    <p className="text-sm text-muted-foreground">
                      Stay hidden from directory searches.
                    </p>
                  </FormItem>
                </RadioGroup>
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />

        <Button className="w-full" disabled={isSubmitting}>
          {isSubmitting ? 'Saving…' : 'Continue'}
        </Button>
      </form>
    </Form>
  )
}

