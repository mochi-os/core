'use client'

import * as React from 'react'
import { useMediaQuery } from '@/hooks/use-media-query'
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import {
  Drawer,
  DrawerClose,
  DrawerContent,
  DrawerDescription,
  DrawerFooter,
  DrawerHeader,
  DrawerTitle,
  DrawerTrigger,
} from '@/components/ui/drawer'

const desktop = '(min-width: 768px)'

interface BaseProps {
  children?: React.ReactNode
}

interface RootResponsiveDialogProps extends BaseProps {
  open?: boolean
  onOpenChange?: (open: boolean) => void
}

function ResponsiveDialog({ children, ...props }: RootResponsiveDialogProps) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogRoot = isDesktop ? Dialog : Drawer

  return <ResponsiveDialogRoot {...props}>{children}</ResponsiveDialogRoot>
}

function ResponsiveDialogTrigger({ className, children, ...props }: BaseProps & { className?: string; asChild?: boolean }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogTriggerComponent = isDesktop ? DialogTrigger : DrawerTrigger

  return (
    <ResponsiveDialogTriggerComponent className={className} {...props}>
      {children}
    </ResponsiveDialogTriggerComponent>
  )
}

function ResponsiveDialogClose({ className, children, ...props }: BaseProps & { className?: string; asChild?: boolean }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogCloseComponent = isDesktop ? DialogClose : DrawerClose

  return (
    <ResponsiveDialogCloseComponent className={className} {...props}>
      {children}
    </ResponsiveDialogCloseComponent>
  )
}

function ResponsiveDialogContent({ className, children, ...props }: BaseProps & { className?: string }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogContentComponent = isDesktop ? DialogContent : DrawerContent

  return (
    <ResponsiveDialogContentComponent className={className} {...props}>
      {children}
    </ResponsiveDialogContentComponent>
  )
}

function ResponsiveDialogDescription({ className, children, ...props }: BaseProps & { className?: string; asChild?: boolean }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogDescriptionComponent = isDesktop ? DialogDescription : DrawerDescription

  return (
    <ResponsiveDialogDescriptionComponent className={className} {...props}>
      {children}
    </ResponsiveDialogDescriptionComponent>
  )
}

function ResponsiveDialogHeader({ className, children, ...props }: BaseProps & { className?: string }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogHeaderComponent = isDesktop ? DialogHeader : DrawerHeader

  return (
    <ResponsiveDialogHeaderComponent className={className} {...props}>
      {children}
    </ResponsiveDialogHeaderComponent>
  )
}

function ResponsiveDialogTitle({ className, children, ...props }: BaseProps & { className?: string }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogTitleComponent = isDesktop ? DialogTitle : DrawerTitle

  return (
    <ResponsiveDialogTitleComponent className={className} {...props}>
      {children}
    </ResponsiveDialogTitleComponent>
  )
}

function ResponsiveDialogFooter({ className, children, ...props }: BaseProps & { className?: string }) {
  const isDesktop = useMediaQuery(desktop)
  const ResponsiveDialogFooterComponent = isDesktop ? DialogFooter : DrawerFooter

  return (
    <ResponsiveDialogFooterComponent className={className} {...props}>
      {children}
    </ResponsiveDialogFooterComponent>
  )
}

export {
  ResponsiveDialog,
  ResponsiveDialogTrigger,
  ResponsiveDialogClose,
  ResponsiveDialogContent,
  ResponsiveDialogDescription,
  ResponsiveDialogHeader,
  ResponsiveDialogTitle,
  ResponsiveDialogFooter,
}

