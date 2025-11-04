import * as React from 'react'

/**
 * Custom hook for media query detection
 * @param query - Media query string (e.g., "(min-width: 768px)")
 * @returns boolean indicating if the media query matches
 * @example const isDesktop = useMediaQuery("(min-width: 768px)")
 */
export function useMediaQuery(query: string) {
  const [value, setValue] = React.useState<boolean | undefined>(undefined)

  React.useEffect(() => {
    function onChange(event: MediaQueryListEvent) {
      setValue(event.matches)
    }

    const result = window.matchMedia(query)
    result.addEventListener('change', onChange)
    setValue(result.matches)

    return () => result.removeEventListener('change', onChange)
  }, [query])

  return !!value
}
