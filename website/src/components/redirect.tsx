'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'fumadocs-core/link';

interface RedirectProps {
  /**
   * The URL to redirect to (can be relative or absolute)
   */
  to: string;
  /**
   * Custom message to display (optional)
   */
  message?: string;
  /**
   * Delay in seconds before redirect (default: 3)
   */
  delay?: number;
}

export function Redirect({ to, message, delay = 3 }: RedirectProps) {
  const linkRef = useRef<HTMLAnchorElement>(null);
  const [countdown, setCountdown] = useState(delay);

  useEffect(() => {
    const timer = setTimeout(() => {
      linkRef.current?.click();
    }, delay * 1000);

    return () => clearTimeout(timer);
  }, [delay]);

  useEffect(() => {
    const interval = setInterval(() => {
      setCountdown((prev) => (prev > 0 ? prev - 1 : 0));
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className='flex flex-col items-center justify-center min-h-[60vh] px-4'>
      <div className='max-w-md w-full text-center space-y-4'>
        <h1 className='text-2xl font-semibold'>Redirecting</h1>
        <p className='text-muted-foreground'>
          {message || 'This page has moved. You will be redirected shortly.'}
        </p>
        <p className='text-sm text-muted-foreground'>
          {countdown} second{countdown !== 1 ? 's' : ''}
        </p>
        <div className='animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto'></div>
        <div className='pt-4'>
          <Link
            ref={linkRef}
            href={to}
            className='text-primary hover:underline font-medium'
          >
            Click here if not redirected automatically
          </Link>
        </div>
      </div>
    </div>
  );
}
