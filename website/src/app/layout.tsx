import type { ReactNode } from 'react';
import { Geist } from 'next/font/google';
import { RootProvider } from 'fumadocs-ui/provider';
import 'katex/dist/katex.css';
import { cn } from '@/utils/cn';
import './globals.css';

const geistSans = Geist({
  subsets: ['latin'],
});

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang='en' suppressHydrationWarning>
      <body
        className={cn(
          'flex flex-col min-h-screen',
          `${geistSans.className} antialiased`
        )}
      >
        <RootProvider
          search={{
            options: {
              type: 'static',
            },
          }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
