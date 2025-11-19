import type { ReactNode } from 'react';
import type { Metadata } from 'next';
import { Geist } from 'next/font/google';
import { RootProvider } from 'fumadocs-ui/provider';
import 'katex/dist/katex.min.css';
import { cn } from '@/utils/cn';
import env from '@/utils/env';
import './globals.css';

const geistSans = Geist({
  subsets: ['latin'],
});

export default function RootLayout({ children }: { children: ReactNode }) {
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
              api: `${env.basePath}/api/search.json`,
            },
          }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}

export const metadata: Metadata = {
  title: {
    template: `%s | Armada Project`,
    default: `Armada Project`,
  },
  description: `multi-kubernetes-cluster batch job meta-scheduler. It helps organizations distribute millions of batch jobs per day across many nodes across many clusters.`,
};
