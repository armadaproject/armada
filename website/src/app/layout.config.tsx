import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { ArmadaIcon } from '@/components/logo';

/**
 * Shared layout configurations
 *
 * you can customize layouts individually from:
 * Home Layout: app/(home)/layout.tsx
 * Docs Layout: app/docs/layout.tsx
 */
export const baseOptions: BaseLayoutProps = {
  nav: {
    title: (
      <>
        <ArmadaIcon width='52' />
        <span className='text-[#00aae1] dark:text-white text-xl tracking-widest'>
          ARMADA
        </span>
      </>
    ),
  },
  // see https://fumadocs.dev/docs/ui/navigation/links
  links: [],
};
