import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { ArmadaIcon, ArmadaText } from '@/components/logo';

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
      <div className='flex gap-3 justify-center items-center ms-3'>
        <ArmadaIcon className='w-10 sm:w-15' />
        <ArmadaText className='text-[#00aae1] w-25 sm:w-30' />
      </div>
    ),
    transparentMode: 'top',
  },
};
