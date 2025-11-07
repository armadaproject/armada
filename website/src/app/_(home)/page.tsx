import Link from 'next/link';
import env from '@/utils/env';

export default function HomePage() {
  return (
    <main className='flex flex-1 flex-col justify-center text-center'>
      <h1 className='mb-4 text-2xl font-bold'>Welcome to Armada!</h1>
      <p className='text-fd-muted-foreground'>
        This is a landing page for the Armada documentation. You can open{' '}
        {env.repositoryUrl ? (
          <Link
            href={env.repositoryUrl}
            className='text-fd-foreground font-semibold underline'
          >
            GitHub repository
          </Link>
        ) : null}{' '}
        and see the documentation.
      </p>
    </main>
  );
}
