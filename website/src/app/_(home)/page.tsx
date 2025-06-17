import Link from 'next/link';

export default function HomePage() {
  return (
    <main className='flex flex-1 flex-col justify-center text-center'>
      <h1 className='mb-4 text-2xl font-bold'>Welcome to Armada!</h1>
      <p className='text-fd-muted-foreground'>
        This is a landing page for the Armada documentation. You can open{' '}
        <Link
          href='https://github.com/armadaproject/armada'
          className='text-fd-foreground font-semibold underline'
        >
          GitHub repository
        </Link>{' '}
        and see the documentation.
      </p>
    </main>
  );
}
