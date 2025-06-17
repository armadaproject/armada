import { notFound } from 'next/navigation';
import { createRelativeLink } from 'fumadocs-ui/mdx';
import {
  DocsPage,
  DocsBody,
  DocsDescription,
  DocsTitle,
  type DocsPageProps,
} from 'fumadocs-ui/page';
import { source } from '@/loaders/source';
import env from '@/utils/env';
import { getMDXComponents } from '@/utils/mdx/mdx-components';

export default async function Page(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const MDXContent = page.data.body;

  const editOnGithub: DocsPageProps['editOnGithub'] | undefined =
    env.repositoryOwner && env.repositoryName
      ? {
          owner: env.repositoryOwner,
          repo: env.repositoryName,
          sha: 'master',
          path: `website/content/${page.path}`,
        }
      : undefined;

  return (
    <DocsPage
      toc={page.data.toc}
      full={page.data.full}
      editOnGithub={editOnGithub}
    >
      <DocsTitle>{page.data.title}</DocsTitle>
      <DocsDescription>{page.data.description}</DocsDescription>
      <DocsBody>
        <MDXContent
          components={getMDXComponents({
            // this allows you to link to other pages with relative file paths
            a: createRelativeLink(source, page),
          })}
        />
      </DocsBody>
    </DocsPage>
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  return {
    title: page.data.title,
    description: page.data.description,
  };
}
