import { remarkMermaid } from '@theguild/remark-mermaid';
import {
  defineConfig,
  defineDocs,
  frontmatterSchema,
  metaSchema,
} from 'fumadocs-mdx/config';
import rehypeKatex from 'rehype-katex';
import remarkEmoji from 'remark-emoji';
import remarkMath from 'remark-math';
import remarkGitHubAlert from '@/utils/mdx/github-alert-remark-plugin';

export const docs = defineDocs({
  dir: 'content',
  // You can customise Zod schemas for frontmatter and `meta.json` here
  // see https://fumadocs.vercel.app/docs/mdx/collections#define-docs
  docs: {
    schema: frontmatterSchema,
  },
  meta: {
    schema: metaSchema,
  },
});

export default defineConfig({
  mdxOptions: {
    remarkPlugins: [
      remarkMermaid, // add Mermaid support
      remarkEmoji, // add Emoji support
      remarkMath, // add Math support
      [remarkGitHubAlert, { overrideDefaultStyle: true }], // add GitHub alert support
    ],
    rehypePlugins: (v) => [rehypeKatex, ...v], // placing rehypeKatex first because it should be executed before the syntax highlighter
  },
});
