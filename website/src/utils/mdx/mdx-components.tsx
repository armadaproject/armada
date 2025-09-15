import defaultMdxComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
import { Blockquote } from '@/components/mdx/blockquote';
import { Mermaid } from '@/components/mdx/mermaid';

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    Mermaid, // add support for Mermaid diagrams
    Alert: Blockquote, // add support for GitHub alerts
    ...components,
  };
}
