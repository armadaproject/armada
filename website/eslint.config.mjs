import { FlatCompat } from '@eslint/eslintrc';
import markdown from '@eslint/markdown';
import gitignore from 'eslint-config-flat-gitignore';
import { defineConfig } from 'eslint/config';

const compat = new FlatCompat({
  baseDirectory: import.meta.dirname,
});

/**
 * @see https://eslint.org/docs/user-guide/configuring
 * @type {import('eslint').Linter.Config[]}
 */
const eslintConfig = [
  gitignore(),
  ...compat.config({
    extends: [
      'next/core-web-vitals',
      'next/typescript',
      'plugin:mdx/recommended',
      'prettier',
    ],
    rules: {
      'sort-imports': 'off',
    },
  }),
  // *.mdx files
  // Fumadocs: Images are automatically optimized for next/image.
  // See: https://fumadocs.dev/docs/ui/markdown#mdx
  {
    files: ['**/*.mdx'],
    rules: {
      '@next/next/no-img-element': 'off', // allow <img> elements in MDX files
    },
  },
  // *.md files
  ...defineConfig([
    {
      files: ['**/*.md'],
      plugins: { markdown },
      language: 'markdown/gfm',
      languageOptions: {
        frontmatter: 'yaml',
      },
      rules: {
        'react/display-name': 'off',
        'react/jsx-key': 'off',
        'react/jsx-uses-react': 'off',
        'react/react-in-jsx-scope': 'off',
        'react/no-deprecated': 'off',
        'react/no-direct-mutation-state': 'off',
        'react/require-render-return': 'off',
      },
    },
  ]),
];

export default eslintConfig;
