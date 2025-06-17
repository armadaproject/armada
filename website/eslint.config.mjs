import { FlatCompat } from '@eslint/eslintrc';
import gitignore from 'eslint-config-flat-gitignore';

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
    extends: ['next/core-web-vitals', 'next/typescript', 'prettier'],
    rules: {
      'sort-imports': 'off',
    },
  }),
];

export default eslintConfig;
