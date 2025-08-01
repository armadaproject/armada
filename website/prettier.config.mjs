/**
 * @see https://prettier.io/docs/en/configuration.html
 * @type {import('prettier').Config}
 */
const config = {
  trailingComma: 'es5',
  tabWidth: 2,
  semi: true,
  singleQuote: true,
  jsxSingleQuote: true,
  importOrder: [
    '^react$',
    '^next(.*)$',
    '^lucide-react$',
    '<THIRD_PARTY_MODULES>',
    '^@/app/(.*)$',
    '^@/components/(.*)$',
    '^@/hooks/(.*)$',
    '^@/lib/(.*)$',
    '^@/(.*)$',
    '^[./]',
  ],
  plugins: [
    // by default: prettier format JS, TS, YAML, JSON, and Markdown
    'prettier-plugin-tailwindcss',
    '@trivago/prettier-plugin-sort-imports',
  ],
};

export default config;
