import { defineConfig } from 'cspell';

export default defineConfig({
  version: '0.2',
  language: 'en',
  useGitignore: true,
  showSuggestions: true,
  showContext: true,
  files: [
    '{src,content}/**/*.{ts,tsx,js,jsx,mjs,md,mdx,json,yaml}',
    'README.md',
  ],
  dictionaryDefinitions: [
    {
      name: 'armada',
      path: './dict/armada.txt',
      description: 'Armada dictionary.',
    },
  ],
  dictionaries: ['armada'],
});
