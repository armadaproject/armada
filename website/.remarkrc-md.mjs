const remarkConfig = {
  plugins: [
    'remark-preset-lint-consistent',
    'remark-frontmatter',
    'remark-validate-links',
    [
      'remark-lint-no-dead-urls',
      {
        skipOffline: true,
        skipLocalhost: true,
        skipUrlPatterns: [/^.*$/], // Disable all URL checks
      },
    ],
  ],
};

export default remarkConfig;
