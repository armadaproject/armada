const remarkConfig = {
  plugins: [
    'remark-mdx',
    'remark-mdx-frontmatter',
    [
      'remark-validate-links',
      {
        // Internal links use absolute Next.js routes (e.g. /docs/clients),
        // not file paths. remark-validate-links resolves paths from the git
        // root and has no extension resolution, so it can't check these.
        // Skip route-shaped links; relative links and heading anchors are
        // still validated.
        // TODO: add a post-build link check against the static export to
        // properly validate routes (see check-links.sh).
        skipPathPatterns: [/\/[^/.]+\/?(#[^/]*)?$/],
      },
    ],
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
