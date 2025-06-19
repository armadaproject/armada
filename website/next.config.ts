import type { NextConfig } from 'next';
import { createMDX } from 'fumadocs-mdx/next';
import env from '@/utils/env';

const nextConfig: NextConfig = {
  eslint: {
    dirs: [`src`, `content`],
  },
  images: {
    unoptimized: true, // because static export does not support image optimization
    // allow images from certain remote hosts
    remotePatterns: [
      'img.youtube.com',
      'github.com',
      'github.githubassets.com',
      '*.githubusercontent.com',
    ].map((hostname) => ({
      hostname: hostname,
    })),
  },
  basePath: env.basePath, // base path for GitHub Pages
  trailingSlash: !!env.basePath, // fix for issue related to GitHub Pages forcing redirect to `/` on index.html when basePath is set
  output: 'export', // Create a static export of the site in `out/`
};

const withMDX = createMDX();
export default withMDX(nextConfig);
