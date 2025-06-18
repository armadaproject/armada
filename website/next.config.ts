import type { NextConfig } from 'next';
import { createMDX } from 'fumadocs-mdx/next';
import env from '@/utils/env';

const nextConfig: NextConfig = {
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
  output: 'export', // Create a static export of the site in `out/`
  trailingSlash: true, // Optional: Change links `/me` -> `/me/` and emit `/me.html` -> `/me/index.html`
  skipTrailingSlashRedirect: false, // Optional: Prevent automatic `/me` -> `/me/`, instead preserve `href`
};

const withMDX = createMDX();
export default withMDX(nextConfig);
