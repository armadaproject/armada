import type { NextConfig } from 'next';
import { createMDX } from 'fumadocs-mdx/next';

const nextConfig: NextConfig = {
  images: {
    // allow images from certain remote hosts
    remotePatterns: [
      'img.youtube.com',
      'github.githubassets.com',
      'user-images.githubusercontent.com',
    ].map((hostname) => ({
      hostname: hostname,
    })),
  },
  output: 'export', // Create a static export of the site in `out/`
  trailingSlash: true, // Change links `/me` -> `/me/`
};

const withMDX = createMDX();
export default withMDX(nextConfig);
