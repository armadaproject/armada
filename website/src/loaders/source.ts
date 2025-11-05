import { createElement } from 'react';
import { icons } from 'lucide-react';
import { loader } from 'fumadocs-core/source';
// .source folder will be generated when you run `next dev`
import { docs } from '@/.source';

// See https://fumadocs.vercel.app/docs/headless/source-api for more info
export const source = loader({
  baseUrl: '/',
  source: docs.toFumadocsSource(),

  icon: (iconKey) => {
    // Load the icon property specified by pages and meta files.
    if (iconKey && iconKey in icons)
      return createElement(icons[iconKey as keyof typeof icons]);
    return; // You may set a default icon
  },
});
