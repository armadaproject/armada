import { loader } from 'fumadocs-core/source';
// .source folder will be generated when you run `next dev`
import { docs } from '@/.source';

// See https://fumadocs.vercel.app/docs/headless/source-api for more info
export const source = loader({
  baseUrl: '/',
  source: docs.toFumadocsSource(),
});
