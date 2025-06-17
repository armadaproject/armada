import { createFromSource } from 'fumadocs-core/search/server';
import { source } from '@/loaders/source';

export const revalidate = false; // it should be cached forever
export const { staticGET: GET } = createFromSource(source);
