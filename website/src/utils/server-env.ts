import { z } from 'zod';

const serverEnvSchema = z.object({
  GITHUB_TOKEN: z.string().optional(),
});

export default serverEnvSchema.parse(process.env);
