import { z } from 'zod';

function isValidGitHubRepositoryUrl(url?: string): boolean {
  try {
    if (url) {
      const { hostname, pathname } = new URL(url);
      if (hostname === 'github.com') {
        const parts = pathname.split('/').filter(Boolean);
        if (parts.length === 2) return true;
      }
    }
  } catch {}
  return false;
}

function extractOwnerAndRepo(url: string) {
  if (isValidGitHubRepositoryUrl(url)) {
    const { pathname } = new URL(url);
    const parts = pathname.split('/').filter(Boolean);
    return {
      repositoryOwner: parts[0],
      repositoryName: parts[1],
    };
  } else {
    return {
      repositoryOwner: undefined,
      repositoryName: undefined,
    };
  }
}

const envSchema = z.object({
  basePath: z
    .string()
    .default(``)
    .refine(
      (value) => !value || (value.startsWith(`/`) && !value.endsWith(`/`))
    ),
  repositoryUrl: z
    .string()
    .url()
    .refine(
      (value) => isValidGitHubRepositoryUrl(value) && !value.endsWith(`/`),
      `Invalid GitHub repository URL, it should be in the format 'https://github.com/OWNER/REPO'`
    )
    .optional(),
  repositoryOwner: z
    .string()
    .transform((value) => extractOwnerAndRepo(value).repositoryOwner)
    .optional(),
  repositoryName: z
    .string()
    .transform((value) => extractOwnerAndRepo(value).repositoryName)
    .optional(),
});

export default envSchema.parse({
  basePath: process.env.NEXT_PUBLIC_BASE_PATH,
  repositoryUrl: process.env.NEXT_PUBLIC_REPOSITORY_URL,
  repositoryOwner: process.env.NEXT_PUBLIC_REPOSITORY_URL,
  repositoryName: process.env.NEXT_PUBLIC_REPOSITORY_URL,
});
