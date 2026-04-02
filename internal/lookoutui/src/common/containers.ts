/**
 * Extracts container names from a job spec's podSpec or podSpecs.
 * Returns an empty array if the spec is undefined or has no containers.
 */
export function getContainers(jobSpec: Record<string, any> | undefined): string[] {
  if (jobSpec === undefined) {
    return []
  }
  let podSpec: Record<string, any> = {}
  if (jobSpec.podSpec !== undefined) {
    podSpec = jobSpec.podSpec
  }
  if (jobSpec.podSpecs !== undefined && Array.isArray(jobSpec.podSpecs) && jobSpec.podSpecs.length > 0) {
    podSpec = jobSpec.podSpecs[0]
  }

  const containers: string[] = []
  containers.push(...readContainerNames(podSpec.containers))
  containers.push(...readContainerNames(podSpec.initContainers))
  return containers
}

function readContainerNames(containers: any): string[] {
  if (containers === undefined || !Array.isArray(containers)) {
    return []
  }
  const containerNames: string[] = []
  for (const container of containers) {
    if (container.name !== undefined) {
      containerNames.push(container.name)
    }
  }
  return containerNames
}
