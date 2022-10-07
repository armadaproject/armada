import { load } from "js-yaml"

// A simple interface that our PodSpec has.
export interface JobYaml {
  podSpec: {
    containers: Container[]
  }
}

export interface ContainerInfo {
  containerInfo: ContainerString[]
}

interface ContainerString {
  name: string
  command: string
  args: string
  resources: {
    limits: Limits
  }
}
interface Container {
  name: string
  command: string[]
  args: string[]
  resources: {
    limits: Limits
  }
}

interface Limits {
  cpu?: string
  "nvidia.com/gpu"?: string
  "ephemeral-storage"?: string
  memory?: string
}

export function getContainerInfoFromYaml(jobYaml: string): ContainerInfo | undefined {
  const yaml = load(jobYaml) as JobYaml
  return detailsFromPodSpec(yaml)
}

function checkContainerFieldExists(yamlified: JobYaml): boolean {
  return !yamlified.podSpec.containers
}

function detailsFromPodSpec(yamlified: JobYaml): ContainerInfo | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const mapVal = yamlified.podSpec.containers.map((val) => {
    const container: ContainerString = {
      name: val.name,
      command: val.command ? val.command.join(" ") : "",
      args: val.args ? val.args.join(" ") : "",
      resources: {
        limits: {
          cpu: val.resources?.limits?.cpu ?? "",
          memory: val.resources?.limits?.memory ?? "",
          "ephemeral-storage": val.resources.limits["ephemeral-storage"] ?? "",
          "nvidia.com/gpu": val.resources.limits["nvidia.com/gpu"] ?? "",
        },
      },
    }
    return container
  })
  return { containerInfo: mapVal }
}
