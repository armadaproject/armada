import { load } from "js-yaml"

// A simple interface that our PodSpec has.
export interface JobYaml {
  podSpec: {
    containers: Container[]
  }
}
interface Container {
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

export function convertStringToYaml(jobYaml: string): JobYaml {
  return load(jobYaml) as JobYaml
}

function checkContainerFieldExists(yamlified: JobYaml): boolean {
  return !yamlified.podSpec.containers
}

export function getCommandFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    if (val.command) {
      stringArray.push(val.command.join(" "))
    } else {
      stringArray.push("")
    }
  })
  return stringArray
}
export function getCommandArgumentsFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    if (val.args) {
      stringArray.push(val.args.join(" "))
    } else {
      stringArray.push("")
    }
  })
  return stringArray
}

export function getCpuFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    stringArray.push(val.resources?.limits?.cpu ?? "")
  })
  return stringArray
}
export function getMemoryFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    stringArray.push(val.resources?.limits?.memory ?? "")
  })
  return stringArray
}
export function getGpuFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    const gpuValue = val.resources.limits["nvidia.com/gpu"]
    gpuValue ? stringArray.push(gpuValue) : stringArray.push("")
  })
  return stringArray
}
export function getStorageFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    stringArray.push(val.resources?.limits["ephemeral-storage"] ?? "")
  })
  return stringArray
}
