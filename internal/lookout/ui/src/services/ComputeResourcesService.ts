import { load } from "js-yaml"

// A simple interface that our PodSpec has.
export interface JobYaml {
  podSpec: {
    containers: Container[]
  }
}

export interface ContainerInfo {
  command?: string[]
  arguments?: string[]
  cpu?: string[]
  memory?: string[]
  gpu?: string[]
  "ephermal-storage"?: string[]
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

export function getContainerInfoFromYaml(jobYaml: string): ContainerInfo {
  const yaml = load(jobYaml) as JobYaml
  return {
    command: getCommandFromJobYaml(yaml),
    arguments: getCommandArgumentsFromJobYaml(yaml),
    cpu: getCpuFromJobYaml(yaml),
    memory: getMemoryFromJobYaml(yaml),
    gpu: getGpuFromJobYaml(yaml),
    "ephermal-storage": getStorageFromJobYaml(yaml),
  }
}

function checkContainerFieldExists(yamlified: JobYaml): boolean {
  return !yamlified.podSpec.containers
}

function getCommandFromJobYaml(yamlified: JobYaml): string[] | undefined {
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

function getCommandArgumentsFromJobYaml(yamlified: JobYaml): string[] | undefined {
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

function getCpuFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    stringArray.push(val.resources?.limits?.cpu ?? "")
  })
  return stringArray
}

function getMemoryFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    stringArray.push(val.resources?.limits?.memory ?? "")
  })
  return stringArray
}

function getGpuFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    const gpuValue = val.resources.limits["nvidia.com/gpu"]
    gpuValue ? stringArray.push(gpuValue) : stringArray.push("")
  })
  return stringArray
}

function getStorageFromJobYaml(yamlified: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlified)) return
  const stringArray: string[] = []
  yamlified.podSpec.containers.map((val) => {
    stringArray.push(val.resources?.limits["ephemeral-storage"] ?? "")
  })
  return stringArray
}
