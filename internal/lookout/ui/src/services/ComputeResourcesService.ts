import { load } from "js-yaml"

// A simple interface that our PodSpec has.
interface JobYaml {
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
  storage?: string
  memory?: string
}

export function convertStringToYaml(jobYaml: string): JobYaml {
  return load(jobYaml) as JobYaml
}

function checkContainerFieldExists(yamlifed: JobYaml): boolean {
  return !yamlifed.podSpec.containers
}

export function getCommandFromJobYaml(yamlifed: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlifed)) return
  const stringArray: string[] = []
  yamlifed.podSpec.containers.map((val) => {
    if (val.command) {
      stringArray.push(val.command.join(" "))
    } else {
      stringArray.push("")
    }
  })
  return stringArray
}
export function getCommandArgumentsFromJobYaml(yamlifed: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlifed)) return
  const stringArray: string[] = []
  yamlifed.podSpec.containers.map((val) => {
    if (val.args) {
      stringArray.push(val.args.join(" "))
    } else {
      stringArray.push("")
    }
  })
  return stringArray
}

export function getCpuFromJobYaml(yamlifed: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlifed)) return
  const stringArray: string[] = []
  yamlifed.podSpec.containers.map((val) => {
    val.resources?.limits?.cpu ? stringArray.push(val.resources?.limits?.cpu) : stringArray.push("")
  })
  return stringArray
}
export function getMemoryFromJobYaml(yamlifed: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlifed)) return
  const stringArray: string[] = []
  yamlifed.podSpec.containers.map((val) => {
    val.resources?.limits?.memory ? stringArray.push(val.resources?.limits?.memory) : stringArray.push("")
  })
  return stringArray
}
export function getGpuFromJobYaml(yamlifed: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlifed)) return
  const stringArray: string[] = []
  yamlifed.podSpec.containers.map((val) => {
    const gpuValue = val.resources.limits["nvidia.com/gpu"]
    gpuValue ? stringArray.push(gpuValue) : stringArray.push("")
  })
  return stringArray
}
export function getStorageFromJobYaml(yamlifed: JobYaml): string[] | undefined {
  if (checkContainerFieldExists(yamlifed)) return
  const stringArray: string[] = []
  yamlifed.podSpec.containers.map((val) => {
    val.resources?.limits?.storage ? stringArray.push(val.resources?.limits?.storage) : stringArray.push("")
  })
  return stringArray
}
