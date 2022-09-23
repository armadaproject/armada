import {
  convertStringToYaml,
  getCommandFromJobYaml,
  getCommandArgumentsFromJobYaml,
  getCpuFromJobYaml,
  getMemoryFromJobYaml,
  getGpuFromJobYaml,
  getDiskFromJobYaml,
} from "./utils"

describe("JobYamlToResourceConvertor", () => {
  const yaml = `id: 01gdnzwhvgxa2y7xzykdddmgc2
owner: anonymous
queue: test
created: '2022-09-23T20:16:10.096556Z'
podSpec:
  containers:
  - args:
    - sleep $(( (RANDOM % 30) + 30 ))
    name: sleep
    image: alpine:latest
    command:
    - sh
    - '-c'
    resources:
      limits:
        cpu: 150m
        memory: 64Mi
      requests:
        cpu: 150m
        memory: 64Mi
imagePullPolicy: IfNotPresent
restartPolicy: Never
terminationGracePeriodSeconds: 0
clientId: 01gdnzwhvb0pt7rmnp509qmqrj
jobSetId: job-set-1
namespace: default
compressedQueueOwnershipUserGroups: eAEAGwDk/wz/gQIBAv+CAAEMAAAN/4IAAQhldmVyeW9uZQEAAP//a8EIJA==`
  test("Happy Path", () => {
    const yamlifed = convertStringToYaml(yaml)
    expect(convertStringToYaml(yaml)).toBeTruthy()
    expect(getCommandFromJobYaml(yamlifed)).toBe("sh,-c")
    expect(getCommandArgumentsFromJobYaml(yamlifed)).toBe("sleep $(( (RANDOM % 30) + 30 ))")
    expect(getCpuFromJobYaml(yamlifed)).toBe("150m")
    expect(getMemoryFromJobYaml(yamlifed)).toBe("64Mi")
    expect(getGpuFromJobYaml(yamlifed)).toBeFalsy()
    expect(getDiskFromJobYaml(yamlifed)).toBeFalsy()
  })
})
