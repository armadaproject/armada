import { getContainerInfoFromYaml } from "./ComputeResourcesService"

describe("JobYamlToResourceConvertor", () => {
  const yaml = `
id: 01gdnzwhvgxa2y7xzykdddmgc2
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
          nvidia.com/gpu: 1
          ephemeral-storage: 50Mi
        requests:
          cpu: 150m
          memory: 64Mi
          nvidia.com/gpu: 1
          ephemeral-storage: 50Mi
  imagePullPolicy: IfNotPresent
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  clientId: 01gdnzwhvb0pt7rmnp509qmqrj
  jobSetId: job-set-1
  namespace: default
  compressedQueueOwnershipUserGroups: eAEAGwDk/wz/gQIBAv+CAAEMAAAN/4IAAQhldmVyeW9uZQEAAP//a8EIJA==`
  test("Happy Path", () => {
    const info = getContainerInfoFromYaml(yaml)
    expect(info).toBeTruthy()
    if (info?.containerInfo[0]) {
      expect(info.containerInfo[0]).toEqual({
        name: "sleep",
        command: "sh -c",
        args: "sleep $(( (RANDOM % 30) + 30 ))",
        resources: {
          limits: {
            cpu: "150m",
            memory: "64Mi",
            "nvidia.com/gpu": 1,
            "ephemeral-storage": "50Mi",
          },
        },
      })
    }
  })
})
