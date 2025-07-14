import { simulateApiWait } from "../../../utils/fakeJobsUtils"
import { IGetJobInfoService } from "../GetJobInfoService"

export default class FakeGetJobInfoService implements IGetJobInfoService {
  constructor(private simulateApiWait = true) {}

  async getJobSpec(_: GlobalFetch["fetch"], jobId: string, signal?: AbortSignal): Promise<Record<string, any>> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }
    return JSON.parse(
      `{"id":"${jobId}","clientId":"01gvgjbr0jrzvschp2f8jhk6n5","jobSetId":"alices-project-0","queue":"alice","namespace":"default","owner":"anonymous","podSpec":{"containers":[{"name":"cpu-burner","image":"containerstack/alpine-stress:latest","command":["sh"],"args":["-c","echo FAILED && echo hello world > /dev/termination-log && exit 137"],"resources":{"limits":{"cpu":"200m","ephemeral-storage":"8Gi","memory":"128Mi","nvidia.com/gpu":"8"},"requests":{"cpu":"200m","ephemeral-storage":"8Gi","memory":"128Mi","nvidia.com/gpu":"8"}},"imagePullPolicy":"IfNotPresent"}],"restartPolicy":"Never","terminationGracePeriodSeconds":1,"tolerations":[{"key":"armadaproject.io/armada","operator":"Equal","value":"true","effect":"NoSchedule"},{"key":"armadaproject.io/pc-armada-default","operator":"Equal","value":"true","effect":"NoSchedule"}],"priorityClassName":"armada-default"},"created":"2023-03-14T17:23:21.29874Z"}`,
    )
  }
}
