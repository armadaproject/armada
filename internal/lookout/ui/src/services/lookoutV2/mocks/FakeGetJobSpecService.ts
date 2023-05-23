import { simulateApiWait } from "../../../utils/fakeJobsUtils"
import { IGetJobSpecService } from "../GetJobSpecService"

export default class FakeGetJobSpecService implements IGetJobSpecService {
  constructor(private simulateApiWait = true) {}

  async getJobSpec(jobId: string, signal: AbortSignal | undefined): Promise<Record<string, any>> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }
    return JSON.parse(
      '{"id":"01gvgjbr1a8nvh5saz51j2nf8b","clientId":"01gvgjbr0jrzvschp2f8jhk6n5","jobSetId":"alices-project-0","queue":"alice","namespace":"default","owner":"anonymous","podSpec":{"containers":[{"name":"cpu-burner","image":"containerstack/alpine-stress:latest","command":["sh"],"args":["-c","echo FAILED && echo hello world > /dev/termination-log && exit 137"],"resources":{"limits":{"cpu":"200m","ephemeral-storage":"8Gi","memory":"128Mi","nvidia.com/gpu":"8"},"requests":{"cpu":"200m","ephemeral-storage":"8Gi","memory":"128Mi","nvidia.com/gpu":"8"}},"imagePullPolicy":"IfNotPresent"}],"restartPolicy":"Never","terminationGracePeriodSeconds":1,"tolerations":[{"key":"armadaproject.io/armada","operator":"Equal","value":"true","effect":"NoSchedule"},{"key":"armadaproject.io/pc-armada-default","operator":"Equal","value":"true","effect":"NoSchedule"}],"priorityClassName":"armada-default"},"created":"2023-03-14T17:23:21.29874Z"}',
    )
  }
}
