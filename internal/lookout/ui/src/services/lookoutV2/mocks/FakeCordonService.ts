import { simulateApiWait } from "../../../utils/fakeJobsUtils"
import { ICordonService } from "../CordonService"

export class FakeCordonService implements ICordonService {
  constructor(private simulateApiWait = true) {}

  async cordonNode(cluster: string, node: string, accessToken?: string, signal?: AbortSignal): Promise<void> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }
    console.log(`Cordoned node ${node} in cluster ${cluster}`)
  }
}
