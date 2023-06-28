import { simulateApiWait } from "../../../utils/fakeJobsUtils"
import { ICordonService } from "../CordonService"

export class FakeCordonService implements ICordonService {
  constructor(private simulateApiWait = true) {}

  async cordonNode(cluster: string, node: string, signal: AbortSignal | undefined): Promise<void> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }
    console.log(`Cordoned node ${node} in cluster ${cluster}`)
  }
}
