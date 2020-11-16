import { LookoutApi, LookoutJobInfo } from '../openapi'

export class JobService {

  api: LookoutApi;

  constructor(lookoutAPi: LookoutApi) {
    this.api = lookoutAPi
  }

  getOverview() {
    return this.api.overview()
  }

  async getJobsInQueue(queue: string, take: number, skip: number = 0): Promise<LookoutJobInfo[]> {
    const response = await this.api.getJobsInQueue({ body: { queue: queue } });
    return response.jobInfos || []
  }
}
