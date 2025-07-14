import { ReactNode } from "react"

import { ServicesProvider } from "./context"
import { IGetJobInfoService } from "./lookout/GetJobInfoService"
import { IGetJobsService } from "./lookout/GetJobsService"
import { IGroupJobsService } from "./lookout/GroupJobsService"
import { UpdateJobSetsService } from "./lookout/UpdateJobSetsService"
import { UpdateJobsService } from "./lookout/UpdateJobsService"
import { Job } from "../models/lookoutModels"
import { SubmitApi } from "../openapi/armada"
import FakeGetJobInfoService from "./lookout/mocks/FakeGetJobInfoService"
import FakeGetJobsService from "./lookout/mocks/FakeGetJobsService"
import FakeGroupJobsService from "./lookout/mocks/FakeGroupJobsService"

export interface FakeServicesProviderProps {
  children: ReactNode
  fakeJobs: Job[]
  simulateApiWait?: boolean
  v2GetJobsService?: IGetJobsService
  v2GroupJobsService?: IGroupJobsService
  v2JobSpecService?: IGetJobInfoService
  v2UpdateJobsService?: UpdateJobsService
  v2UpdateJobSetsService?: UpdateJobSetsService
}

export const FakeServicesProvider = ({
  children,
  fakeJobs,
  simulateApiWait = true,
  v2GetJobsService = new FakeGetJobsService(fakeJobs, simulateApiWait),
  v2GroupJobsService = new FakeGroupJobsService(fakeJobs, simulateApiWait),
  v2JobSpecService = new FakeGetJobInfoService(simulateApiWait),
  v2UpdateJobsService = new UpdateJobsService(new SubmitApi()),
  v2UpdateJobSetsService = new UpdateJobSetsService(new SubmitApi()),
}: FakeServicesProviderProps) => (
  <ServicesProvider
    services={{
      v2GetJobsService,
      v2GroupJobsService,
      v2JobSpecService,
      v2UpdateJobsService,
      v2UpdateJobSetsService,
    }}
  >
    {children}
  </ServicesProvider>
)
