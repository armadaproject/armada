import { ReactNode } from "react"

import { Job } from "../models/lookoutModels"
import { SubmitApi } from "../openapi/armada"

import { ServicesProvider } from "./context"
import { IGetJobInfoService } from "./lookout/GetJobInfoService"
import { IGroupJobsService } from "./lookout/GroupJobsService"
import { UpdateJobSetsService } from "./lookout/UpdateJobSetsService"
import FakeGetJobInfoService from "./lookout/mocks/FakeGetJobInfoService"
import FakeGroupJobsService from "./lookout/mocks/FakeGroupJobsService"

export interface FakeServicesProviderProps {
  children: ReactNode
  fakeJobs: Job[]
  simulateApiWait?: boolean
  v2GroupJobsService?: IGroupJobsService
  v2JobSpecService?: IGetJobInfoService
  v2UpdateJobSetsService?: UpdateJobSetsService
}

export const FakeServicesProvider = ({
  children,
  fakeJobs,
  simulateApiWait = true,
  v2GroupJobsService = new FakeGroupJobsService(fakeJobs, simulateApiWait),
  v2JobSpecService = new FakeGetJobInfoService(simulateApiWait),
  v2UpdateJobSetsService = new UpdateJobSetsService(new SubmitApi()),
}: FakeServicesProviderProps) => (
  <ServicesProvider
    services={{
      v2GroupJobsService,
      v2JobSpecService,
      v2UpdateJobSetsService,
    }}
  >
    {children}
  </ServicesProvider>
)
