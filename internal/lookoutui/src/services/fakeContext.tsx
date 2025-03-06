import { ReactNode } from "react"

import { ServicesProvider } from "./context"
import { ICordonService } from "./lookout/CordonService"
import { IGetJobInfoService } from "./lookout/GetJobInfoService"
import { IGetJobsService } from "./lookout/GetJobsService"
import { IGetRunInfoService } from "./lookout/GetRunInfoService"
import { IGroupJobsService } from "./lookout/GroupJobsService"
import { ILogService } from "./lookout/LogService"
import { UpdateJobSetsService } from "./lookout/UpdateJobSetsService"
import { UpdateJobsService } from "./lookout/UpdateJobsService"
import { Job } from "../models/lookoutModels"
import { SubmitApi } from "../openapi/armada"
import { FakeCordonService } from "./lookout/mocks/FakeCordonService"
import FakeGetJobInfoService from "./lookout/mocks/FakeGetJobInfoService"
import FakeGetJobsService from "./lookout/mocks/FakeGetJobsService"
import { FakeGetRunInfoService } from "./lookout/mocks/FakeGetRunInfoService"
import FakeGroupJobsService from "./lookout/mocks/FakeGroupJobsService"
import { FakeLogService } from "./lookout/mocks/FakeLogService"

export interface FakeServicesProviderProps {
  children: ReactNode
  fakeJobs: Job[]
  simulateApiWait?: boolean
  v2GetJobsService?: IGetJobsService
  v2GroupJobsService?: IGroupJobsService
  v2RunInfoService?: IGetRunInfoService
  v2JobSpecService?: IGetJobInfoService
  v2LogService?: ILogService
  v2UpdateJobsService?: UpdateJobsService
  v2UpdateJobSetsService?: UpdateJobSetsService
  v2CordonService?: ICordonService
}

export const FakeServicesProvider = ({
  children,
  fakeJobs,
  simulateApiWait = true,
  v2GetJobsService = new FakeGetJobsService(fakeJobs, simulateApiWait),
  v2GroupJobsService = new FakeGroupJobsService(fakeJobs, simulateApiWait),
  v2RunInfoService = new FakeGetRunInfoService(simulateApiWait),
  v2JobSpecService = new FakeGetJobInfoService(simulateApiWait),
  v2LogService = new FakeLogService(simulateApiWait),
  v2UpdateJobsService = new UpdateJobsService(new SubmitApi()),
  v2UpdateJobSetsService = new UpdateJobSetsService(new SubmitApi()),
  v2CordonService = new FakeCordonService(simulateApiWait),
}: FakeServicesProviderProps) => (
  <ServicesProvider
    services={{
      v2GetJobsService,
      v2GroupJobsService,
      v2RunInfoService,
      v2JobSpecService,
      v2LogService,
      v2UpdateJobsService,
      v2UpdateJobSetsService,
      v2CordonService,
    }}
  >
    {children}
  </ServicesProvider>
)
