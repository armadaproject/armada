import { ReactNode } from "react"

import { ServicesProvider } from "./context"
import { ICordonService } from "./lookoutV2/CordonService"
import { IGetJobInfoService } from "./lookoutV2/GetJobInfoService"
import { IGetJobsService } from "./lookoutV2/GetJobsService"
import { IGetRunInfoService } from "./lookoutV2/GetRunInfoService"
import { IGroupJobsService } from "./lookoutV2/GroupJobsService"
import { ILogService } from "./lookoutV2/LogService"
import { UpdateJobSetsService } from "./lookoutV2/UpdateJobSetsService"
import { UpdateJobsService } from "./lookoutV2/UpdateJobsService"
import { FakeCordonService } from "./lookoutV2/mocks/FakeCordonService"
import FakeGetJobInfoService from "./lookoutV2/mocks/FakeGetJobInfoService"
import FakeGetJobsService from "./lookoutV2/mocks/FakeGetJobsService"
import { FakeGetRunInfoService } from "./lookoutV2/mocks/FakeGetRunInfoService"
import FakeGroupJobsService from "./lookoutV2/mocks/FakeGroupJobsService"
import { FakeLogService } from "./lookoutV2/mocks/FakeLogService"
import { Job } from "../models/lookoutV2Models"
import { SubmitApi } from "../openapi/armada"

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
