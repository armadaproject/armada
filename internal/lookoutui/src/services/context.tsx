import { createContext, ReactNode, useContext } from "react"

import { ICordonService } from "./lookout/CordonService"
import { IGetJobInfoService } from "./lookout/GetJobInfoService"
import { IGetJobsService } from "./lookout/GetJobsService"
import { IGetRunInfoService } from "./lookout/GetRunInfoService"
import { IGroupJobsService } from "./lookout/GroupJobsService"
import { ILogService } from "./lookout/LogService"
import { UpdateJobSetsService } from "./lookout/UpdateJobSetsService"
import { UpdateJobsService } from "./lookout/UpdateJobsService"

export interface Services {
  v2GetJobsService: IGetJobsService
  v2GroupJobsService: IGroupJobsService
  v2RunInfoService: IGetRunInfoService
  v2JobSpecService: IGetJobInfoService
  v2LogService: ILogService
  v2UpdateJobsService: UpdateJobsService
  v2UpdateJobSetsService: UpdateJobSetsService
  v2CordonService: ICordonService
}

interface ServicesContextValue {
  services: Services
}

const ServicesContext = createContext<ServicesContextValue | undefined>(undefined)

export interface ServicesProviderProps extends ServicesContextValue {
  children: ReactNode
}

export const ServicesProvider = ({ children, services }: ServicesProviderProps) => (
  <ServicesContext.Provider value={{ services }}>{children}</ServicesContext.Provider>
)

export const useServices = () => {
  const context = useContext(ServicesContext)
  if (context === undefined) {
    throw new Error("useServices() must be used within a ServicesProvider")
  }
  return context.services
}
