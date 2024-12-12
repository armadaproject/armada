import { createContext, ReactNode, useContext } from "react"

import { ICordonService } from "./lookoutV2/CordonService"
import { IGetJobInfoService } from "./lookoutV2/GetJobInfoService"
import { IGetJobsService } from "./lookoutV2/GetJobsService"
import { IGetRunInfoService } from "./lookoutV2/GetRunInfoService"
import { IGroupJobsService } from "./lookoutV2/GroupJobsService"
import { ILogService } from "./lookoutV2/LogService"
import { UpdateJobSetsService } from "./lookoutV2/UpdateJobSetsService"
import { UpdateJobsService } from "./lookoutV2/UpdateJobsService"

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
