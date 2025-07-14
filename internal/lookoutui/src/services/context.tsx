import { createContext, ReactNode, useContext } from "react"

import { IGetJobInfoService } from "./lookout/GetJobInfoService"
import { IGetJobsService } from "./lookout/GetJobsService"
import { IGroupJobsService } from "./lookout/GroupJobsService"
import { UpdateJobSetsService } from "./lookout/UpdateJobSetsService"
import { UpdateJobsService } from "./lookout/UpdateJobsService"

export interface Services {
  v2GetJobsService: IGetJobsService
  v2GroupJobsService: IGroupJobsService
  v2JobSpecService: IGetJobInfoService
  v2UpdateJobsService: UpdateJobsService
  v2UpdateJobSetsService: UpdateJobSetsService
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
