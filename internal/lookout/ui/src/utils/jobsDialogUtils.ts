import _ from "lodash"
import { Job, JobFilter } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"

export const getAllJobsMatchingFilters = async (
  filters: JobFilter[],
  activeJobSets: boolean,
  getJobsService: IGetJobsService,
): Promise<Job[]> => {
  const MAX_JOBS_PER_REQUEST = 10000
  const receivedJobs: Job[] = []
  let continuePaginating = true
  while (continuePaginating) {
    const { jobs, count: totalJobs } = await getJobsService.getJobs(
      filters,
      activeJobSets,
      { direction: "DESC", field: "jobId" },
      receivedJobs.length,
      MAX_JOBS_PER_REQUEST,
      undefined,
    )
    receivedJobs.push(...jobs)
    if (receivedJobs.length >= totalJobs || jobs.length === 0) {
      continuePaginating = false
    }
  }
  return receivedJobs
}

export const getUniqueJobsMatchingFilters = async (
  filtersGroups: JobFilter[][],
  activeJobSets: boolean,
  getJobsService: IGetJobsService,
): Promise<Job[]> => {
  const jobsBySelectedItem = await Promise.all(
    filtersGroups.map(async (filters) => await getAllJobsMatchingFilters(filters, activeJobSets, getJobsService)),
  )
  return _.uniqBy(jobsBySelectedItem.flat(), (job) => job.jobId)
}
