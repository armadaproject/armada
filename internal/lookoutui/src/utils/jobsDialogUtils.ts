import _ from "lodash"

import { Job, JobFilter, JobFiltersWithExcludes } from "../models/lookoutModels"
import { IGetJobsService } from "../services/lookout/GetJobsService"

export const getAllJobsMatchingFilters = async (
  filters: JobFilter[],
  activeJobSets: boolean,
  getJobsService: IGetJobsService,
): Promise<Job[]> => {
  const MAX_JOBS_PER_REQUEST = 10000
  const receivedJobs: Job[] = []
  let continuePaginating = true
  while (continuePaginating) {
    const { jobs } = await getJobsService.getJobs(
      filters,
      activeJobSets,
      { direction: "DESC", field: "jobId" },
      receivedJobs.length,
      MAX_JOBS_PER_REQUEST,
      undefined,
    )
    receivedJobs.push(...jobs)
    if (jobs.length < MAX_JOBS_PER_REQUEST) {
      continuePaginating = false
    }
  }
  return receivedJobs
}

export const getUniqueJobsMatchingFilters = async (
  filtersGroups: JobFiltersWithExcludes[],
  activeJobSets: boolean,
  getJobsService: IGetJobsService,
): Promise<Job[]> => {
  const jobsBySelectedItem = await Promise.all(
    filtersGroups.map(async ({ jobFilters, excludesJobFilters }) => {
      const allMatchingJobs = await getAllJobsMatchingFilters(jobFilters, activeJobSets, getJobsService)
      const excludedJobs = (
        await Promise.all(
          excludesJobFilters.map((excludeJobFilters) =>
            getAllJobsMatchingFilters(excludeJobFilters, activeJobSets, getJobsService),
          ),
        )
      ).flat()
      const excludedJobIds = new Set(excludedJobs.map((job) => job.jobId))
      return allMatchingJobs.filter((job) => !excludedJobIds.has(job.jobId))
    }),
  )
  return _.uniqBy(jobsBySelectedItem.flat(), (job) => job.jobId)
}
