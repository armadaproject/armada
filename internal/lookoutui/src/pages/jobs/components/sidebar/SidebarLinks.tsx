import { useMemo } from "react"

import { Link } from "@mui/icons-material"
import { Stack, Chip, alpha, styled } from "@mui/material"
import { blend } from "@mui/system"
import { template } from "lodash"

import { SPACING } from "../../../../common/spacing"
import { getConfig } from "../../../../config"
import { Job } from "../../../../models/lookoutModels"
import { useGetJobSpec } from "../../../../services/lookout/useGetJobSpec"

const LinkChip = styled(Chip<"a">)<{ linkChipColour: string }>(({ theme, linkChipColour }) => ({
  backgroundColor: linkChipColour,
  color: theme.palette.getContrastText(linkChipColour),
  transitionProperty: "background-color, box-shadow, color",

  "&:hover": {
    backgroundColor: alpha(linkChipColour, 0.5),
    color: theme.palette.getContrastText(blend(theme.palette.background.paper, linkChipColour, 0.5)),
  },
}))

const { jobLinks, userAnnotationPrefix } = getConfig()

interface JobLink {
  label: string
  colour: string
  href: string
}

const getJobLinks = (job: Job): JobLink[] => {
  // Template links using all job annoation keys, plus all job annotation keys prepended with userAnnotationPrefix
  const templateContext = { ...job, annotations: { ...job.annotations } }
  Object.entries(templateContext.annotations).forEach(([k, v]) => {
    templateContext.annotations[userAnnotationPrefix + k] = v
  })
  // Wrap job annotations in a Proxy that throws on missing annotation keys.
  templateContext.annotations = new Proxy(templateContext.annotations, {
    get(target: any, prop) {
      if (!(prop in target)) {
        throw new Error(`Missing key: ${String(prop)}`)
      }
      return target[prop]
    },
  })

  return jobLinks.flatMap<JobLink>(({ label, colour, linkTemplate }) => {
    try {
      const compiledTemplate = template(linkTemplate, { interpolate: /{{([\s\S]+?)}}/g })
      const templated = compiledTemplate(templateContext)
      return [{ label, colour, href: templated }]
    } catch {
      // Do not return a link if templating fails for any reason
      return []
    }
  })
}

export interface SidebarLinksProps {
  job: Job
}

export const SidebarLinks = ({ job }: SidebarLinksProps) => {
  // The Lookout ingester may be configured to filter out annotation. We therefore
  // merge in the annotations present in the job spec on a best-effort basis.
  const { status, data } = useGetJobSpec(job.jobId, Boolean(job.jobId))
  const linksForJob = useMemo(() => {
    if (status === "success") {
      const mergedJob: Job = { ...job, annotations: { ...job.annotations, ...(data.annotations ?? {}) } }
      return getJobLinks(mergedJob)
    }
    return getJobLinks(job)
  }, [job, data])

  if (linksForJob.length === 0) {
    return null
  }

  return (
    <Stack spacing={SPACING.xs} direction="row">
      {linksForJob.map(({ label, colour, href }) => (
        <div key={label + colour + href}>
          <LinkChip
            component="a"
            clickable
            label={label}
            href={href}
            target="_blank"
            variant="filled"
            size="small"
            icon={<Link color="inherit" />}
            linkChipColour={colour}
          />
        </div>
      ))}
    </Stack>
  )
}
