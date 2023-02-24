import { memo, useCallback, useEffect, useRef, useState } from "react"

import { TabContext, TabPanel } from "@mui/lab"
import { Box, Divider, Drawer, Tab, Tabs } from "@mui/material"
import { Job } from "models/lookoutV2Models"

import { IGetRunErrorService } from "../../../services/lookoutV2/GetRunErrorService"
import { SidebarHeader } from "./SidebarHeader"
import { SidebarTabJobDetails } from "./SidebarTabJobDetails"
import { SidebarTabJobRuns } from "./SidebarTabJobRuns"

enum SidebarTab {
  JobDetails = "JobDetails",
  JobRuns = "JobRuns",
  Yaml = "Yaml",
  Logs = "Logs",
}

export interface SidebarProps {
  job: Job
  runErrorService: IGetRunErrorService
  onClose: () => void
}
export const Sidebar = memo(({ job, runErrorService, onClose }: SidebarProps) => {
  const [openTab, setOpenTab] = useState<SidebarTab>(SidebarTab.JobDetails)
  const handleTabChange = useCallback((_, newValue: SidebarTab) => {
    setOpenTab(newValue)
  }, [])

  // Logic to keep the sidebar the correct height while accounting for possible page headers
  const ref = useRef<HTMLDivElement>(null)
  const [visibleHeightAboveElement, setVisibleHeightAboveElement] = useState(0)
  useEffect(() => {
    const onScroll = () => {
      if (ref.current !== null) {
        setVisibleHeightAboveElement(ref.current.offsetTop - window.scrollY)
      }
    }

    // Calculate on first load
    onScroll()

    // Recalculate on every scroll
    window.addEventListener("scroll", onScroll)
    return () => window.removeEventListener("scroll", onScroll)
  }, [ref])

  return (
    <Drawer
      ref={ref}
      anchor="right"
      variant="permanent"
      role="complementary"
      // Root element
      sx={{
        position: "sticky",
        top: 0,
        height: `calc(100vh - ${visibleHeightAboveElement}px)`,

        width: "30%",
        minWidth: "350px",
      }}
      // Child element
      PaperProps={{
        sx: {
          width: "100%",
          position: "initial",
        },
      }}
      open={true}
    >
      <Box sx={{ display: "flex", flexDirection: "column", gap: "0.5em", padding: "0.5em" }}>
        <SidebarHeader job={job} onClose={onClose} />
        <Divider />
        <TabContext value={openTab}>
          <Tabs value={openTab} onChange={handleTabChange}>
            <Tab label="Details" value={SidebarTab.JobDetails} sx={{ minWidth: "50px" }}></Tab>
            <Tab label="Runs" value={SidebarTab.JobRuns} sx={{ minWidth: "50px" }}></Tab>
            <Tab label="Yaml" value={SidebarTab.Yaml} disabled sx={{ minWidth: "50px" }}></Tab>
            <Tab label="Logs" value={SidebarTab.Logs} disabled sx={{ minWidth: "50px" }}></Tab>
          </Tabs>

          <TabPanel value={SidebarTab.JobDetails}>
            <SidebarTabJobDetails job={job} />
          </TabPanel>

          <TabPanel value={SidebarTab.JobRuns}>
            <SidebarTabJobRuns job={job} runErrorService={runErrorService} />
          </TabPanel>

          <TabPanel value={SidebarTab.Yaml}>TODO</TabPanel>

          <TabPanel value={SidebarTab.Logs}>TODO</TabPanel>
        </TabContext>
      </Box>
    </Drawer>
  )
})
