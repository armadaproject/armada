import { TabContext, TabPanel } from "@mui/lab"
import { Box, Divider, Drawer, Tab, Tabs, Toolbar } from "@mui/material"
import { Job } from "models/lookoutV2Models"
import { useCallback, useState } from "react"
import { SidebarJobDetails } from "./SidebarJobDetails"
import { SidebarHeader } from "./SidebarHeader"
import { SidebarJobRuns } from "./SidebarJobRuns"

enum SidebarTab {
  JobDetails = "JobDetails",
  JobRuns = "JobRuns",
  Yaml = "Yaml",
  Logs = "Logs",
}

export interface SidebarProps {
  job: Job
  onClose: () => void
}
export const Sidebar = ({ job, onClose }: SidebarProps) => {

  const [openTab, setOpenTab] = useState<SidebarTab>(SidebarTab.JobDetails)
  const handleTabChange = useCallback((_, newValue: SidebarTab) => {
    setOpenTab(newValue)
  }, [])
  return (
    <Drawer
      anchor="right"
      variant="permanent"
      sx={{
        width: "30%",
        minWidth: "350px",
        // maxWidth: "30%",
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          minWidth: "350px",
          width: "30%",
          boxSizing: 'border-box',
        },
      }}
      open={true}
    >
      <Box sx={{ display: "flex", flexDirection: "column", gap: "0.5em", padding: "0.5em" }}>
        <SidebarHeader job={job} onClose={onClose}/>
        <Divider />
        <TabContext value={openTab}>
          <Tabs value={openTab} onChange={handleTabChange}>
            <Tab label="Details" value={SidebarTab.JobDetails} sx={{minWidth: "50px"}}></Tab>
            <Tab label="Runs" value={SidebarTab.JobRuns} sx={{minWidth: "50px"}}></Tab>
            <Tab label="Yaml" value={SidebarTab.Yaml} disabled sx={{minWidth: "50px"}}></Tab>
            <Tab label="Logs" value={SidebarTab.Logs} disabled sx={{minWidth: "50px"}}></Tab>
          </Tabs>
          <TabPanel value={SidebarTab.JobDetails}>
            <SidebarJobDetails job={job}/>
          </TabPanel>
          <TabPanel value={SidebarTab.JobRuns}>
            <SidebarJobRuns job={job} />
          </TabPanel>
          <TabPanel value={SidebarTab.Yaml}>
            TODO
          </TabPanel>
          <TabPanel value={SidebarTab.Logs}>
            TODO
          </TabPanel>
        </TabContext>
      </Box>

    </Drawer>
  )
}
