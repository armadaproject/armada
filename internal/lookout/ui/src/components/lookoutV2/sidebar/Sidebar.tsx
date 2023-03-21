import { memo, useCallback, useEffect, useRef, useState } from "react"

import { TabContext, TabPanel } from "@mui/lab"
import { Box, Divider, Drawer, Tab, Tabs } from "@mui/material"
import { Job } from "models/lookoutV2Models"

import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { IGetRunErrorService } from "../../../services/lookoutV2/GetRunErrorService"
import { ILogService } from "../../../services/lookoutV2/LogService"
import styles from "./Sidebar.module.css"
import { SidebarHeader } from "./SidebarHeader"
import { SidebarTabJobDetails } from "./SidebarTabJobDetails"
import { SidebarTabJobLogs } from "./SidebarTabJobLogs"
import { SidebarTabJobRuns, SidebarTabJobRunsProps } from "./SidebarTabJobRuns"
import { SidebarTabJobYaml } from "./SidebarTabJobYaml"

enum SidebarTab {
  JobDetails = "JobDetails",
  JobRuns = "JobRuns",
  Yaml = "Yaml",
  Logs = "Logs",
}

type ResizeState = {
  isResizing: boolean
  startX: number
  currentX: number
}

export interface SidebarProps {
  job: Job
  runErrorService: IGetRunErrorService
  jobSpecService: IGetJobSpecService
  logService: ILogService
  sidebarWidth: number
  onClose: () => void
  onWidthChange: (width: number) => void
}

export const Sidebar = memo(
  ({ job, runErrorService, jobSpecService, logService, sidebarWidth, onClose, onWidthChange }: SidebarProps) => {
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
      return () => {
        window.removeEventListener("scroll", onScroll)
      }
    }, [ref])

    // Hack: setting `isResizing` state field does not seem to work well with mousedown/mousemove listeners,
    // so we use a ref here instead. Note that the state is still needed to trigger re-renders
    const resizeRef = useRef(false)
    const [resizeState, setResizeState] = useState<ResizeState>({
      isResizing: false,
      startX: 0,
      currentX: 0,
    })
    const handleMouseDown = useCallback((x: number) => {
      resizeRef.current = true
      setResizeState({
        isResizing: true,
        startX: x,
        currentX: x,
      })
    }, [])
    const handleMouseMove = useCallback((x: number) => {
      if (!resizeRef.current) {
        return
      }
      setResizeState({
        ...resizeState,
        currentX: x,
      })
      const offsetRight = document.body.offsetWidth - (x - document.body.offsetLeft)
      const minWidth = 350
      const maxWidth = 1920
      if (offsetRight > minWidth && offsetRight < maxWidth) {
        onWidthChange(offsetRight)
      }
    }, [])
    const handleMouseUp = useCallback(() => {
      resizeRef.current = false
      setResizeState({
        ...resizeState,
        isResizing: false,
      })
    }, [])

    useEffect(() => {
      const mousemove = (e: MouseEvent) => {
        handleMouseMove(e.clientX)
      }
      document.addEventListener("mousemove", mousemove)
      document.addEventListener("mouseup", handleMouseUp)
      return () => {
        document.removeEventListener("mousemove", mousemove)
        document.removeEventListener("mouseup", handleMouseUp)
      }
    }, [])

    const resizerClasses = (resizeRef.current ? [styles.resizer, styles.isResizing] : [styles.resizer]).join(" ")
    return (
      <Drawer
        id="resizable"
        ref={ref}
        anchor="right"
        variant="permanent"
        role="complementary"
        sx={{
          position: "sticky",
          top: 0,
          height: `calc(100vh - ${visibleHeightAboveElement}px)`,
          width: sidebarWidth,
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
        <div
          style={{
            display: "flex",
            flexDirection: "row",
            height: "100%",
            width: "100%",
            justifyContent: "space-between",
          }}
        >
          <div onMouseDown={(e) => handleMouseDown(e.clientX)} id="dragger" className={resizerClasses} />
          <Box
            sx={{
              flex: "1 1 auto",
              display: "flex",
              flexDirection: "column",
              gap: "0.5em",
              padding: "0.5em",
              width: "100%",
              height: "100%",
              overflowY: "auto",
            }}
          >
            <SidebarHeader job={job} onClose={onClose} />
            <Divider />
            <TabContext value={openTab}>
              <Tabs value={openTab} onChange={handleTabChange}>
                <Tab label="Details" value={SidebarTab.JobDetails} sx={{ minWidth: "50px" }}></Tab>
                <Tab label="Runs" value={SidebarTab.JobRuns} sx={{ minWidth: "50px" }}></Tab>
                <Tab label="Yaml" value={SidebarTab.Yaml} sx={{ minWidth: "50px" }}></Tab>
                <Tab label="Logs" value={SidebarTab.Logs} sx={{ minWidth: "50px" }}></Tab>
              </Tabs>

              <TabPanel value={SidebarTab.JobDetails}>
                <SidebarTabJobDetails job={job} />
              </TabPanel>

              <TabPanel value={SidebarTab.JobRuns}>
                <SidebarTabJobRuns job={job} runErrorService={runErrorService} />
              </TabPanel>

              <TabPanel value={SidebarTab.Yaml}>
                <SidebarTabJobYaml job={job} jobSpecService={jobSpecService} />
              </TabPanel>

              <TabPanel value={SidebarTab.Logs} style={{ height: "100%" }}>
                <SidebarTabJobLogs job={job} jobSpecService={jobSpecService} logService={logService} />
              </TabPanel>
            </TabContext>
          </Box>
        </div>
      </Drawer>
    )
  },
)
