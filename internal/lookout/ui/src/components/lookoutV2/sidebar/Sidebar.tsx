import { memo, useCallback, useEffect, useRef, useState } from "react"

import { TabContext, TabPanel } from "@mui/lab"
import { Box, Divider, Drawer, Tab, Tabs } from "@mui/material"
import { Job, JobState } from "models/lookoutV2Models"

import styles from "./Sidebar.module.css"
import { SidebarHeader } from "./SidebarHeader"
import { SidebarTabJobDetails } from "./SidebarTabJobDetails"
import { SidebarTabJobLogs } from "./SidebarTabJobLogs"
import { SidebarTabJobRuns } from "./SidebarTabJobRuns"
import { SidebarTabJobYaml } from "./SidebarTabJobYaml"
import { ICordonService } from "../../../services/lookoutV2/CordonService"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { IGetRunErrorService } from "../../../services/lookoutV2/GetRunErrorService"
import { ILogService } from "../../../services/lookoutV2/LogService"

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
  cordonService: ICordonService
  sidebarWidth: number
  onClose: () => void
  onWidthChange: (width: number) => void
}

export const Sidebar = memo(
  ({
    job,
    runErrorService,
    jobSpecService,
    logService,
    cordonService,
    sidebarWidth,
    onClose,
    onWidthChange,
  }: SidebarProps) => {
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
        <div className={styles.sidebarContainer}>
          <div onMouseDown={(e) => handleMouseDown(e.clientX)} id="dragger" className={resizerClasses} />
          <Box className={styles.sidebarContent}>
            <SidebarHeader job={job} onClose={onClose} className={styles.sidebarHeader} />
            <Divider />
            <div className={styles.sidebarTabContext}>
              <TabContext value={openTab}>
                <Tabs value={openTab} onChange={handleTabChange} className={styles.sidebarTabs}>
                  <Tab label="Details" value={SidebarTab.JobDetails} sx={{ minWidth: "50px" }}></Tab>
                  <Tab label="Runs" value={SidebarTab.JobRuns} sx={{ minWidth: "50px" }}></Tab>
                  <Tab label="Yaml" value={SidebarTab.Yaml} sx={{ minWidth: "50px" }}></Tab>
                  <Tab
                    label="Logs"
                    value={SidebarTab.Logs}
                    sx={{ minWidth: "50px" }}
                    disabled={job.state === JobState.Queued}
                  ></Tab>
                </Tabs>

                <TabPanel value={SidebarTab.JobDetails} className={styles.sidebarTabPanel}>
                  <SidebarTabJobDetails job={job} jobSpecService={jobSpecService} />
                </TabPanel>

                <TabPanel value={SidebarTab.JobRuns} className={styles.sidebarTabPanel}>
                  <SidebarTabJobRuns job={job} runErrorService={runErrorService} cordonService={cordonService} />
                </TabPanel>

                <TabPanel value={SidebarTab.Yaml} className={styles.sidebarTabPanel}>
                  <SidebarTabJobYaml job={job} jobSpecService={jobSpecService} />
                </TabPanel>

                <TabPanel value={SidebarTab.Logs} className={styles.sidebarTabPanel}>
                  <SidebarTabJobLogs job={job} jobSpecService={jobSpecService} logService={logService} />
                </TabPanel>
              </TabContext>
            </div>
          </Box>
        </div>
      </Drawer>
    )
  },
)
