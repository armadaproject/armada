import { HTMLProps, memo, SyntheticEvent, useCallback, useEffect, useRef, useState } from "react"

import { TabContext, TabPanel, TabPanelProps } from "@mui/lab"
import { Divider, Drawer, DrawerProps, Stack, styled, Tab, Tabs } from "@mui/material"
import { grey } from "@mui/material/colors"

import { SidebarHeader } from "./SidebarHeader"
import { SidebarTabJobCommands } from "./SidebarTabJobCommands"
import { SidebarTabJobDetails } from "./SidebarTabJobDetails"
import { SidebarTabJobLogs } from "./SidebarTabJobLogs"
import { SidebarTabJobResult } from "./SidebarTabJobResult"
import { SidebarTabJobYaml } from "./SidebarTabJobYaml"
import { SidebarTabScheduling } from "./SidebarTabScheduling"
import { Job, JobState } from "../../../models/lookoutV2Models"
import { ICordonService } from "../../../services/lookoutV2/CordonService"
import { IGetJobInfoService } from "../../../services/lookoutV2/GetJobInfoService"
import { IGetRunInfoService } from "../../../services/lookoutV2/GetRunInfoService"
import { SPACING } from "../../../styling/spacing"
import { CommandSpec } from "../../../utils"

enum SidebarTab {
  JobDetails = "JobDetails",
  JobResult = "JobResult",
  Scheduling = "Scheduling",
  Yaml = "Yaml",
  Logs = "Logs",
  Commands = "Commands",
}

const SidebarContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  height: "100%",
  width: "100%",
  justifyContent: "space-between",
})

const SidebarContent = styled("div")({
  flex: "1 0 1px",
  display: "flex",
  flexDirection: "column",
  gap: "0.5em",
  padding: "0.5em",
  height: "100%",
  overflowY: "auto",
})

const SidebarTabContextContainer = styled("div")({
  width: "100%",
  flex: "1 0 1px",
  overflow: "auto",
  display: "flex",
  flexDirection: "column",
})

const SidebarTabs = styled(Tabs)({
  maxWidth: "100%",
  flex: "0 0 auto",
})

const StyledSidebarTab = styled(Tab)({ minWidth: 50 })

const Resizer = styled("div", { shouldForwardProp: (prop) => prop !== "isResizing" })<
  HTMLProps<"div"> & { isResizing: boolean }
>(({ isResizing, theme }) => [
  {
    flex: "0 0 5px",
    height: "100%",
    width: 5,
    backgroundColor: isResizing ? theme.palette.primary.dark : grey[500],
    cursor: "col-resize",
    userSelect: "none",
    touchAction: "none",
    opacity: isResizing ? 1 : 0,
  },
  theme.applyStyles("dark", {
    backgroundColor: isResizing ? theme.palette.primary.light : grey[600],
  }),
])

const StyledDrawer = styled(Drawer, {
  shouldForwardProp: (prop) => prop !== "visibleHeightAboveElement" && prop !== "sidebarWidth",
})<DrawerProps & { visibleHeightAboveElement: number; sidebarWidth: number }>(
  ({ visibleHeightAboveElement, sidebarWidth }) => ({
    position: "sticky",
    top: 0,
    height: `calc(100vh - ${visibleHeightAboveElement}px)`,
    width: sidebarWidth,
    minWidth: 350,

    "&:hover": {
      // With @emotion/babel-plugin, we can use emotion styled components to be targeted as a CSS
      // selector (https://emotion.sh/docs/styled#targeting-another-emotion-component), however, our
      // TS Config does not currently resolve this
      [Resizer as unknown as string]: {
        opacity: 1,
      },
    },
  }),
)

const StyledSidebarTabPanelStack = styled(Stack)({
  width: "100%",
  height: "100%",
})

const StyledSidebarTabPanel = styled(TabPanel)({
  width: "100%",
  flex: "1 0 1px",
  overflow: "auto",
  paddingBottom: 5,
})

const TabsContainer = styled("div")(({ theme }) => ({
  borderBottom: "1px solid",
  borderColor: theme.palette.divider,
}))

export const SidebarTabPanel = ({ children, ...props }: TabPanelProps) => (
  <StyledSidebarTabPanel {...props}>
    <StyledSidebarTabPanelStack spacing={SPACING.md}>{children}</StyledSidebarTabPanelStack>
  </StyledSidebarTabPanel>
)

type ResizeState = {
  isResizing: boolean
  startX: number
  currentX: number
}

export interface SidebarProps {
  job: Job
  runInfoService: IGetRunInfoService
  jobSpecService: IGetJobInfoService
  cordonService: ICordonService
  sidebarWidth: number
  commandSpecs: CommandSpec[]
  onClose: () => void
  onWidthChange: (width: number) => void
}

export const Sidebar = memo(
  ({
    job,
    runInfoService,
    jobSpecService,
    cordonService,
    sidebarWidth,
    onClose,
    onWidthChange,
    commandSpecs,
  }: SidebarProps) => {
    const [openTab, setOpenTab] = useState<SidebarTab>(SidebarTab.JobDetails)
    useEffect(() => {
      if (openTab === SidebarTab.Scheduling && job.state !== JobState.Queued) {
        setOpenTab(SidebarTab.JobDetails)
      }
    }, [openTab, job.state])

    const handleTabChange = useCallback((_: SyntheticEvent, newValue: SidebarTab) => {
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

    return (
      <StyledDrawer
        id="resizable"
        ref={ref}
        anchor="right"
        variant="permanent"
        role="complementary"
        visibleHeightAboveElement={visibleHeightAboveElement}
        sidebarWidth={sidebarWidth}
        // Child element
        PaperProps={{
          sx: {
            width: "100%",
            position: "initial",
          },
        }}
        open={true}
      >
        <SidebarContainer>
          <Resizer onMouseDown={(e) => handleMouseDown(e.clientX)} id="dragger" isResizing={resizeRef.current} />
          <SidebarContent>
            <SidebarHeader job={job} onClose={onClose} />
            <Divider />
            <SidebarTabContextContainer>
              <TabContext value={openTab}>
                <TabsContainer>
                  <SidebarTabs value={openTab} onChange={handleTabChange}>
                    <StyledSidebarTab label="Details" value={SidebarTab.JobDetails} />
                    <StyledSidebarTab label="Result" value={SidebarTab.JobResult} />
                    {job.state === JobState.Queued && (
                      <StyledSidebarTab label="Scheduling" value={SidebarTab.Scheduling} />
                    )}
                    <StyledSidebarTab label="Yaml" value={SidebarTab.Yaml} />
                    <StyledSidebarTab label="Logs" value={SidebarTab.Logs} disabled={job.state === JobState.Queued} />
                    <StyledSidebarTab
                      label="Commands"
                      value={SidebarTab.Commands}
                      disabled={job.state === JobState.Queued}
                    />
                  </SidebarTabs>
                </TabsContainer>

                <SidebarTabPanel value={SidebarTab.JobDetails}>
                  <SidebarTabJobDetails key={job.jobId} job={job} />
                </SidebarTabPanel>

                <SidebarTabPanel value={SidebarTab.JobResult}>
                  <SidebarTabJobResult
                    key={job.jobId}
                    job={job}
                    jobInfoService={jobSpecService}
                    runInfoService={runInfoService}
                    cordonService={cordonService}
                  />
                </SidebarTabPanel>

                {job.state === JobState.Queued && (
                  <SidebarTabPanel value={SidebarTab.Scheduling}>
                    <SidebarTabScheduling key={job.jobId} job={job} />
                  </SidebarTabPanel>
                )}

                <SidebarTabPanel value={SidebarTab.Yaml}>
                  <SidebarTabJobYaml key={job.jobId} job={job} />
                </SidebarTabPanel>

                <SidebarTabPanel value={SidebarTab.Logs}>
                  <SidebarTabJobLogs key={job.jobId} job={job} />
                </SidebarTabPanel>

                <SidebarTabPanel value={SidebarTab.Commands}>
                  <SidebarTabJobCommands key={job.jobId} job={job} commandSpecs={commandSpecs} />
                </SidebarTabPanel>
              </TabContext>
            </SidebarTabContextContainer>
          </SidebarContent>
        </SidebarContainer>
      </StyledDrawer>
    )
  },
)
