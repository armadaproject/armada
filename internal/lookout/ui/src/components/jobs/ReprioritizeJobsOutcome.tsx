import React, { Fragment, Ref } from "react"

import {
    Paper,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Theme,
    colors,
    createStyles,
} from "@material-ui/core"
import { makeStyles } from "@material-ui/core/styles"

import { ReprioritizeJobsResult } from "../../services/JobService"
import LoadingButton from "./LoadingButton"

const useStyles = makeStyles((theme: Theme) =>
    createStyles({
        paper: {
            backgroundColor: theme.palette.background.paper,
            boxShadow: theme.shadows[5],
            padding: theme.spacing(2, 4, 3),
            outline: "none",
            borderRadius: "0.66em",
            maxHeight: "80%",
            maxWidth: "75%",
            display: "flex",
            flexDirection: "column",
            overflow: "hidden",
        },
        container: {
            minHeight: "6em",
            overflow: "auto",
            margin: theme.spacing(0, 0, 1),
        },
        successHeader: {
            backgroundColor: colors.green[100],
        },
        success: {
            backgroundColor: colors.green[50],
        },
        failureHeader: {
            backgroundColor: colors.red[100],
        },
        failure: {
            backgroundColor: colors.red[50],
        },
        button: {
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            marginTop: "1em",
        },
    }),
)

type ReprioritizeJobsOutcomeProps = {
    reprioritizeJobsResult: ReprioritizeJobsResult
    isLoading: boolean
    onReprioritizeJobs: () => void
}

const ReprioritizeJobsOutcome = React.forwardRef((props: ReprioritizeJobsOutcomeProps, ref: Ref<any>) => {
    const classes = useStyles()

    return (
        <div ref={ref} className={classes.paper}>
            <h2 id="cancel-jobs-modal-title" className="cancel-jobs-modal-title">
                Reprioritize jobs
            </h2>
            {props.reprioritizeJobsResult.reprioritizedJobs.length > 0 && (
                <Fragment>
                    <p id="cancel-jobs-modal-description" className="cancel-jobs-modal-description">
                        The following jobs were reprioritized successfully:
                    </p>
                    <TableContainer component={Paper} className={classes.container}>
                        <Table stickyHeader>
                            <TableHead>
                                <TableRow>
                                    <TableCell className={classes.successHeader}>Id</TableCell>
                                    <TableCell className={classes.successHeader}>Job Set</TableCell>
                                    <TableCell className={classes.successHeader}>Submission Time</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody className={classes.success}>
                                {props.reprioritizeJobsResult.reprioritizedJobs.map((job) => (
                                    <TableRow key={job.jobId}>
                                        <TableCell>{job.jobId}</TableCell>
                                        <TableCell>{job.jobSet}</TableCell>
                                        <TableCell>{job.submissionTime}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Fragment>
            )}
            {props.reprioritizeJobsResult.error.length > 0 && (
                <Fragment>
                    <p id="cancel-jobs-modal-description" className="cancel-jobs-modal-description">
                        Failed to reprioritize jobs because {props.reprioritizeJobsResult.error}
                    </p>
                    <div className={classes.button}>
                        <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onReprioritizeJobs} />
                    </div>
                </Fragment>
            )}
        </div>
    )
})

export default ReprioritizeJobsOutcome
