import React, { useEffect, useState } from 'react';
import { TableContainer, Table, TableHead, TableRow, TableCell, TableBody, Paper, Container } from '@material-ui/core';

import { LookoutSystemOverview } from './openapi';
import JobService from './services/JobService';

export function Overview({ jobService: jobs }: { jobService: JobService }): JSX.Element {

  const [overview, setOverview] = useState<LookoutSystemOverview>({});

  useEffect(() => {
    const loadData = async () => {
      const res = await jobs.getOverview();
      setOverview(res);
    };

    loadData();
    return () => {
    };
  }, [jobs]);

  return (
    <Container style={{ marginTop: "1em" }}>
      <TableContainer component={Paper}>
        <Table aria-label="simple table">
          <TableHead>
            <TableRow>
              <TableCell>Queue</TableCell>
              <TableCell align="right">jobs queued</TableCell>
              <TableCell align="right">jobs pending</TableCell>
              <TableCell align="right">jobs running</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {(overview.queues || []).map(q =>
              <TableRow key={q.queue}>
                <TableCell component="th" scope="row">
                  {q.queue}
                </TableCell>
                <TableCell align="right">{q.jobsQueued ?? 0}</TableCell>
                <TableCell align="right">{q.jobsPending ?? 0}</TableCell>
                <TableCell align="right">{q.jobsRunning ?? 0}</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
}
