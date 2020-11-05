import  React, { useEffect, useState } from 'react';
import { LookoutSystemOverview } from './openapi';
import { JobService } from './services/jobs';

export function Overview ({jobService: jobs}: {jobService: JobService}): JSX.Element {

    const [overview, setOverview] = useState<LookoutSystemOverview>({});
    const loadData = async () => {
        const res = await jobs.getOverview();
        setOverview(res);
    };  

    useEffect(() => {
        loadData();
        return () => {};
    });

    return  (<div>
        { (overview.queues || []).map(q => 
            
            <p> { q.queue } </p>
        ) }      
        </div>);
}
