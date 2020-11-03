import  React, { useEffect, useState } from 'react';
import { LookoutSystemOverview } from '../openapi';
import { JobService } from './services/jobs';

export function Overview ({jobs}: {jobs: JobService}): JSX.Element {

    console.log(jobs)

    const [overview, setOverview] = useState<LookoutSystemOverview>({});
    const loadData = async () => {
        const res = await jobs.getOverview();
        setOverview(res);
    };  

    useEffect(() => {
        loadData();
        return () => {};
    }, []);

    
    return  (<div>
        { (overview.queues || []).map(q => 
            
            <p> { q.queue } </p>
            
            
        ) }      
        </div>);
}