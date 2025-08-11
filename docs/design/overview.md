# Armada Plan

"To offer scheduling features that surpass any other scheduler including both Slurm and AWS batch"

The focus here should be on providing value to the end user.  Typically this means either features or ease of use.

## API
A first class public API on which everything else can be built.  This API will power the Armada GUI (Lookout) and CLI (armadactl). This API should have first class support for 
array, mpi and gang jobs and should make common AI training patterns easy.

## CLI
A fully featured CLI in Armadactl that surpasses the functionality in the various Slurm CLIs as well as the functionality offered by AWS batch.  Functionality offered should include:
- Submitting jobs
- Monitoring jobs and jobsets
- Debugging jobs
- Understanding why jobs are not being scheduled.
- Investigating available resources

## Lookout
This is an area where Armada is already strong.  The aim here is to build on this with the following functionality:
- Fast analytics over all recent (4 weeks jobs) along multiple axes (jobset, node, cluster, submission time etc)
- Auto-refreshing views that allow users to track their jobs in real time
- Ability to analyse job performance (cpu/gpu utilisation etc) from within Lookout
- Views on available resources (e.g. number of nodes)

## Pluggable scheduler
To develop the scheduler to a point where it can be regarded pluggable. Key features that we may want to develop in this 
way so as to surpass the funcationality offered by Slurm would be:
- Market based scheduling
- Backfilling
- Defragmentation.
- Autoscaling

## Integrations
Integrations with industry standard tooling so that customers may easily use Armada at scale. Integrations will likely include:
* Airflow
* Flyte
* Jupyter
* Metaflow
* NextFlow
* Snakemake
* Prefect
* Torch distributed
* Dask
* Polars Distributed (when available)

## Logging
A complete logging solution that allows logs to fetched via the Armada API regardless of whether the job is running or
has finished.  This will necessarily need to be backed by persistant storage and must include retention policies.

# Analytics
The extension of drake to output data both more frequently (e.g. every 15 minutes) and to a format that can be read easily
into industry standard data science tooling. A paved road of importing into a tool we recommend e.g. duckdb or polars) should
also be provided.









