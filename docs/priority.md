# Armada priority

This document describes priority calculation algorithm in detail.

## How is priority calculated

### Resource usage
Armada schedules jobs which can use multiple types of resources (cpu, memory, gpu, ...).
To get one number which represents the share of a resource by a particular queue, Armada firstly calculates how much of particular
resource is available for one cpu `resource factor`.
Then queue usage can be calculated as `usage = # of cpu + # gpu / gpu factor + # memory / memory factor + ...` 

In example:
If our cluster has 10 cpus, 20Gb of memory and 5 gpus. <br />
Gpu factor will be `0.5` and memory factor `2`.<br />
Queue using 5 cpu, 2 Gb memory and 1 gpu will have usage `5 + 2 / 2 + 1 / 0.5 = 8` . 

### Queue priority
Queue priority is calculated based on current resource usage; if a particular queue usage is constant, the queue priority will approach this number and eventually stabilize on this value.
Armada allows configuration of `priorityHalftime` which influences how quickly queue priority approaches resource usage.

The formula for priority update is as follows (inspired by Condor priority calculation):

`priority = priority (1 - beta) + resourceUsage * beta`

`beta = 0.5 ^ (timeChange / priorityHalftime)` 

### Priority factor
Each queue has a priority factor, this is a multiplicative constant which is applied to the priority. The lower this number is the more resources a queue will be allocated in scheduling.

`effectivePriority = priority * priorityFactor`

## Scheduling resources
Available resources are divided between non empty queues based on queue priority. The share allocated to the queue is proportional to inverse of its priority.

For example if queue `A` has priority `1` and queue `B` priority `2`, `A` will get `2/3` and `B` `1/3` of the resources.

There are 2 approaches Armada uses to schedule jobs:

### Slices of resources
When the Executor requests new jobs with information about available resources, resources are divided into slices according to the inverse priority.

Armada iterates through queues and allocates jobs up to the slice size for each queue.

Whatever resources remain after this round are scheduled using probabilistic slicing.

This round is skipped if Armada Server is configured with the option `scheduling.useProbabilisticSchedulingForAllResources = true`.

### Probabilistic scheduling
To schedule any remaining resources Armada randomly selects a non-empty queue with probability distribution corresponding to  the remainders of queue slices. One job from this queue is scheduled, and the queue slice is reduced. This continues until there is no resource available, queues are empty or the scheduling time is up.

This way there is a chance than one queue will get allocated more than it is entitled to in the scheduling round. However as we are concerned with fair share over the time, rather than in a moment, this does not matter much. Queue priority will compensate for this in the future.
