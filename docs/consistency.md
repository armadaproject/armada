# A note on consistency

The data stream approach taken by Armada is not the only way to maintain consistency across views. Here, we compare this approach with the two other possible solutions.

Armada stores its state across several databases. Whenever Armada receives an API call to update its state, all those databases need to be updated. However, if each database were to be updated independently it is possible for some of those updates to succeed while others fail, leading to an inconsistent application state. It would require complex logic to detect and correct for such partial failures. However, even with such logic we could not guarantee that the application state is consistent; if Armada crashes before it has had time to correct for the partial failure the application may remain in an inconsistent state.

There are three commonly used approaches to address this issue:

* Store all state in a single database with support for transactions. Changes are submitted atomically and are rolled back in case of failure; there are no partial failures.
* Distributed transaction frameworks (e.g., X/Open XA), which extend the notation of transactions to operations involving several databases.
* Ordered idempotent updates.

The first approach results in tight coupling between components and would limit us to a single database technology. Adding a new component (e.g., a new dashboard) could break existing component since all operations part of the transaction are rolled back if one fails. The second approach allows us to use multiple databases (as long as they support the distributed transaction framework), but components are still tightly coupled since they have to be part of the same transaction. Further, there are performance concerns associated with these options, since transactions may not be easily scalable. Hence, we use the third approach, which we explain next.

First, note that if we can replay the sequence of state transitions that led to the current state, in case of a crash we can recover the correct state by truncating the database and replaying all transitions from the beginning of time. Because operations are ordered, this always results in the same end state. If we also, for each database, store the id of the most recent transition successfully applied to that database, we only need to replay transitions more recent than that. This saves us from having to start over from a clean database; because we know where we left off we can keep going from there. For this to work, we need transactions but not distributed transactions. Essentially, applying a transition already written to the database results in a no-op, i.e., the updates are idempotent (meaning that applying the same update twice has the same effect as applying it once).

The two principal drawbacks of this approach are:

* Eventual consistency: Whereas the first two approaches result in a system that is always consistent, with the third approach, because databases are updated independently, there will be some replication lag during which some part of the state may be inconsistent.
* Timeliness: There is some delay between submitting a change and that change being reflected in the application state.

Working around eventual consistency requires some care, but is not impossible. For example, it is fine for the UI to show the a job as "running" for a few seconds after the job has finished before showing "completed". Regarding timeliness, it is not a problem if there is a few seconds delay between a job being submitted and the job being considered for queueing. However, poor timeliness may lead to clients (i.e., the entity submitting jobs to the system) not being able to read their own writes for some time, which may lead to confusion (i.e., there may be some delay between a client submitting a job a that job showing as "pending"). This issue can be worked around by storing the set of submitted jobs in-memory either at the client or at the API endpoint.
