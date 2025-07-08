# Maintaining consistency across views
- [Maintaining consistency across views](#maintaining-consistency-across-views)
  - [Ensuring a consistent application state](#ensuring-a-consistent-application-state)
    - [Storing in a single database](#storing-in-a-single-database)
    - [Distributed transaction frameworks](#distributed-transaction-frameworks)
    - [Ordered idempotent updates](#ordered-idempotent-updates)
  - [Drawbacks of idempotent updates](#drawbacks-of-idempotent-updates)
    - [Handling eventual consistency](#handling-eventual-consistency)
    - [Handling timeliness](#handling-timeliness)

Armada stores its state across several databases. Whenever Armada receives an API call to update its state, all those databases need to be updated.

However, if we update each database independently, some of those updates would succeed while others failed, leading to an inconsistent application state. It would require complex logic to detect and correct for such partial failures. However, even with such logic, we could not guarantee a consistent application state. If Armada crashes before it's had time to correct for the partial failure, the application may remain in an inconsistent state.

## Ensuring a consistent application state

There are three commonly used approaches to address this issue:

* store all state in a single database with support for transactions (changes are submitted atomically and rolled back in case of failure; there are no partial failures)
* distributed transaction frameworks (for example, X/Open XA), which extend the notation of transactions to operations involving several databases
* ordered idempotent updates

### Storing in a single database

This approach results in tight coupling between components and would limit us to a single database technology. Adding a new component (for example, a new dashboard) could break existing components since all operations part of the transaction are rolled back if one fails.

### Distributed transaction frameworks

This approach enables us to use multiple databases (as long as they support the distributed transaction framework), but components are still tightly coupled since they have to be part of the same transaction.

### Ordered idempotent updates

G-Research uses the following approach, since the previous approaches have performance concerns because transactions may not be easily scalable.

If we can replay the sequence of state transitions that led to the current state, in case of a crash we can recover the correct state by truncating the database and replaying all transitions from the beginning of time. Because operations are ordered, this always results in the same end state.

If we also, for each database, store the ID of the most recent transition successfully applied to that database, we only need to replay transitions more recent than that. This saves us from having to start over from a clean database; because we know where we left off, we can keep going from there. For this to work, we need transactions, but not distributed transactions. Essentially, applying a transition already written to the database results in a no-op. In other words, the updates are idempotent (meaning that applying the same update twice has the same effect as applying it once).

## Drawbacks of idempotent updates

The two principal drawbacks of this approach are:

* eventual consistency: whereas the first two approaches result in a system that is always consistent, with the third approach, because databases are updated independently, there will be some replication lag during which some part of the state may be inconsistent
* timeliness: there is some delay between submitting a change and that change being reflected in the application state

### Handling eventual consistency

It is fine for the UI to show a job as 'running' for a few seconds after the job has finished before showing 'completed'.

### Handling timeliness

It is fine if there is a few seconds delay between a job being submitted and the job being considered for queueing. However, poor timeliness may lead to clients (the entities submitting jobs to the system) being unable to read their own writes for some time. This can create confusion if, for example, there's a delay between a client submitting a job and that job showing as 'pending'. To work around this issue, store the set of submitted jobs in-memory either at the client or at the API endpoint.
