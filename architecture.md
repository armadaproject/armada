# Armada architecture
- [Armada architecture](#armada-architecture)
  - [Log properties](#log-properties)
  - [Log implementation](#log-implementation)
  - [System overview](#system-overview)
    - [Job submission logic](#job-submission-logic)
    - [Streams API](#streams-api)
  - [Maintaining consistency](#maintaining-consistency)
    - [Ensuring a consistent application state](#ensuring-a-consistent-application-state)
      - [Storing in a single database](#storing-in-a-single-database)
      - [Distributed transaction frameworks](#distributed-transaction-frameworks)
      - [Ordered idempotent updates](#ordered-idempotent-updates)
        - [Drawbacks of idempotent updates](#drawbacks-of-idempotent-updates)
          - [Handling eventual consistency](#handling-eventual-consistency)
          - [Handling timeliness](#handling-timeliness)

Armada is designed to manage millions of batch jobs across compute clusters made up of potentially hundreds of thousands of nodes, while providing near-constant uptime. Hence, the its architecture must be highly resilient and scalable. The current architecture was chosen in early 2022 to achieve these goals while also ensuring new features, for example, advanced scheduling techniques, can be delivered.

At a high level, Armada is a so-called data stream system (sometimes referred to as an event sourcing system), for which there are two components responsible for tracking the state of the system:

* a log-based message broker that stores state transitions durably in order, referred to throughout this document simply as 'the log'
* a set of databases, each deriving its state from the log (but are otherwise mutually independent) and storing a different so-called materialised view of the state of the system

## Log properties

The log is a publish-subscribe system consisting of multiple topics to which messages can be published. Those messages are eventually delivered to all subscribers of the topic. Important properties of the log are:

* durability: messages published to the log are stored in a durable manner, i.e., they are not lost in case of up to x node failures, where x is a tuneable parameter
* ordering: all subscribers of a topic see messages in the same order as they were published in, and replaying messages on a topic always results in the same message order (further, all messages on the same topic are annotated with a message ID that is monotonically increasing within each topic)

## Log implementation

In Armada, the log is implemented using Apache Pulsar.

In a datastream system, the log is the source of truth and the databases an optimisation to simplify querying – since the databases can be re-constructed by replaying messages from the log, if the log was replayed for each query, although highly unpractical, the databases could be omitted. For example, in Armada there are separate PostgreSQL databases for storing jobs to be scheduled and the jobs to be shown in the UI, Lookout. Both of these derive their state from the log but are otherwise independent.

To change the state of the system, a message (for example, corresponding to a job being submitted) is published to the log. Later, that message is picked up by a log processor, which updates some database accordingly (in the case of a job being submitted, by storing the new job in the database). Hence, the log serialises state transitions and the database is a materialised view of part of the state of the system, as derived from the state transitions submitted to the log. In effect, a datastream system is a bespoke distributed database with the log acting as the transaction log.

This approach has several benefits:

* resiliency towards bursts of high load: because the log buffers state transitions, the components reading from the log and acting on those transitions are not directly affected by incoming requests
* simplicity and extensibility: adding new materialised views (for example, for a new dashboard) can be accomplished by adding a new subscriber to the log (this new subscriber has the same source of truth as all others, the log, but is loosely coupled to those components; adding or removing views does not affect other components of the system)
* consistency: When storing state across several independent databases, those databases are guaranteed to eventually be consistent; there is no failure scenario where the different databases become permanently inconsistent, thus requiring a human to manually reconcile them (assuming acting on state transitions is idempotent)

However, the approach also has some drawbacks:

* eventual consistency: because each database is updated from the log independently, they do not necessarily represent the state of the system at the same point of time (for example, a job may be written to the scheduler database, thus making it eligible for scheduling, before it shows up in the UI)
* timeliness: because databases are updated from the log asynchronously, there may be a lag between a message being published and the system being updated to reflect the change (for example, a submitted job may not show up in the UI immediately)

## System overview

Besides the log, Armada consists of the following components:

* Submit API: clients (or users) connect to this API to request state transitions (for example, submitting jobs or updating job priorities), and each such state transition is communicated to the rest of the system by writing to the log
* Streams API: clients connect to this API to subscribe to log messages for a particular set of jobs (Armada components can receive messages either via this API or directly from the log, but users have to go via the streams API to isolate them from internal messages)
* Scheduler: a log processor responsible for maintaining a global view of the system and preempting and scheduling jobs (preemption and scheduling decisions are communicated to he rest of the system by writing to the log)
* Executors: each executor is responsible for one Kubernetes worker cluster and is the component that communicates between the Armada scheduler and the Kubernetes API of the cluster it is responsible for
* Lookout: the web UI showing the current state of the system (Lookout maintains its views by reading log messages to populate its database)

### Job submission logic

Here, we outline the sequence of actions resulting from submitting a job.

1. A client submits a job to the submit-query API, which is composed of a Kubernetes podspec and some Armada-specific metadata (for example, the priority of the job).
2. The submit API authenticates and authorizes the user, validates the submitted job, and, if valid, submits the job spec. to the log. The submit API annotates each job with a randomly generated UUID that uniquely identifies the job. This UUID is returned to the user.
3. The scheduler receives the job spec. and stores it in-memory (discarding any data it doesn't need, such as the pod spec). The scheduler runs periodically, at which point it schedules queued jobs. At the start of each scheduling run, the scheduler queries each executor for its available resources. The scheduler uses this information in making scheduling decisions. When the scheduler assigns a job to an executor, it submits a message to the log indicating this state transition. It also updates its in-memory storage immediately to reflect the change (to avoid scheduling the same job twice).
4. A log processor receives the message indicating the job was scheduled, and writes this decision to a database acting as the interface between the scheduler and the executor.
5. Periodically, each executor queries the database for the list of jobs it should be running. It compares that list with the list of jobs it is actually running and makes changes necessary to reconcile any differences.
6. When a job has finished, the executor responsible for running the job informs the scheduler, which on its behalf submits a 'job finished' message to the log. The same log processor as in step 4. updates its database to reflect that the job has finished.

### Streams API

Armada does not maintain a user-queryable database of the current state of the system. This is by design to avoid overloading the system with connections. For example, say there is one million active jobs in the system and that there are clients who want to track the state of all of those jobs. With a current-state-of-the-world database, those client would need to resort to polling that database to catch any updates, thus opening a total of one million connections to the database, which, while not impossible to manage, would pose significant challenges.

Instead, users are expected to be notified of updates to their jobs via an event stream (the streams API), where a client opens a single connection for all jobs in a so-called job set over which all state transitions are streamed as they happen. This approach is highly scalable since data is only sent when something happens and since a single connection that contain updates for thousands of jobs. Users who want to maintain a view of their jobs are thus responsible for maintaining that view themselves by subscribing to events.

## Maintaining consistency

Armada stores its state across several databases. Whenever Armada receives an API call to update its state, all those databases need to be updated.

However, if we update each database independently, some of those updates would succeed while others failed, leading to an inconsistent application state. It would require complex logic to detect and correct for such partial failures. However, even with such logic, we could not guarantee a consistent application state. If Armada crashes before it's had time to correct for the partial failure, the application may remain in an inconsistent state.

### Ensuring a consistent application state

There are three commonly used approaches to address this issue:

* store all state in a single database with support for transactions (changes are submitted atomically and rolled back in case of failure; there are no partial failures)
* distributed transaction frameworks (for example, X/Open XA), which extend the notation of transactions to operations involving several databases
* ordered idempotent updates

#### Storing in a single database

This approach results in tight coupling between components and would limit us to a single database technology. Adding a new component (for example, a new dashboard) could break existing components since all operations part of the transaction are rolled back if one fails.

#### Distributed transaction frameworks

This approach enables us to use multiple databases (as long as they support the distributed transaction framework), but components are still tightly coupled since they have to be part of the same transaction.

#### Ordered idempotent updates

G-Research uses the following approach, since the previous approaches have performance concerns because transactions may not be easily scalable.

If we can replay the sequence of state transitions that led to the current state, in case of a crash we can recover the correct state by truncating the database and replaying all transitions from the beginning of time. Because operations are ordered, this always results in the same end state.

If we also, for each database, store the ID of the most recent transition successfully applied to that database, we only need to replay transitions more recent than that. This saves us from having to start over from a clean database; because we know where we left off, we can keep going from there. For this to work, we need transactions, but not distributed transactions. Essentially, applying a transition already written to the database results in a no-op. In other words, the updates are idempotent (meaning that applying the same update twice has the same effect as applying it once).

##### Drawbacks of idempotent updates

The two principal drawbacks of this approach are:

* eventual consistency: whereas the first two approaches result in a system that is always consistent, with the third approach, because databases are updated independently, there will be some replication lag during which some part of the state may be inconsistent
* timeliness: there is some delay between submitting a change and that change being reflected in the application state

###### Handling eventual consistency

It is fine for the UI to show a job as 'running' for a few seconds after the job has finished before showing 'completed'.

###### Handling timeliness

It is fine if there is a few seconds delay between a job being submitted and the job being considered for queueing. However, poor timeliness may lead to clients (the entities submitting jobs to the system) being unable to read their own writes for some time. This can create confusion if, for example, there's a delay between a client submitting a job and that job showing as 'pending'. To work around this issue, store the set of submitted jobs in-memory either at the client or at the API endpoint.
