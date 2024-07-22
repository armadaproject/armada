@startuml
JobService -> EventClient : Calls Event client with job-set and queue
EventClient -> JobService : Stores all job status and message for a given job-set in the database
Database -> JobServiceClient : Calls Database via rpc call to retrieve status and message for given id
@enduml