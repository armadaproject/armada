@startuml
JobService -> EventClient : Calls Event client with job-set and queue
EventClient -> Redis : Stores all job status and message for a given job-set
Redis -> JobServiceClient : Calls redis via rpc call to retrieve status and message for given id 
@enduml
