@startuml
User -> Airflow : Creates a dag
Airflow -> AirflowOperator : Specify ArmadaPythonClient and JobServiceClient and pod definitions
AirflowOperator -> ArmadaPythonClient : Submits pod spec to Armada
AirflowOperator -> JobServiceClient : Polls GetJobStatus rpc call for given job id
AirflowOperator <- JobServiceClient : Wait for finished event and returns state, message
Airflow <- AirflowOperator : Airflow moves on to new task in schedule
@enduml