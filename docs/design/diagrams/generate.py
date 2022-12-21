from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.k8s.controlplane import API
from diagrams.custom import Custom

graph_attr = {
    "concentrate": "false",
    "splines": "ortho",
    "pad": "2",
    "nodesep": "0.30",
    "ranksep": "1.5",
    "fontsize": "20",
}

node_attr = {
    # decrease image size
    "fixedsize": "true",
    "width": "1",
    "height": "1",
    "fontsize": "15",
}

edge_attr = {
    "minlen": "1",
}

cluster_attr_common = {
    "margin": "20",
    "fontsize": "15",
}

cluster_attr_server = {
    "labelloc": "b",
    "bgcolor": "#c7ffd5",
}
cluster_attr_server = {**cluster_attr_common, **cluster_attr_server}

cluster_attr_exec = {
    "labelloc": "t",
    "bgcolor": "#c7ffd5",
}

cluster_attr_exec = {**cluster_attr_common, **cluster_attr_exec}

armada_logo = "./images/armada.png"
pulsar_logo = "./images/pulsar.png"
browser_logo = "./images/browser.png"

with Diagram(
    name="Armada Systems Diagram",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    edge_attr=edge_attr,
    node_attr=node_attr,
    filename="armada-system",
):

    pulsar = Custom("Pulsar", pulsar_logo)

    # Databases
    postgres_lookout = PostgreSQL("Postgres (Lookout)")
    postgres_scheduler = PostgreSQL("Postgres (Scheduler)")
    redis_events = Redis("Redis (Events)")
    redis_scheduler = Redis("Redis (Scheduler)")

    # Components
    submit_api = Custom("Submit API", armada_logo)

    # Ingesters
    lookout_ingester = Custom("Lookout Ingester", armada_logo)
    scheduler_ingester = Custom("Scheduler Ingester", armada_logo)
    event_ingerster = Custom("Event Ingester", armada_logo)
    job_ingester = Custom("Job Ingester", armada_logo)

    legacy_scheduler = Custom("Legacy Scheduler", armada_logo)
    scheduler = Custom("Scheduler", armada_logo)

    executor_api = Custom("Executor API", armada_logo)

    with Cluster("Executor Cluster", graph_attr=cluster_attr_server):

        executor = Custom("Executor", armada_logo)
        k8s_api = API("K8s API")

    with Cluster("Executor Cluster 2", graph_attr=cluster_attr_server):

        executor2 = Custom("Executor 2", armada_logo)
        k8s_api2 = API("K8s API 2")

    # Relationships

    # submit api talks to pulsar
    submit_api >> Edge(color="red") >> pulsar

    # pulsar talks to each of the ingesters
    pulsar >> Edge(color="red") >> lookout_ingester
    pulsar >> Edge(color="red") >> scheduler_ingester
    pulsar >> Edge(color="red") >> event_ingerster
    pulsar >> Edge(color="red") >> job_ingester

    # make postgres blue, redis orange
    # lookout and scheduler ingesters talk to postgres
    # the other ingesters talk to redis
    lookout_ingester >> Edge(color="blue") >> postgres_lookout
    scheduler_ingester >> Edge(color="blue") >> postgres_scheduler

    event_ingerster >> Edge(color="orange") >> redis_events
    job_ingester >> Edge(color="orange") >> redis_scheduler

    # the legacy scheduler talks to the jobs redis in both directions
    legacy_scheduler >> Edge(color="orange") >> redis_scheduler
    redis_scheduler >> Edge(color="orange") >> legacy_scheduler

    # the postgres scheduler talks to the scheduler and executor api
    postgres_scheduler >> Edge(color="blue") >> scheduler
    postgres_scheduler >> Edge(color="blue") >> executor_api

    # the scheduler talks to pulsar
    scheduler >> Edge(color="red") >> pulsar

    # the executor api talks to the executor on both clusters in both directions
    # the executor talks to the k8s api

    executor_api >> Edge(color="black") >> executor
    executor >> Edge(color="black") >> executor_api

    executor_api >> Edge(color="black") >> executor2
    executor2 >> Edge(color="black") >> executor_api

    executor >> Edge(color="blue") >> k8s_api
    k8s_api >> Edge(color="blue") >> executor

    executor2 >> Edge(color="blue") >> k8s_api2
    k8s_api2 >> Edge(color="blue") >> executor2
