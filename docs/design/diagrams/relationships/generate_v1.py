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
    name="Armada V1 System",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    edge_attr=edge_attr,
    node_attr=node_attr,
    # filename="out/armada_systems_diagram",
):
    pulsar = Custom("Pulsar", pulsar_logo)


    # Databases
    postgres_lookout = PostgreSQL("Postgres (Lookout)")
    redis_events = Redis("Redis (Events)")
    redis_scheduler = Redis("Redis Scheduler")

    # Components
    server = Custom("Server", armada_logo)
    executorAPI = Custom("Executor API", armada_logo)

    # Ingesters
    lookout_ingester = Custom("Lookout Ingester", armada_logo)

    with Cluster("Executor Cluster", graph_attr=cluster_attr_server):
        executor = Custom("Executor", armada_logo)
        k8s_api = API("K8s API")

    with Cluster("Executor Cluster 2", graph_attr=cluster_attr_server):
        executor2 = Custom("Executor 2", armada_logo)
        k8s_api2 = API("K8s API 2")

    # Relationships

    # Lookout ingester talks to each other Postgres lookout
    lookout_ingester >> Edge(color="blue") >> postgres_lookout

    # Pulsar talks to lookout_ingester
    pulsar >> Edge(color="red") >> lookout_ingester

    # Pulsar talks to server
    pulsar >> Edge(color="red") >> server

    # Pulsar talks to executorAPI
    pulsar >> Edge(color="red") >> executorAPI

    # server talks to redis_events
    server >> Edge(color="orange") >> redis_events

    # server talks to redis_scheduler
    server >> Edge(color="orange") >> redis_scheduler

    # executorAPI talks to redis_scheduler
    executorAPI >> Edge(color="orange") >> redis_scheduler

    # in Executor Cluster
    executor >> Edge(color="blue") >> k8s_api
    k8s_api >> Edge(color="blue") >> executor

    # in Executor Cluster 2
    executor2 >> Edge(color="blue") >> k8s_api2
    k8s_api2 >> Edge(color="blue") >> executor2

    # Executor talks to executor API
    executor >> Edge(color="black") >> executorAPI
    executorAPI >> Edge(color="black") >> executor

    # Executor 2 talks to executor API
    executor2 >> Edge(color="black") >> executorAPI
    executorAPI >> Edge(color="black") >> executor2





