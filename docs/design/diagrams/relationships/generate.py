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

armada_logo = "../files/armada.png"
pulsar_logo = "../files/pulsar.png"
browser_logo = "../files/browser.png"

with Diagram(
    name="Armada Systems Diagram",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    edge_attr=edge_attr,
    node_attr=node_attr,
    filename="out/armada_systems_diagram",
):
    pulsar = Custom("Pulsar", pulsar_logo)

    # Databases
    postgres_lookout = PostgreSQL("Postgres (Lookout)")
    postgres_scheduler = PostgreSQL("Postgres (Scheduler)")
    redis_events = Redis("Redis (Events)")

    # Components
    server = Custom("Server", armada_logo)
    client = Custom("Client", armada_logo)
    scheduler = Custom("Scheduler", armada_logo)

    # Lookout Parts
    lookout_api = Custom("Lookout API", armada_logo)
    lookoutUI = Custom("Lookout UI", armada_logo)

    # Ingesters
    lookout_ingester = Custom("Lookout Ingester", armada_logo)
    scheduler_ingester = Custom("Scheduler Ingester", armada_logo)
    event_ingerster = Custom("Event Ingester", armada_logo)

    with Cluster("Executor Cluster", graph_attr=cluster_attr_server):
        executor = Custom("Executor", armada_logo)
        k8s_api = API("K8s API")
        binoculars = Custom("Binoculars", armada_logo)

    with Cluster("Executor Cluster 2", graph_attr=cluster_attr_server):
        executor2 = Custom("Executor 2", armada_logo)
        k8s_api2 = API("K8s API 2")
        binoculars2 = Custom("Binoculars", armada_logo)

    # Relationships

    # client sends requests to the server
    client >> Edge(color="black") >> server

    # submit api talks to pulsar
    server >> Edge(color="red") >> pulsar

    # pulsar talks to each of the ingesters
    pulsar >> Edge(color="red") >> lookout_ingester
    pulsar >> Edge(color="red") >> scheduler_ingester
    pulsar >> Edge(color="red") >> event_ingerster

    # make postgres blue, redis orange
    # lookout and scheduler ingesters talk to postgres
    # the other ingesters talk to redis
    lookout_ingester >> Edge(color="blue") >> postgres_lookout
    scheduler_ingester >> Edge(color="blue") >> postgres_scheduler

    event_ingerster >> Edge(color="orange") >> redis_events

    # the postgres scheduler talks to the scheduler and executor api
    postgres_scheduler >> Edge(color="blue") >> scheduler

    # the scheduler talks to pulsar
    scheduler >> Edge(color="red") >> pulsar

    executor >> Edge(color="blue") >> k8s_api
    k8s_api >> Edge(color="blue") >> executor

    executor2 >> Edge(color="blue") >> k8s_api2
    k8s_api2 >> Edge(color="blue") >> executor2

    # The binoculars in every cluster talks to k8s, and
    # then talks directly to the lookout UI
    k8s_api >> Edge(color="blue") >> binoculars
    binoculars >> Edge(color="black") >> lookoutUI

    k8s_api2 >> Edge(color="blue") >> binoculars2
    binoculars2 >> Edge(color="black") >> lookoutUI

    # Lookout API gets its data from postgres
    # and passes it to the lookout UI
    postgres_lookout >> Edge(color="blue") >> lookout_api
    lookout_api >> Edge(color="black") >> lookoutUI

    # The scheduler talks to the executor api
    scheduler >> Edge(color="blue") >> executor
    scheduler >> Edge(color="blue") >> executor2

    # pulsar talks to the server
    pulsar >> Edge(color="red") >> server

    # redis events are given back to the server
    redis_events >> Edge(color="orange") >> server

    # and passed to the client
    server >> Edge(color="black") >> client
