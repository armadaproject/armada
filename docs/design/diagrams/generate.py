from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.k8s.controlplane import API
from diagrams.custom import Custom
from diagrams.onprem.client import Client

graph_attr = {
    "concentrate": "false",
    "splines": "ortho",
    "pad": "0.5",
    "nodesep": "0.60",
    "ranksep": "1.5",
}

edge_attr = {
    "minlen": "1",
}

cluster_attr_common = {
    "margin": "20",
}

cluster_attr_armada = {
    "labelloc": "b",
    "margin": "1",
}
cluster_attr_armada.update(cluster_attr_common)


armada_logo = "./images/armada.png"
pulsar_logo = "./images/pulsar.png"
browser_logo = "./images/browser.png"

with Diagram(
    name="Armada Systems Diagram",
    show=False,
    direction="BT",
    graph_attr=graph_attr,
    edge_attr=edge_attr,
    filename="armada-system",
):

    with Cluster("External Services", graph_attr=cluster_attr_common):
        pulsar = Custom("Pulsar", pulsar_logo)
        postgres = PostgreSQL("Postgres")
        redis = Redis("Redis")

    armada_clients = Client("Armada Clients")

    with Cluster("Armada Server Components", graph_attr=cluster_attr_armada):

        with Cluster(
            "Armada Server Cluster", graph_attr=cluster_attr_armada
        ) as services:

            lookout_api = Custom("Lookout API", armada_logo)

            lookout_ingester = Custom("Lookout Ingester", armada_logo)
            event_ingester = Custom("Event Ingester", armada_logo)

            armada_server = Custom("Armada Server", armada_logo)

        with Cluster(
            "Armada Executor Cluster", graph_attr=cluster_attr_armada
        ) as workers:

            armada_executor = Custom("Armada Executor", armada_logo)
            binoculars = Custom("Binoculars", armada_logo)

    k8s_api = API("K8s API")

    lookout_browser = Custom("Lookout Browser", browser_logo)

    # Relations
    # use edges to label the direction of the data flow

    # clients all send requests to server both ways
    armada_clients >> Edge(color="black") >> armada_server

    # executor sends its data to the server
    # the server then responds with jobs to execute
    armada_server >> Edge(color="black") >> armada_executor
    armada_executor >> Edge(color="black") >> armada_server

    # event ingester recieves from pulsar and sends to redis
    pulsar >> Edge(color="red") >> event_ingester
    event_ingester >> Edge(color="orange") >> redis

    # lookout ingester recieves from pulsar and sends to postgres
    pulsar >> Edge(color="red") >> lookout_ingester
    lookout_ingester >> Edge(color="blue") >> postgres

    # lookout broswer sends requests to the lookout api
    lookout_api >> Edge(color="green") >> lookout_browser

    # binoculars gets data from k8s, and is used by the browser
    k8s_api >> Edge(color="blue") >> binoculars
    binoculars >> Edge(color="green") >> lookout_browser

    # executor gets data from k8s
    k8s_api >> Edge(color="blue") >> armada_executor

    # lookout browser sends requests to armada server
    lookout_browser >> Edge(color="green") >> armada_server

    # armada server gets dats from pulsar and redis
    pulsar >> Edge(color="red") >> armada_server
    redis >> Edge(color="orange") >> armada_server

    # lookout api gets data from postgres
    postgres >> Edge(color="blue") >> lookout_api
