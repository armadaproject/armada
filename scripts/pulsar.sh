# Create the partitioned topic used by Armada.
# pulsar-admin errors if the Pulsar server hasn't started up yet.
sleep 10

docker exec -i pulsar bin/pulsar-admin tenants create armada
docker exec -i pulsar bin/pulsar-admin namespaces create armada/armada
docker exec -i pulsar bin/pulsar-admin topics delete-partitioned-topic persistent://armada/armada/events -f || true
docker exec -i pulsar bin/pulsar-admin topics create-partitioned-topic persistent://armada/armada/events -p 2
docker exec -i pulsar bin/pulsar-admin topics create-partitioned-topic persistent://public/default/events -p 2

# Disable topic auto-creation to ensure an error is thrown on using the wrong topic
# (Pulsar automatically created the public tenant and default namespace).
docker exec -i pulsar bin/pulsar-admin namespaces set-auto-topic-creation public/default --disable
docker exec -i pulsar bin/pulsar-admin namespaces set-auto-topic-creation armada/armada --disable
