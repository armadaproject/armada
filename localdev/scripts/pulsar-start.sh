# Start pulsar in background
/pulsar/bin/pulsar standalone &

# Create the partitioned topic used by Armada.
# pulsar-admin errors if the Pulsar server hasn't started up yet.
sleep 10 

/pulsar/bin/pulsar-admin tenants create armada
/pulsar/bin/pulsar-admin namespaces create armada/armada
/pulsar/bin/pulsar-admin topics delete-partitioned-topic persistent://armada/armada/events -f || true
/pulsar/bin/pulsar-admin topics create-partitioned-topic persistent://armada/armada/events -p 2

# Disable topic auto-creation to ensure an error is thrown on using the wrong topic
# (Pulsar automatically created the public tenant and default namespace).
/pulsar/bin/pulsar-admin namespaces set-auto-topic-creation public/default --disable
/pulsar/bin/pulsar-admin namespaces set-auto-topic-creation armada/armada --disable

# Keep the container running until a signal is received
sleep infinity
