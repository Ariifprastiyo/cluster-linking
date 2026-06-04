# cluster-linking

# Cluster Linking — node-all-service → node1/2/3 (Destination-Initiated)

Replicates topics from `node-all-service.alldataint.com` (source) to `node1/2/3.alldataint.com` (destination).

The `kafka-cluster-links --create` command runs on the **destination** (`node1.alldataint.com`).

| File                     | CLI flag           | Purpose                                                         |
| ------------------------ | ------------------ | --------------------------------------------------------------- |
| `destination.properties` | `--command-config` | Admin auth for the destination cluster (where the command runs) |
| `source.properties`      | `--config-file`    | Link config: how to reach the source + link behavior            |

---

## Step 1 — Copy source truststore to ALL destination brokers

The truststore path in `source.properties` is loaded by the **broker process**, not the CLI. The controller or partition leader can be on any destination node — if the file only exists on node1, nodes 2 and 3 will throw `NoSuchFileException` when the broker tries to validate the link.

Copy the source cluster truststore from node-all-service to every destination broker:

```bash
# On node-all-service — copy to all destination brokers
scp /var/ssl/private/kafka_broker.truststore.jks node1.alldataint.com:/var/ssl/private/kafka_broker_source.truststore.jks
scp /var/ssl/private/kafka_broker.truststore.jks node2.alldataint.com:/var/ssl/private/kafka_broker_source.truststore.jks
scp /var/ssl/private/kafka_broker.truststore.jks node3.alldataint.com:/var/ssl/private/kafka_broker_source.truststore.jks
```

Fix ownership on each destination node so the broker process (runs as `cp-kafka`) can read it:

```bash
for node in node1 node2 node3; do
  ssh $node.alldataint.com "chown cp-kafka:confluent /var/ssl/private/kafka_broker_source.truststore.jks && chmod 640 /var/ssl/private/kafka_broker_source.truststore.jks"
done
```

Verify the file is present on all nodes:

```bash
for node in node1 node2 node3; do
  echo "$node:"
  ssh $node.alldataint.com "ls -lh /var/ssl/private/kafka_broker_source.truststore.jks"
done
```

---

## Step 2 — Destination admin auth (`destination.properties`)

```properties
# Admin auth for the destination cluster only.
# link.mode and connection.mode do NOT belong here — they belong in source.properties (--config-file).
bootstrap.servers=node1.alldataint.com:9093,node2.alldataint.com:9093,node3.alldataint.com:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="P@ssw0rd";
ssl.truststore.location=/var/ssl/private/kafka_broker.truststore.jks
ssl.truststore.password=confluenttruststorepass
```

---

## Step 3 — Source cluster link config (`source.properties`)

```properties
# Link config: how the destination connects to the source + link behavior.
# link.mode=DESTINATION is correct here (destination-initiated).
# ssl.truststore.location must exist on ALL destination broker nodes (see Step 1).
bootstrap.servers=node-all-service.alldataint.com:9093
link.mode=DESTINATION
# No need for connection.mode (default = INBOUND for destination-initiated)
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="P@ssw0rd";
ssl.truststore.location=/var/ssl/private/kafka_broker_source.truststore.jks
ssl.truststore.password=confluenttruststorepass
mirror.start.offset.spec=latest
#auto.create.mirror.topics.enable=true
#consumer.offset.sync.enable=true
```

---

## Step 4 — Topic filter file (`topic-filters.json`)

Mirror all topics:

```json
{
  "topicFilters": [
    {
      "name": "*",
      "patternType": "LITERAL",
      "filterType": "INCLUDE"
    }
  ]
}
```

Mirror a single topic:

```json
{
  "topicFilters": [
    {
      "name": "your_topic_name",
      "patternType": "LITERAL",
      "filterType": "INCLUDE"
    }
  ]
}
```

---

## Step 5 — Consumer group filter file (`group-filters.json`)

```json
{
  "groupFilters": [
    {
      "name": "*",
      "patternType": "LITERAL",
      "filterType": "INCLUDE"
    }
  ]
}
```

---

## Step 6 — Create the link (run on destination: node1)

```bash
kafka-cluster-links --create \
  --bootstrap-server node1.alldataint.com:9093,node2.alldataint.com:9093,node3.alldataint.com:9093 \
  --link migration_link \
  --command-config destination.properties \
  --config-file source.properties \
  --topic-filters-json-file topic-filters.json \
  --consumer-group-filters-json-file group-filters.json
```

Expected output:

```
Cluster link 'migration_link' creation successfully completed.
```

---

## Step 7 — Verify

```bash
kafka-cluster-links --list \
  --bootstrap-server node1.alldataint.com:9093 \
  --command-config destination.properties
```

Expected: `link state: 'ACTIVE'`

Check mirror topics were created on destination:

```bash
kafka-topics --bootstrap-server node1.alldataint.com:9093 \
  --command-config destination.properties \
  --list
```

---

## Troubleshooting

### `NoSuchFileException: /var/ssl/private/kafka_broker_source.truststore.jks`

The file exists on the node where you ran the CLI but not on the node where the broker controller is running. The path in `source.properties` is resolved by the **broker process** on whichever node holds the controller or partition leader — not by the CLI.

Fix: copy the truststore to all destination broker nodes (Step 1).

### `Unable to create client using provided properties`

Usually means one of:

- Wrong truststore password in `source.properties`
- Source broker not reachable from destination nodes (firewall / DNS)
- Truststore does not contain the source broker's CA cert

Test connectivity from a destination node:

```bash
openssl s_client -connect node-all-service.alldataint.com:9093 \
  -CAfile /var/ssl/private/kafka_broker_source.truststore.jks 2>&1 | head -20
```

# Schema Lingking

Setelah cluster lingking berhasil maka lakukan schema lingking agar bisa produce 

## Stelah itu jalankan command ini Untuk disaster di source  cluster (cluster mati mendadak)  jalankan 
```
kafka-mirrors --failover \
   --topic-filters-json-file topic-filters.json \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties
```
