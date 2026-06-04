# Kafka Cluster Linking — Failover & Disaster Recovery Runbook

**Platform:** Confluent Platform (On-Premise)  
**Prepared by:** Tim Alldataint  
**Date:** Juni 2026

---

## Arsitektur Overview

```
[MySQL Source DB]          [MySQL Target DB]
  10.100.13.154              10.100.13.153
       │                           ▲
       ▼                           │
[Debezium Source]          [JDBC Sink Connector]
 node1/2/3:8083             node-all-service:8083
       │                           │
       ▼                           │
[Kafka node1/2/3]          [Kafka node-all-service]
 SOURCE CLUSTER    ──────▶  DESTINATION CLUSTER
  OAUTHBEARER               PLAIN (broker)
                             OAUTHBEARER (connect)
       │                           │
[Schema Registry]          [Schema Registry]
 node1/2/3:8081      ──▶   node-all-service:8081
                     Schema Linking
```

### Info Cluster

| Komponen | Source (node1/2/3) | Destination (node-all-service) |
|---|---|---|
| Broker | node1/2/3.alldataint.com:9093 | node-all-service.alldataint.com:9093 |
| Schema Registry | node1/2/3.alldataint.com:8081 | node-all-service.alldataint.com:8081 |
| Kafka Connect | node1.alldataint.com:8083 | node-all-service.alldataint.com:8083 |
| MDS | node1/2/3.alldataint.com:8090 | node-all-service.alldataint.com:8090 |
| Broker SASL | OAUTHBEARER | PLAIN |
| Connect SASL | OAUTHBEARER | OAUTHBEARER |

### Topic yang Di-link

| Topic | Pattern | Filter |
|---|---|---|
| `db_ecommerce` | LITERAL | INCLUDE |
| `db_ecommerce.db_ecommerce.orders` | LITERAL | INCLUDE |
| `db_ecommerce.history-arif` | LITERAL | INCLUDE |
| `db_ecommerce.orders` | LITERAL | INCLUDE |

---

## Pre-Requisite

### 1. Copy Truststore ke node-all-service

```bash
scp root@node1.alldataint.com:/var/ssl/private/kafka_broker.truststore.jks \
    /var/ssl/private/kafka_broker_source.truststore.jks

chown cp-kafka:cp-kafka /var/ssl/private/kafka_broker_source.truststore.jks
```

### 2. Tingkatkan Timeout Schema Registry

Tambahkan ke `/etc/schema-registry/schema-registry.properties`:

```bash
cat >> /etc/schema-registry/schema-registry.properties << 'EOF'
kafkastore.timeout.ms=10000
kafkastore.write.max.retries=5
EOF

systemctl restart confluent-schema-registry
sleep 15
```

### 3. Buat File Config

**`destination.properties`** (admin ke node-all-service):
```properties
bootstrap.servers=node-all-service.alldataint.com:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="P@ssw0rd";
ssl.truststore.location=/var/ssl/private/kafka_broker.truststore.jks
ssl.truststore.password=confluenttruststorepass
```

**`source.properties`** (koneksi ke source + schema linking):
```properties
bootstrap.servers=node1.alldataint.com:9093,node2.alldataint.com:9093,node3.alldataint.com:9093
link.mode=DESTINATION
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username="admin" password="P@ssw0rd" metadataServerUrls="https://node1.alldataint.com:8090,https://node2.alldataint.com:8090,https://node3.alldataint.com:8090";
sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler
ssl.truststore.location=/var/ssl/private/kafka_broker.truststore.jks
ssl.truststore.password=confluenttruststorepass
mirror.start.offset.spec=earliest
consumer.offset.sync.enable=true
consumer.offset.sync.ms=5000

# Schema Linking — otomatis sync schema dari source ke destination SR
schema.registry.url=https://node1.alldataint.com:8081,https://node2.alldataint.com:8081
schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
schema.registry.ssl.truststore.password=confluenttruststorepass
schema.registry.basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=admin:P@ssw0rd
```

> ⚠️ Dengan menambahkan `schema.registry.*` di `source.properties`, schema dari source SR akan **otomatis di-sync** ke destination SR saat cluster link dibuat — tidak perlu manual import schema satu per satu.

**`topic-filters.json`**:
```json
{
  "topicFilters": [
    { "name": "db_ecommerce", "patternType": "LITERAL", "filterType": "INCLUDE" },
    { "name": "db_ecommerce.db_ecommerce.orders", "patternType": "LITERAL", "filterType": "INCLUDE" },
    { "name": "db_ecommerce.history-arif", "patternType": "LITERAL", "filterType": "INCLUDE" },
    { "name": "db_ecommerce.orders", "patternType": "LITERAL", "filterType": "INCLUDE" }
  ]
}
```

**`group-filters.json`**:
```json
{
  "groupFilters": [
    { "name": "connect-JdbcSinkConnector-orders-arif", "patternType": "LITERAL", "filterType": "INCLUDE" }
  ]
}
```

---

## Langkah 1 — Buat Cluster Link

### 1.1 Buat admin.properties untuk CLI tools

```bash
cat > /tmp/admin.properties << 'EOF'
bootstrap.servers=node-all-service.alldataint.com:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="P@ssw0rd";
ssl.truststore.location=/var/ssl/private/kafka_broker.truststore.jks
ssl.truststore.password=confluenttruststorepass
EOF
```

### 1.2 Buat Cluster Link

```bash
kafka-cluster-links --create \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --link migration_link \
  --command-config destination.properties \
  --config-file source.properties \
  --topic-filters-json-file topic-filters.json \
  --consumer-group-filters-json-file group-filters.json
```

### 1.3 Verifikasi Link ACTIVE

```bash
kafka-cluster-links --list \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties
```

> ✅ Pastikan status = `ACTIVE`

### 1.4 Verifikasi Mirror Topic Terbentuk

```bash
kafka-topics --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties \
  --list | grep db_ecommerce
```

### 1.5 Verifikasi Schema Sudah Ter-sync (Schema Linking)

```bash
curl -sk https://node-all-service.alldataint.com:8081/subjects \
  -u admin:P@ssw0rd
```

> ✅ Harusnya sudah ada `db_ecommerce.db_ecommerce.orders-key` dan `db_ecommerce.db_ecommerce.orders-value` secara otomatis.

---

## Langkah 2 — Setup Connector di node-all-service

### 2.1 Debezium MySQL Source Connector

> ⚠️ **Perbedaan penting dari config di node1/2/3:**
> - `database.server.id` → **HARUS BERBEDA** (gunakan `26052027` bukan `26052026`)
> - `database.history.kafka.bootstrap.servers` → ganti ke `node-all-service`
> - `database.history.consumer/producer sasl` → ganti ke **PLAIN**
> - `schema.registry.url` → ganti ke `node-all-service:8081`

Buat file `MySqlConnector-node-all-service.json`:
```json
{
  "name": "MySqlConnectorConnector_1-arif",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",
    "database.hostname": "10.100.13.154",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "P@ssw0rd",
    "database.server.name": "db_ecommerce",
    "database.server.id": "26052027",
    "snapshot.mode": "when_needed",
    "database.history.kafka.bootstrap.servers": "node-all-service.alldataint.com:9093",
    "database.history.kafka.topic": "db_ecommerce.history-arif",
    "table.include.list": "db_ecommerce.orders",
    "include.schema.changes": "true",
    "database.include.list": "db_ecommerce",
    "database.allowPublicKeyRetrieval": "true",
    "database.connectionTimeZone": "Asia/Jakarta",
    "topic.creation.default.partitions": "1",
    "topic.creation.default.replication.factor": "-1",
    "topic.creation.default.cleanup.policy": "compact",
    "database.history.consumer.security.protocol": "SASL_SSL",
    "database.history.consumer.sasl.mechanism": "PLAIN",
    "database.history.consumer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"P@ssw0rd\";",
    "database.history.consumer.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "database.history.consumer.ssl.truststore.password": "confluenttruststorepass",
    "database.history.producer.security.protocol": "SASL_SSL",
    "database.history.producer.sasl.mechanism": "PLAIN",
    "database.history.producer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"P@ssw0rd\";",
    "database.history.producer.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "database.history.producer.ssl.truststore.password": "confluenttruststorepass",
    "key.converter.schema.registry.url": "https://node-all-service.alldataint.com:8081",
    "value.converter.schema.registry.url": "https://node-all-service.alldataint.com:8081",
    "key.converter.schema.registry.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "key.converter.schema.registry.ssl.truststore.password": "confluenttruststorepass",
    "key.converter.schema.registry.ssl.keystore.location": "/var/ssl/private/kafka_connect.keystore.jks",
    "key.converter.schema.registry.ssl.keystore.password": "confluentkeystorestorepass",
    "key.converter.schema.registry.ssl.key.password": "confluentkeystorestorepass",
    "value.converter.schema.registry.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "value.converter.schema.registry.ssl.truststore.password": "confluenttruststorepass",
    "value.converter.schema.registry.ssl.keystore.location": "/var/ssl/private/kafka_connect.keystore.jks",
    "value.converter.schema.registry.ssl.keystore.password": "confluentkeystorestorepass",
    "value.converter.schema.registry.ssl.key.password": "confluentkeystorestorepass",
    "principal.service.name": "admin",
    "principal.service.password": "P@ssw0rd"
  }
}
```

Register connector:
```bash
curl -sk -X POST https://node-all-service.alldataint.com:8083/connectors \
  -u admin:P@ssw0rd \
  -H "Content-Type: application/json" \
  -d @MySqlConnector-node-all-service.json
```

### 2.2 JDBC Sink Connector

> ⚠️ **Perbedaan penting dari config di node1/2/3:**
> - `consumer.override.sasl.mechanism` → **OAUTHBEARER** (worker Connect di node-all-service pakai OAUTHBEARER)
> - `consumer.override.sasl.jaas.config` → metadataServerUrls ke `node-all-service:8090`
> - Tambah `consumer.override.sasl.login.callback.handler.class`
> - `schema.registry.url` → ganti ke `node-all-service:8081`

Buat file `JdbcSinkConnector-node-all-service.json`:
```json
{
  "name": "JdbcSinkConnector-orders-arif",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "transforms": "convertCreatedAt, convertUpdatedAt",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "topics": "db_ecommerce.db_ecommerce.orders",
    "transforms.convertCreatedAt.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.convertCreatedAt.target.type": "Timestamp",
    "transforms.convertCreatedAt.field": "created_at",
    "transforms.convertUpdatedAt.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.convertUpdatedAt.target.type": "Timestamp",
    "transforms.convertUpdatedAt.field": "updated_at",
    "connection.url": "jdbc:mysql://10.100.13.153:3306/db_ecommerce?serverTimezone=Asia/Jakarta&allowPublicKeyRetrieval=true&useSSL=false",
    "connection.user": "kafka_sink",
    "connection.password": "P@ssw0rd",
    "insert.mode": "upsert",
    "delete.enabled": "true",
    "table.name.format": "orders",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "auto.create": "false",
    "auto.evolve": "false",
    "max.retries": "10",
    "retry.backoff.ms": "5000",
    "consumer.override.security.protocol": "SASL_SSL",
    "consumer.override.sasl.mechanism": "OAUTHBEARER",
    "consumer.override.sasl.jaas.config": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username=\"admin\" password=\"P@ssw0rd\" metadataServerUrls=\"https://node-all-service.alldataint.com:8090\";",
    "consumer.override.sasl.login.callback.handler.class": "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler",
    "consumer.override.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "consumer.override.ssl.truststore.password": "confluenttruststorepass",
    "key.converter.schema.registry.url": "https://node-all-service.alldataint.com:8081",
    "value.converter.schema.registry.url": "https://node-all-service.alldataint.com:8081",
    "key.converter.schema.registry.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "key.converter.schema.registry.ssl.truststore.password": "confluenttruststorepass",
    "key.converter.schema.registry.ssl.keystore.location": "/var/ssl/private/kafka_connect.keystore.jks",
    "key.converter.schema.registry.ssl.keystore.password": "confluentkeystorestorepass",
    "key.converter.schema.registry.ssl.key.password": "confluentkeystorestorepass",
    "value.converter.schema.registry.ssl.truststore.location": "/var/ssl/private/kafka_connect.truststore.jks",
    "value.converter.schema.registry.ssl.truststore.password": "confluenttruststorepass",
    "value.converter.schema.registry.ssl.keystore.location": "/var/ssl/private/kafka_connect.keystore.jks",
    "value.converter.schema.registry.ssl.keystore.password": "confluentkeystorestorepass",
    "value.converter.schema.registry.ssl.key.password": "confluentkeystorestorepass",
    "principal.service.name": "admin",
    "principal.service.password": "P@ssw0rd"
  }
}
```

Register connector:
```bash
curl -sk -X POST https://node-all-service.alldataint.com:8083/connectors \
  -u admin:P@ssw0rd \
  -H "Content-Type: application/json" \
  -d @JdbcSinkConnector-node-all-service.json
```

### 2.3 Verifikasi Kedua Connector RUNNING

```bash
# Cek Debezium Source
curl -sk https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool

# Cek JDBC Sink
curl -sk https://node-all-service.alldataint.com:8083/connectors/JdbcSinkConnector-orders-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool
```

> ✅ State connector dan task harus `RUNNING`

---

## Langkah 3 — Failover (Skenario DR)

> ⚠️ Lakukan langkah ini ketika source cluster (node1/2/3) akan dimatikan atau terjadi disaster.

### 3.1 Pause Debezium di Source Cluster

Harus dilakukan **sebelum failover** untuk menghindari konflik `server_id`:

```bash
curl -sk -X PUT https://node1.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/pause \
  -u admin:P@ssw0rd

# Verifikasi sudah PAUSED
curl -sk https://node1.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool
```

### 3.2 Tunggu Consumer Group LAG = 0

```bash
kafka-consumer-groups \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties \
  --group connect-JdbcSinkConnector-orders-arif \
  --describe
```

> ⚠️ Lanjutkan **hanya jika** kolom `LAG = 0`

### 3.3 Promote Mirror Topics (Failover)

```bash
kafka-mirrors --failover \
  --topics db_ecommerce,db_ecommerce.db_ecommerce.orders,db_ecommerce.history-arif,db_ecommerce.orders \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties
```

### 3.4 Verifikasi Topic Sudah Writable

```bash
# Output harus kosong (No mirror topics found)
kafka-mirrors --describe \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties

# Describe topic untuk konfirmasi tidak ada flag mirrorTopic
kafka-topics --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties \
  --describe --topic db_ecommerce.db_ecommerce.orders
```

---

## Langkah 4 — Aktivasi Pipeline di Destination

### 4.1 Restart Debezium di node-all-service

```bash
# Restart task
curl -sk -X POST \
  https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/tasks/0/restart \
  -u admin:P@ssw0rd

# Atau restart full connector jika task masih FAILED
curl -sk -X POST \
  'https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/restart?includeTasks=true&onlyFailed=true' \
  -u admin:P@ssw0rd

sleep 10

# Verifikasi RUNNING
curl -sk https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool
```

### 4.2 Test Inject Data ke MySQL Source

```bash
mysql -h 10.100.13.154 -u debezium -p'P@ssw0rd' \
  -e "INSERT INTO db_ecommerce.orders (customer_id, product, total, status, created_at, updated_at)
      VALUES (99, 'test-failover', 999.99, 'pending', NOW(), NOW());"
```

### 4.3 Verifikasi Offset Topic Naik

```bash
kafka-get-offsets \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties \
  --topic db_ecommerce.db_ecommerce.orders
```

> ✅ Angka offset harus bertambah setelah inject data

### 4.4 Verifikasi JDBC Sink LAG = 0

```bash
kafka-consumer-groups \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config /tmp/admin.properties \
  --group connect-JdbcSinkConnector-orders-arif \
  --describe
```

### 4.5 Verifikasi Data di MySQL Target

```bash
mysql -h 10.100.13.153 -u kafka_sink -p'P@ssw0rd' \
  -e "SELECT COUNT(*) FROM db_ecommerce.orders;"
```

---

## Schema Linking (Cara yang Benar)

Schema Linking memungkinkan schema Registry di destination **otomatis sync** dari source tanpa perlu manual import.

### Setup Schema Linking via Cluster Link

Cara paling mudah adalah menyertakan config schema registry di `source.properties` **sebelum** membuat cluster link:

```properties
# Tambahkan ini ke source.properties
schema.registry.url=https://node1.alldataint.com:8081,https://node2.alldataint.com:8081
schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
schema.registry.ssl.truststore.password=confluenttruststorepass
schema.registry.basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=admin:P@ssw0rd
```

### Setup Schema Linking via schema-exporter (Alternatif)

Jika cluster link sudah terlanjur dibuat tanpa schema linking, gunakan `schema-exporter`:

```bash
# Buat config source SR
cat > /tmp/schema-link-source.properties << 'EOF'
schema.registry.url=https://node1.alldataint.com:8081,https://node2.alldataint.com:8081
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=admin:P@ssw0rd
schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
schema.registry.ssl.truststore.password=confluenttruststorepass
EOF

# Buat schema exporter
schema-exporter --create \
  --name schema-link-migration \
  --config-file /tmp/schema-link-source.properties \
  --schema-registry-url https://node-all-service.alldataint.com:8081 \
  --basic-auth-credentials-source USER_INFO \
  --basic-auth-user-info admin:P@ssw0rd \
  --schema-registry-ssl.truststore.location /var/ssl/private/kafka_connect.truststore.jks \
  --schema-registry-ssl.truststore.password confluenttruststorepass \
  --subject-format ":.*:" \
  --context-type NONE

# Verifikasi exporter running
schema-exporter --list \
  --schema-registry-url https://node-all-service.alldataint.com:8081 \
  --basic-auth-credentials-source USER_INFO \
  --basic-auth-user-info admin:P@ssw0rd

# Cek schema sudah masuk
curl -sk https://node-all-service.alldataint.com:8081/subjects \
  -u admin:P@ssw0rd
```

### Manual Import Schema (Fallback)

Jika schema linking tidak tersedia, import manual:

```bash
# 1. Ambil schema dari source
KEY_SCHEMA=$(curl -sk \
  https://node1.alldataint.com:8081/subjects/db_ecommerce.db_ecommerce.orders-key/versions/latest \
  -u admin:P@ssw0rd | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['schema'])")

VALUE_SCHEMA=$(curl -sk \
  https://node1.alldataint.com:8081/subjects/db_ecommerce.db_ecommerce.orders-value/versions/latest \
  -u admin:P@ssw0rd | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['schema'])")

# 2. Register ke destination
curl -sk -X POST \
  https://node-all-service.alldataint.com:8081/subjects/db_ecommerce.db_ecommerce.orders-key/versions \
  -u admin:P@ssw0rd \
  -H "Content-Type: application/json" \
  -d "{\"schema\": $(echo $KEY_SCHEMA | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}"

curl -sk -X POST \
  https://node-all-service.alldataint.com:8081/subjects/db_ecommerce.db_ecommerce.orders-value/versions \
  -u admin:P@ssw0rd \
  -H "Content-Type: application/json" \
  -d "{\"schema\": $(echo $VALUE_SCHEMA | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}"
```

---

## Troubleshooting

| Error | Penyebab | Solusi |
|---|---|---|
| `A replica with same server_id has connected` | `database.server.id` konflik dengan Debezium di source | Pause Debezium di source ATAU ganti `server.id` ke angka berbeda |
| `Unexpected SASL mechanism: PLAIN` | Worker Connect pakai OAUTHBEARER tapi `consumer.override` pakai PLAIN | Ganti `consumer.override.sasl.mechanism` ke OAUTHBEARER |
| `Register operation timed out (50002)` | `kafkastore.timeout.ms` terlalu kecil (default 500ms) | Tambah `kafkastore.timeout.ms=10000` di SR properties lalu restart |
| `Connection refused` ke Schema Registry | SR baru di-restart, belum fully up | Restart task: `POST /connectors/{name}/tasks/0/restart` |
| `NoSuchFileException` (truststore) | File truststore belum ada di node destination | Copy truststore dari source ke destination |
| Topic masih read-only setelah failover | `kafka-mirrors --failover` belum dijalankan | Jalankan `kafka-mirrors --failover` untuk semua topic |
| Data tidak masuk ke topic setelah inject | Debezium di source masih jalan, konflik `server_id` | Pause Debezium di source, restart connector di destination |
| SR subjects kosong `[]` setelah link dibuat | Schema linking tidak dikonfigurasi di `source.properties` | Tambahkan `schema.registry.*` config atau gunakan `schema-exporter` |

---

## Checklist Failover

### Pre-Failover
- [ ] Truststore source sudah di-copy ke node-all-service
- [ ] `kafkastore.timeout.ms=10000` sudah ditambahkan ke SR config
- [ ] Cluster link sudah `ACTIVE`
- [ ] Schema sudah ter-sync di node-all-service SR (via Schema Linking)
- [ ] Kedua connector (Source + Sink) sudah `RUNNING` di node-all-service
- [ ] Consumer group `LAG = 0`

### Saat Failover
- [ ] Pause Debezium di source cluster (node1/2/3)
- [ ] Verifikasi LAG masih `= 0`
- [ ] Jalankan `kafka-mirrors --failover` untuk semua topic
- [ ] Verifikasi `kafka-mirrors --describe` output kosong

### Post-Failover
- [ ] Restart Debezium connector di node-all-service
- [ ] Inject test data ke MySQL source
- [ ] Verifikasi offset topic naik
- [ ] Verifikasi LAG JDBC Sink `= 0`
- [ ] Verifikasi `COUNT(*)` di MySQL target bertambah

---

## Referensi

- [Confluent Platform Cluster Linking](https://docs.confluent.io/platform/current/multi-cloud/cluster-linking/)
- [Confluent Schema Linking](https://docs.confluent.io/platform/current/schema-registry/schema-linking.html)
- [Debezium MySQL Connector](https://debezium.io/documentation/reference/connectors/mysql.html)
- [JDBC Sink Connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/)
- [GitHub Repo](https://github.com/Ariifprastiyo/cluster-linking)

---

*© 2026 Alldataint — Internal Documentation*
