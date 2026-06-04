# Kafka Cluster Linking — Failover & Disaster Recovery Runbook

**Platform:** Confluent Platform (On-Premise)  
**Prepared by:** Tim Alldataint  
**Date:** Juni 2026

---


### Info Cluster

| Komponen | Source (node1/2/3) | Destination (node-all-service) |
|---|---|---|
| Broker | node1/2/3.alldataint.com:9093 | node-all-service.alldataint.com:9093 |
| Schema Registry | node1/2/3.alldataint.com:8081 | node-all-service.alldataint.com:8081 |
| Kafka Connect | node1.alldataint.com:8083 | node-all-service.alldataint.com:8083 |
| MDS | node1/2/3.alldataint.com:8090 | node-all-service.alldataint.com:8090 |
| Broker SASL | OAUTHBEARER | OAUTHBEARER |
| Connect Worker SASL | OAUTHBEARER | OAUTHBEARER |

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

### 2. Buat File Konfigurasi

**`destination.properties`** — digunakan untuk semua perintah CLI dan sebagai admin config ke node-all-service:
```properties
bootstrap.servers=node-all-service.alldataint.com:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="P@ssw0rd";
ssl.truststore.location=/var/ssl/private/kafka_broker.truststore.jks
ssl.truststore.password=confluenttruststorepass
```

**`source.properties`** — koneksi dari destination ke source cluster:
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
```

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

### 1.1 Buat Cluster Link di node-all-service

```bash
kafka-cluster-links --create \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --link migration_link \
  --command-config destination.properties \
  --config-file source.properties \
  --topic-filters-json-file topic-filters.json \
  --consumer-group-filters-json-file group-filters.json
```

### 1.2 Verifikasi Link ACTIVE

```bash
kafka-cluster-links --list \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties
```

> ✅ Pastikan status = `ACTIVE`

### 1.3 Verifikasi Mirror Topic Terbentuk

```bash
kafka-topics --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties \
  --list | grep db_ecommerce
```

### 1.4 Verifikasi Offset Consumer Group Ter-sync

```bash
kafka-consumer-groups \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties \
  --group connect-JdbcSinkConnector-orders-arif \
  --describe
```

> ⚠️ Tunggu kolom `LAG = 0` sebelum melakukan failover.

---

## Langkah 2 — Setup Schema Linking

Schema Linking adalah fitur Confluent Platform yang men-sync schema dari source Schema Registry ke destination secara otomatis dan continuous. Dikonfigurasi via `schema-exporter` — **terpisah dari cluster link**.

### 2.1 Buat Config Source Schema Registry

```bash
cat > /tmp/schema-link-source.properties << 'EOF'
schema.registry.url=https://node1.alldataint.com:8081,https://node2.alldataint.com:8081
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=admin:P@ssw0rd
schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
schema.registry.ssl.truststore.password=confluenttruststorepass
EOF
```

### 2.2 Buat Schema Exporter

```bash
schema-exporter --create \
  --name schema-link-ecommerce \
  --config-file /tmp/schema-link-source.properties \
  --schema-registry-url https://node-all-service.alldataint.com:8081 \
  --basic-auth-credentials-source USER_INFO \
  --basic-auth-user-info admin:P@ssw0rd \
  --schema-registry-ssl.truststore.location /var/ssl/private/kafka_connect.truststore.jks \
  --schema-registry-ssl.truststore.password confluenttruststorepass \
  --subject-format ":.*:" \
  --context-type NONE
```

### 2.3 Verifikasi Exporter Running

```bash
schema-exporter --list \
  --schema-registry-url https://node-all-service.alldataint.com:8081 \
  --basic-auth-credentials-source USER_INFO \
  --basic-auth-user-info admin:P@ssw0rd
```

### 2.4 Verifikasi Schema Sudah Ter-sync

```bash
curl -sk https://node-all-service.alldataint.com:8081/subjects \
  -u admin:P@ssw0rd
```

> ✅ Harus sudah ada `db_ecommerce.db_ecommerce.orders-key` dan `db_ecommerce.db_ecommerce.orders-value` secara otomatis.

---

## Langkah 3 — Setup Connector di node-all-service

### 3.1 Debezium MySQL Source Connector

> ⚠️ **Perbedaan penting dari config di node1/2/3:**
> - `database.server.id` → **HARUS BERBEDA** (gunakan `26052027`, bukan `26052026`) untuk menghindari konflik MySQL replica
> - `database.history.kafka.bootstrap.servers` → ganti ke `node-all-service`
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

### 3.2 JDBC Sink Connector

> ⚠️ **Perbedaan penting dari config di node1/2/3:**
> - `consumer.override.sasl.mechanism` → **OAUTHBEARER** (worker Kafka Connect di node-all-service pakai OAUTHBEARER)
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

### 3.3 Verifikasi Kedua Connector RUNNING

```bash
# Cek Debezium Source
curl -sk https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool

# Cek JDBC Sink
curl -sk https://node-all-service.alldataint.com:8083/connectors/JdbcSinkConnector-orders-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool
```

> ✅ State connector dan semua task harus `RUNNING`

> ⚠️ Jika task FAILED, cek log: `tail -50 /data/log/kafka-connect/connect.log`

---

## Langkah 4 — Failover (Skenario DR)

> ⚠️ Lakukan langkah ini ketika source cluster (node1/2/3) akan dimatikan atau terjadi disaster.

### 4.1 Pause Debezium di Source Cluster

Harus dilakukan **sebelum failover** untuk menghindari konflik `server_id`:

```bash
curl -sk -X PUT https://node1.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/pause \
  -u admin:P@ssw0rd

# Verifikasi sudah PAUSED
curl -sk https://node1.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool
```

### 4.2 Tunggu Consumer Group LAG = 0

```bash
kafka-consumer-groups \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties \
  --group connect-JdbcSinkConnector-orders-arif \
  --describe
```

> ⚠️ Lanjutkan **hanya jika** kolom `LAG = 0`

### 4.3 Promote Mirror Topics (Failover)

```bash
kafka-mirrors --failover \
  --topics db_ecommerce,db_ecommerce.db_ecommerce.orders,db_ecommerce.history-arif,db_ecommerce.orders \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties
```

### 4.4 Verifikasi Topic Sudah Writable

```bash
# Output harus kosong — "No mirror topics found"
kafka-mirrors --describe \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties

# Describe topic untuk konfirmasi tidak ada flag mirrorTopic
kafka-topics --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties \
  --describe --topic db_ecommerce.db_ecommerce.orders
```

---

## Langkah 5 — Aktivasi Pipeline di Destination

### 5.1 Restart Debezium di node-all-service

```bash
# Restart task
curl -sk -X POST \
  https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/tasks/0/restart \
  -u admin:P@ssw0rd

# Tunggu startup
sleep 10

# Verifikasi RUNNING
curl -sk https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/status \
  -u admin:P@ssw0rd | python3 -m json.tool
```

> Jika task masih FAILED:
> ```bash
> curl -sk -X POST \
>   'https://node-all-service.alldataint.com:8083/connectors/MySqlConnectorConnector_1-arif/restart?includeTasks=true&onlyFailed=true' \
>   -u admin:P@ssw0rd
> ```

### 5.2 Test Inject Data ke MySQL Source

```bash
 python3 inject_data_db_ecommerce.py
```

### 5.3 Verifikasi Offset Topic Naik

```bash
kafka-get-offsets \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties \
  --topic db_ecommerce.db_ecommerce.orders
```

> ✅ Angka offset harus bertambah setelah inject data

### 5.4 Verifikasi JDBC Sink LAG = 0

```bash
kafka-consumer-groups \
  --bootstrap-server node-all-service.alldataint.com:9093 \
  --command-config destination.properties \
  --group connect-JdbcSinkConnector-orders-arif \
  --describe
```

### 5.5 Verifikasi Data di MySQL Target

```bash
mysql -h 10.100.13.153 -u kafka_sink -p'P@ssw0rd' \
  -e "SELECT COUNT(*) FROM db_ecommerce.orders;"
```

> ✅ Pipeline CDC lengkap sudah berjalan: **MySQL Source → Debezium → Kafka → JDBC Sink → MySQL Target**

---

## Troubleshooting

| Error | Penyebab | Solusi |
|---|---|---|
| `A replica with same server_id has connected` | `database.server.id` konflik dengan Debezium di source | Pause Debezium di source ATAU ganti `server.id` ke angka berbeda |
| `Unexpected SASL mechanism: PLAIN` | Worker Connect pakai OAUTHBEARER tapi `consumer.override` pakai PLAIN | Ganti `consumer.override.sasl.mechanism` ke OAUTHBEARER |
| `Connection refused` ke Schema Registry | Connector restart saat SR belum fully up | Restart task: `POST /connectors/{name}/tasks/0/restart` |
| `NoSuchFileException` truststore | File truststore belum ada di node destination | Copy truststore dari source ke destination |
| Topic masih read-only setelah failover | `kafka-mirrors --failover` belum dijalankan | Jalankan `kafka-mirrors --failover` untuk semua topic |
| Data tidak masuk ke topic setelah inject | Debezium di source masih jalan, konflik `server_id` | Pause Debezium di source, restart connector di destination |
| SR subjects kosong setelah link dibuat | Schema exporter belum dibuat | Jalankan `schema-exporter --create` (Langkah 2) |

---

## Checklist Failover

### Pre-Failover
- [ ] Truststore source sudah di-copy ke node-all-service
- [ ] Cluster link sudah `ACTIVE`
- [ ] Schema exporter sudah running dan schema sudah ter-sync di destination SR
- [ ] Kedua connector (Source + Sink) sudah `RUNNING` di node-all-service
- [ ] Consumer group `LAG = 0`

### Saat Failover
- [ ] Pause Debezium di source cluster (node1/2/3)
- [ ] Verifikasi LAG masih `= 0`
- [ ] Jalankan `kafka-mirrors --failover` untuk semua topic
- [ ] Verifikasi `kafka-mirrors --describe` output kosong

### Post-Failover
- [ ] Restart Debezium task di node-all-service
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
