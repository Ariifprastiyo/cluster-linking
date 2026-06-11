# Schema Linking — Confluent Platform On-Premise

> Dokumentasi ini menjelaskan cara melakukan Schema Linking antar Schema Registry Confluent Platform secara on-premise, dari DEV Cluster ke UAT/DR Cluster.

---

## Daftar Isi

- [Overview](#overview)
- [Arsitektur](#arsitektur)
- [Prerequisites](#prerequisites)
- [Struktur File](#struktur-file)
- [Cara Penggunaan](#cara-penggunaan)
- [Verifikasi](#verifikasi)
- [Operasi Lanjutan](#operasi-lanjutan)
- [Troubleshooting](#troubleshooting)

---

## Overview

Schema Linking adalah fitur Confluent Platform yang men-sync schema secara **otomatis dan continuous** dari Schema Registry source ke destination. Schema ID dipertahankan sehingga Avro consumer tidak error saat berpindah cluster — sangat penting untuk skenario DR/Failover.

```
[DEV Cluster]                            [UAT/DR Cluster]
node1/2/3.alldataint.com:8081  ───────▶  node-all-service.alldataint.com:8081
     Schema Registry Source    schema-exporter   Schema Registry Destination
                               (push, dijalankan
                                dari source side)
```

### Perbedaan Schema Linking vs Import Manual

| | Schema Linking | Import Manual |
|---|---|---|
| Sync | Otomatis & continuous | Sekali jalan |
| Schema ID | Dipertahankan | Bisa berbeda |
| Schema baru | Otomatis ter-sync | Harus import ulang |
| Cocok untuk DR | ✅ Ya | ❌ Tidak ideal |

---

## Arsitektur

```
node1.alldataint.com (DEV)
├── Kafka Broker      :9093
├── Schema Registry   :8081  ◀── source schema
├── Kafka Connect     :8083
└── MDS               :8090

node-all-service.alldataint.com (UAT/DR)
├── Kafka Broker      :9093
├── Schema Registry   :8081  ◀── destination schema (hasil sync)
├── Kafka Connect     :8083
└── MDS               :8090
```

---

## Prerequisites

| Requirement | Keterangan |
|---|---|
| Confluent Platform | Terinstall di kedua cluster |
| Confluent CLI | `confluent` binary tersedia di PATH |
| Konektivitas | node1 bisa reach node-all-service:8081 |
| SSL Truststore | `/var/ssl/private/kafka_connect.truststore.jks` |
| Kredensial SR | username: `admin`, password: `P@ssw0rd` |

Verifikasi konektivitas sebelum mulai:
```bash
curl -sk https://node-all-service.alldataint.com:8081/ -u admin:P@ssw0rd
# Expected: {}
```

Verifikasi Confluent CLI tersedia:
```bash
confluent version
```

---

## Struktur File

```
.
├── README-Schema-Linking.md   # Dokumen ini
├── config.txt                 # Konfigurasi koneksi ke destination SR
└── Schema-Linking-Try.md      # Panduan langkah lengkap
```

---

## Cara Penggunaan

### 1. Buat `config.txt`

File ini berisi URL dan kredensial **destination** Schema Registry. Buat di node1:

```bash
cat > /tmp/config.txt << 'EOF'
schema.registry.url=https://node-all-service.alldataint.com:8081
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=admin:P@ssw0rd
schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
schema.registry.ssl.truststore.password=confluenttruststorepass
EOF
```

### 2. (Opsional) Set Mode IMPORT di Destination

Lakukan hanya jika subject **sudah ada** di destination dengan ID yang berbeda:

```bash
curl -sk -X PUT \
  -H "Content-Type: application/json" \
  -u admin:P@ssw0rd \
  https://node-all-service.alldataint.com:8081/mode/db_ecommerce.db_ecommerce.orders-value \
  --data '{"mode": "IMPORT"}'
```

### 3. Buat Schema Exporter

Jalankan dari **node1** (source side):

```bash
confluent schema-registry exporter create schema-link-ecommerce \
  --context-type NONE \
  --config-file /tmp/config.txt \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks \
  --subjects db_ecommerce.db_ecommerce.orders-key,db_ecommerce.db_ecommerce.orders-value
```

> Untuk sync **semua subject** sekaligus, ganti `--subjects` dengan `--subject-format ":.*:"`

### 4. Cek Status

```bash
confluent schema-registry exporter get-status schema-link-ecommerce \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks
```

---

## Verifikasi

### Cek Subject Sudah Ter-sync di Destination

```bash
curl -sk https://node-all-service.alldataint.com:8081/subjects \
  -u admin:P@ssw0rd
```

### Verifikasi Schema ID Identik

```bash
# Source
curl -sk https://node1.alldataint.com:8081/subjects/db_ecommerce.db_ecommerce.orders-value/versions/latest \
  -u admin:P@ssw0rd | python3 -c "import sys,json; d=json.load(sys.stdin); print('Source ID:', d['id'])"

# Destination
curl -sk https://node-all-service.alldataint.com:8081/subjects/db_ecommerce.db_ecommerce.orders-value/versions/latest \
  -u admin:P@ssw0rd | python3 -c "import sys,json; d=json.load(sys.stdin); print('Destination ID:', d['id'])"
```

> ✅ Nilai ID harus **sama** di kedua cluster.

---

## Operasi Lanjutan

### List Semua Exporter
```bash
confluent schema-registry exporter list \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks
```

### Pause Exporter
```bash
confluent schema-registry exporter pause schema-link-ecommerce \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks
```

### Resume Exporter
```bash
confluent schema-registry exporter resume schema-link-ecommerce \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks
```

### Update Config Exporter
```bash
confluent schema-registry exporter update schema-link-ecommerce \
  --config-file /tmp/config.txt \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks
```

### Delete Exporter
```bash
confluent schema-registry exporter delete schema-link-ecommerce \
  --schema-registry-endpoint https://node1.alldataint.com:8081 \
  --ca-location /var/ssl/private/kafka_connect.truststore.jks
```

---

## Troubleshooting

| Error | Penyebab | Solusi |
|---|---|---|
| `Connection refused` | Port 8081 destination tidak bisa diakses | Cek firewall antara node1 dan node-all-service |
| `SSL handshake failed` | CA cert tidak cocok | Pastikan `--ca-location` menggunakan cert yang benar |
| Status `FAILED` | Kredensial salah atau destination SR down | Cek `config.txt`, verifikasi SR destination running |
| Schema ID berbeda | Subject sudah ada di destination dengan ID lain | Set mode IMPORT (Step 2), buat ulang exporter |
| `confluent: command not found` | Confluent CLI belum terinstall | Install: `curl -sL --http1.1 https://cnfl.io/cli \| sh -s -- latest` |
| Subject tidak muncul | Exporter baru dibuat | Tunggu beberapa detik, cek status exporter |

---

## Referensi

- [Schema Linking on Confluent Platform](https://docs.confluent.io/platform/current/schema-registry/schema-linking-cp.html)
- [Confluent CLI — schema-registry exporter](https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/exporter/)
- [Schema Registry Overview](https://docs.confluent.io/platform/current/schema-registry/overview.html)
- [GitHub Repo](https://github.com/Ariifprastiyo/cluster-linking)

---

*© 2026 Alldataint — Internal Documentation*
