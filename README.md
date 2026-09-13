# 🛒 CartStreamAnalysis — Real-Time E-Commerce Stream Processing with Apache Flink

## 📌 Project Overview

**CartStreamAnalysis** is an end-to-end Big Data Engineering project that demonstrates real-time stream processing for e-commerce events using **Apache Kafka** and **Apache Flink SQL**.

The project simulates a continuous stream of e-commerce user interactions such as:

- `view`
- `cart`
- `remove_from_cart`
- `purchase`

The pipeline ingests events from a CSV dataset through a Python Kafka Producer, publishes them to Kafka, processes them with Apache Flink using **event time** and a **5-second watermark**, and aggregates purchase activity into **5-minute tumbling windows by brand**.

The final output contains:

- Total orders
- Gross revenue
- Average order value
- Unique buyers

The project is containerized with Docker so the required infrastructure can be reproduced consistently.

---

## 🏗️ Architecture

```text
                         ┌─────────────────────┐
                         │     100k.csv        │
                         │   E-Commerce Data   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │  Python Producer    │
                         │     kafka-python    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       Apache        │
                         │       Kafka         │
                         │                     │
                         │ ecommerce_events    │
                         │    3 partitions     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │            Apache Flink              │
                 │                                      │
                 │  Kafka Source                        │
                 │       ↓                              │
                 │  JSON Parsing                        │
                 │       ↓                              │
                 │  event_time_str                      │
                 │       ↓                              │
                 │  TIMESTAMP(3) event_time             │
                 │       ↓                              │
                 │  5-second Event-Time Watermark       │
                 │       ↓                              │
                 │  5-minute TUMBLE Window              │
                 │       ↓                              │
                 │  Filter: purchase                    │
                 │       ↓                              │
                 │  Group by window + brand             │
                 │       ↓                              │
                 │  Business Aggregations               │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ brand_window_sales  │
                         │     Print Sink      │
                         └─────────────────────┘
```

---

## 🧰 Technology Stack

| Technology | Purpose |
|---|---|
| Python | Kafka event producer |
| `kafka-python` | Kafka client used by the producer |
| Apache Kafka 4.1.0 | Event streaming / message broker |
| Apache Flink 2.1.0 | Distributed stream processing |
| Flink SQL | Stream processing logic |
| Flink Kafka Connector 5.0.0-2.1 | Kafka ↔ Flink integration |
| Kafka Clients 4.1.1 | Kafka client dependency for Flink |
| Docker | Containerization |
| Docker Compose | Local multi-service orchestration |
| JSON | Kafka event serialization |
| CSV | Input dataset format |

---

# 📂 Project Structure

```text
CartStreamAnalysis/
│
├── Data/
│   └── 100k.csv
│
├── Docker/
│   └── flink.Dockerfile
│
├── Flink/
│   └── flink_ecommerce_pipeline.sql
│
├── Kafka/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
└── docker-compose.yml
```

---

# 📊 Dataset

The project uses a sample of **100,000 records** taken from the Kaggle **eCommerce Events History in Cosmetics Shop** dataset.

The original dataset contains e-commerce interaction events with fields such as:

```text
event_time
event_type
product_id
category_id
category_code
brand
price
user_id
user_session
```

For this streaming pipeline, the producer sends only the fields required by Flink:

```text
event_time_str
event_type
product_id
category_code
brand
price
user_id
```

### Example input record

```text
2019-10-07 16:53:39 UTC,
view,
1004139,
2053013555631882655,
electronics.smartphone,
xiaomi,
97.27,
557789371,
...
```

The dataset intentionally contains real-world data quality characteristics such as:

- Missing `brand`
- Missing `category_code`
- Out-of-order event timestamps
- Bursty purchase activity

These characteristics make it useful for demonstrating event-time stream processing.

---

# 🔄 Data Flow

## 1. CSV → Python Producer

The Python producer reads:

```text
Data/100k.csv
```

using Python's `csv.DictReader`.

Each row is transformed into a JSON event.

Example:

```json
{
  "event_time_str": "2019-11-30 18:23:45 UTC",
  "event_type": "purchase",
  "product_id": 1005105,
  "category_code": "electronics.smartphone",
  "brand": "apple",
  "price": 1342.53,
  "user_id": 515147453
}
```

Empty values for `brand` and `category_code` are converted to JSON `null`.

---

# 📨 Kafka

The producer publishes events to:

```text
ecommerce_events
```

Kafka is configured with:

```text
Partitions: 3
Replication Factor: 1
```

For local development, the Kafka broker is reachable from other Docker containers using:

```text
kafka:9092
```

The producer uses:

```python
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"
```

---

# ⚡ Apache Flink

Apache Flink consumes the JSON events from Kafka using the Flink Kafka Connector.

The Flink deployment consists of:

```text
Flink JobManager
Flink TaskManager
```

The Flink Web UI is available at:

```text
http://localhost:8081
```

---

# ⏱️ Event Time Processing

The original timestamp is kept as:

```sql
event_time_str STRING
```

Flink then converts it into a native timestamp:

```sql
event_time AS TO_TIMESTAMP(
    event_time_str,
    'yyyy-MM-dd HH:mm:ss z'
)
```

This allows the pipeline to perform time-based processing using **event time** instead of processing/arrival time.

---

# 💧 Watermark

The pipeline uses a 5-second bounded-out-of-orderness watermark:

```sql
WATERMARK FOR event_time AS
    event_time - INTERVAL '5' SECOND
```

Conceptually:

```text
Maximum observed event time
            │
            ▼
      minus 5 seconds
            │
            ▼
        Watermark
```

The watermark allows Flink to handle events that arrive slightly out of order while still determining when event-time windows can be finalized.

---

# 🪟 5-Minute Tumbling Window

The project uses Flink's modern Windowing TVF with:

```sql
TUMBLE(
    TABLE ecommerce_events,
    DESCRIPTOR(event_time),
    INTERVAL '5' MINUTES
)
```

A tumbling window creates non-overlapping five-minute intervals:

```text
10:00 ───────── 10:05
10:05 ───────── 10:10
10:10 ───────── 10:15
```

Each purchase event belongs to one five-minute window.

---

# 🛍️ Purchase Filtering

Only purchase events are included in the business aggregation:

```sql
WHERE event_type = 'purchase'
```

This prevents page views, cart additions, and other interaction types from affecting sales metrics.

---

# 📈 Business Aggregations

The results are grouped by:

```text
window_start
window_end
brand
```

The following metrics are calculated.

### Total Orders

```sql
COUNT(*)
```

Counts purchase events inside each five-minute window for each brand.

### Gross Revenue

```sql
ROUND(SUM(price), 2)
```

Calculates the total purchase value.

### Average Order Value

```sql
ROUND(AVG(price), 2)
```

Calculates the average purchase value.

### Unique Buyers

```sql
COUNT(DISTINCT user_id)
```

Counts distinct users who made purchases in the window.

---

# 📤 Sink — `brand_window_sales`

The aggregated output is written to:

```text
brand_window_sales
```

The project currently uses Flink's **Print Sink** for execution verification:

```sql
WITH (
    'connector' = 'print'
)
```

This makes the generated records visible directly in the Flink TaskManager logs.

Example output:

```text
+I[2019-11-16T12:20,
   2019-11-16T12:25,
   givenchy,
   1,
   120.98,
   120.98,
   1]
```

This represents:

```text
window_start      = 2019-11-16 12:20
window_end        = 2019-11-16 12:25
brand             = givenchy
total_orders      = 1
gross_revenue     = 120.98
avg_order_value   = 120.98
unique_buyers     = 1
```

---

# 🧠 Complete Flink SQL Logic

The executable pipeline is stored in:

```text
Flink/flink_ecommerce_pipeline.sql
```

The main processing flow is:

```sql
INSERT INTO brand_window_sales
SELECT
    window_start,
    window_end,
    brand,
    COUNT(*) AS total_orders,
    CAST(
        ROUND(SUM(price), 2)
        AS DECIMAL(20, 2)
    ) AS gross_revenue,
    CAST(
        ROUND(AVG(price), 2)
        AS DECIMAL(20, 2)
    ) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_buyers
FROM TABLE(
    TUMBLE(
        TABLE ecommerce_events,
        DESCRIPTOR(event_time),
        INTERVAL '5' MINUTES
    )
)
WHERE event_type = 'purchase'
GROUP BY
    window_start,
    window_end,
    brand;
```

---

# 🐳 Docker Setup

The project uses Docker Compose to run the complete streaming environment.

Services:

```text
kafka
flink-jobmanager
flink-taskmanager
producer
```

### Kafka

```yaml
kafka:
  image: apache/kafka:4.1.0
```

### Flink

The project builds a custom Flink image so that the Kafka connector is available:

```dockerfile
FROM flink:2.1.0-scala_2.12-java17

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.1/flink-connector-kafka-5.0.0-2.1.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.1.1/kafka-clients-4.1.1.jar
```

The connector is installed under:

```text
/opt/flink/lib
```

---

# 🖥️ Flink Web UI

Once the project is running, open:

```text
http://localhost:8081
```

The Flink UI can be used to inspect:

- Job status
- JobManager
- TaskManager
- Operators
- Records received
- Records sent
- Running jobs
- Execution graph

For this project, the UI was used as execution verification for the streaming pipeline.

---

# ▶️ How to Run the Project

## Prerequisites

You need:

- Docker Desktop
- Docker Compose
- Git

No local Kafka or Flink installation is required because they run inside Docker containers.

---

## 1. Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd CartStreamAnalysis
```

---

## 2. Start Kafka and Flink

From the project root:

```bash
docker compose up -d --build kafka flink-jobmanager flink-taskmanager
```

Check running containers:

```bash
docker ps
```

You should see:

```text
cartstream-kafka
cartstream-flink-jobmanager
cartstream-flink-taskmanager
```

---

## 3. Create the Kafka Topic

Run:

```bash
docker exec -it cartstream-kafka \
/opt/kafka/bin/kafka-topics.sh \
--create \
--topic ecommerce_events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

If the topic already exists, you can skip this step.

---

## 4. Build the Producer

```bash
docker compose build producer
```

---

## 5. Start the Producer

```bash
docker compose up producer
```

The producer reads:

```text
Data/100k.csv
```

and publishes the events to:

```text
ecommerce_events
```

Wait until:

```text
Finished sending all events.
```

---

# 6. Start Flink SQL Client

Open another terminal:

```bash
docker exec -it cartstream-flink-jobmanager \
/opt/flink/bin/sql-client.sh
```

You should see:

```text
Flink SQL>
```

---

# 7. Run the SQL Pipeline

From the Flink SQL Client, execute the contents of:

```text
Flink/flink_ecommerce_pipeline.sql
```

The SQL script creates:

```text
ecommerce_events
brand_window_sales
```

and submits the streaming aggregation job.

---

# 8. Monitor the Job

Open:

```text
http://localhost:8081
```

Go to:

```text
Running Jobs
```

You should see the streaming job.

The execution graph should contain the Kafka source, aggregation, and sink.

---

# 9. Verify Sink Output

Open another terminal:

```bash
docker logs cartstream-flink-taskmanager --tail 50
```

You should see output similar to:

```text
+I[2019-11-16T12:20, 2019-11-16T12:25, givenchy, 1, 120.98, 120.98, 1]
```

---

# 🧹 Cleanup

When finished:

```bash
docker compose down
```

To remove containers, networks, and the local environment created by Compose.

---

### Quick Start

```bash
git clone <https://github.com/YoussefKh117/Ecommerce_Cart_Streaming_Pipeline.git>
cd CartStreamAnalysis

docker compose up -d --build kafka flink-jobmanager flink-taskmanager

docker exec -it cartstream-kafka \
/opt/kafka/bin/kafka-topics.sh \
--create \
--topic ecommerce_events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

docker compose build producer

docker compose up producer
```

Then open another terminal and start:

```bash
docker exec -it cartstream-flink-jobmanager \
/opt/flink/bin/sql-client.sh
```

Execute:

```text
Flink/flink_ecommerce_pipeline.sql
```

Finally, monitor the job through:

```text
http://localhost:8081
```

and verify the output through:

```bash
docker logs cartstream-flink-taskmanager --tail 50
```

---

The project demonstrates practical stream-processing concepts including **Kafka ingestion, event time, watermarks, windowing, streaming SQL, aggregations, Dockerized infrastructure, and execution monitoring**.
