# Vehicle Telemetry Streaming with ELK

Simulated real-time vehicle telemetry data including speed, fuel level, engine and tire health metrics. This project streams vehicle sensor events through **Kafka**, processes and enriches the data with **Spark Structured Streaming**, and stores the results in **Elasticsearch** for real-time visualization and alerting in **Kibana**.

---

## Project Overview

- **Data Generation**: A Python Kafka Producer simulates vehicle telemetry data such as speed, fuel level, tire quality, engine quality, GPS coordinates, and usage statistics.
- **Data Streaming**: Vehicle data is streamed in real-time to a Kafka topic.
- **Data Processing**: Spark Structured Streaming reads from Kafka, parses JSON telemetry records, computes additional metrics like average tire quality, speed categories, fuel alerts, engine status, vehicle age, and quality scores.
- **Data Storage**: The enriched streaming data is written to Elasticsearch for efficient indexing and search.
- **Visualization**: Kibana is used to build interactive real-time dashboards to monitor vehicle health, detect anomalies, and alert on critical conditions.

---
## Architecture Diagram

```mermaid
graph LR
    A[Vehicle Simulator] -->|JSON Telemetry| B[Kafka]
    B -->|Stream| C[Spark]
    C -->|Enriched Data| D[Elasticsearch]
    D -->|Visualize| E[Kibana]
    
    subgraph Data Producers
        A
    end
    
    subgraph Streaming Layer
        B["Kafka Topic<br>vehicle_telemetry<br>(3 partitions)"]
    end
    
    subgraph Processing
        C["Spark Structured Streaming<br>- Enrichment<br>- Aggregation<br>- Alert Detection"]
    end
    
    subgraph Storage/Visualization
        D["Elasticsearch<br>(Index: vehicle_metrics)"]
        E["Kibana<br>- Dashboards<br>- Alerts"]
    end
