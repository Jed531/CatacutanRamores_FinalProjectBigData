# 🌊 Marine Weather Streaming Dashboard

Real-time and historical dashboard for marine conditions in Philippine waters.
**Pipeline:** WeatherAPI Marine → Kafka → Spark Structured Streaming → MongoDB → Streamlit

## 1. The API used
**WeatherAPI.com Marine API** (`https://api.weatherapi.com/v1/marine.json`).
Query by latitude/longitude (`q`), number of days (`days`) and API key (`key`).
It returns a JSON Marine object with hourly forecast data for a sea point, including
significant wave height (`sig_ht_mt`), swell height/direction/period, wind speed,
visibility, tides, and water temperature (`water_temp_c`, Pro+ plans only).
The data is model-based forecast data (not buoy observations) and changes hourly, so the
producer linearly interpolates between the current and next hour (`--no-interpolate` disables this).
Monitored points: Manila Bay, Subic Bay, Cebu Strait, Davao Gulf, Batangas Bay.

## 2. The pipeline implemented
| Stage | Component | Role |
|---|---|---|
| Ingest | `producer.py` | Polls the API every 60 s, publishes one JSON message per metric/location (`timestamp`, `value`, `metric_type`, `sensor_id`, + location, unit, lat/lon) |
| Buffer | Apache Kafka (KRaft) | Decouples producer from consumers; durable, replayable log (topic `marine-weather`) |
| Process | `spark_stream.py` | Spark Structured Streaming reads Kafka, parses JSON, upserts raw readings into MongoDB, and computes 10-minute windowed avg/min/max/count per location+metric |
| Store | MongoDB (`marine_db`) | `readings` (raw) and `aggregates` (Spark windows); unique indexes make writes idempotent |
| Serve | `app.py` (Streamlit) | **Live tab** consumes Kafka directly (refresh every 15 s); **History tab** queries MongoDB with filters, hourly/daily/weekly aggregation, CSV and PNG export |

Dual path: a fast path (Kafka → Streamlit) for live monitoring and a durable path
(Kafka → Spark → MongoDB → Streamlit) for history and aggregation.

## 3. Significance of the dashboard
The Philippines is an archipelago where fishing, ferries, cargo, and tourism depend on sea conditions.
The Live tab lets operators see wave, swell, wind and visibility at a glance across several areas
and judge whether conditions are safe for sailing. The History tab shows trends and extremes
(e.g. which areas are consistently rough, when waves peak), supporting scheduling, safety advisories,
and disaster preparedness. Aggregations (hourly/daily/weekly and Spark windows) turn many readings
into comparable statistics, and CSV/PNG exports make results reusable in reports.
Limitation: values are forecast-model outputs, so they should support, not replace, official advisories (PAGASA).

## 4. Setup and run
See the step-by-step guide: install Java 17, Miniconda, Kafka 3.7 (KRaft), MongoDB 7.0, then:

```bash
conda create -n bigdata python=3.10.13 -y && conda activate bigdata
pip install -r requirements.txt
cp .env.example .env          # add your WEATHERAPI_KEY
# create topic once:
~/kafka/bin/kafka-topics.sh --create --topic marine-weather --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
Start in this order (each in its own terminal):
```bash
# 1) Kafka broker      2) mongod
python spark_stream.py                 # Kafka -> MongoDB
python producer.py                     # API -> Kafka
streamlit run app.py --server.address 0.0.0.0
```

## 5. Deliverables
`app.py`, `producer.py`, `spark_stream.py`, `requirements.txt`, `README.md`