"""
Kafka Producer: WeatherAPI Marine data -> Kafka

Fetches hourly marine data (wave height, swell height, water temperature, wind
speed, visibility) for several Philippine sea points from the Marine API
(marine.json) and publishes one message per metric per location.

Message schema (required fields first):
{
  "timestamp": "2026-11-06T10:15:00+00:00",  # ISO 8601, UTC (time of reading)
  "value": 1.23,
  "metric_type": "wave_height",
  "sensor_id": "manila_bay",
  "location": "Manila Bay", "unit": "m", "latitude": 14.5, "longitude": 120.8
}
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

API_URL = "https://api.weatherapi.com/v1/marine.json"

# Approximate sea points; the Marine API snaps to the nearest marine location.
LOCATIONS = [
    {"id": "manila_bay", "name": "Manila Bay", "lat": 14.50, "lon": 120.80},
    {"id": "subic_bay", "name": "Subic Bay", "lat": 14.78, "lon": 120.25},
    {"id": "cebu_strait", "name": "Cebu Strait", "lat": 10.05, "lon": 124.05},
    {"id": "davao_gulf", "name": "Davao Gulf", "lat": 6.80, "lon": 125.75},
    {"id": "batangas_bay", "name": "Batangas Bay", "lat": 13.70, "lon": 121.05},
]

# metric_type -> (field in API "hour" element, unit)
METRIC_MAP = {
    "wave_height": ("sig_ht_mt", "m"),
    "swell_height": ("swell_ht_mt", "m"),
    "water_temperature": ("water_temp_c", "celsius"),  # Pro+ plans only
    "wind_speed": ("wind_kph", "km/h"),
    "visibility": ("vis_km", "km"),
}


class ApiAccessError(Exception):
    """Raised for key/quota/plan problems that retrying will not fix."""


class StreamingDataProducer:
    """Fetches marine data from WeatherAPI and sends it to Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str, api_key: str, interpolate: bool = True):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.interpolate = interpolate
        self._warned_missing = set()

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                retries=3,
                linger_ms=10,
            )
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable: 
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    # ------------------------------------------------------------------ API
    def fetch_marine_records(self, loc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Call marine.json for one location and return one record per metric."""
        resp = requests.get(
            API_URL,
            params={"key": self.api_key, "q": f"{loc['lat']},{loc['lon']}", "days": 1},
            timeout=15,
        )
        if resp.status_code != 200:
            try:
                msg = resp.json()["error"]["message"]
            except Exception:
                msg = resp.text[:200]
            if resp.status_code in (401, 403):
                raise ApiAccessError(f"HTTP {resp.status_code}: {msg}")
            raise RuntimeError(f"HTTP {resp.status_code}: {msg}")

        data = resp.json()
        hours = [
            h
            for day in data.get("forecast", {}).get("forecastday", [])
            for h in day.get("hour", [])
        ]
        if not hours:
            raise RuntimeError("API returned no hourly marine data for this point")

        # Find the hour containing "now" using epoch time (timezone-independent).
        now = time.time()
        idx = 0
        for i, h in enumerate(hours):
            if h.get("time_epoch", 0) <= now:
                idx = i
        h0 = hours[idx]
        h1 = hours[idx + 1] if idx + 1 < len(hours) else None
        frac = 0.0
        if h1 and h1.get("time_epoch") and h0.get("time_epoch"):
            frac = (now - h0["time_epoch"]) / (h1["time_epoch"] - h0["time_epoch"])
            frac = min(max(frac, 0.0), 1.0)

        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        records = []
        for metric_type, (field, unit) in METRIC_MAP.items():
            v0 = h0.get(field)
            if v0 is None:
                if metric_type not in self._warned_missing:
                    print(f"WARNING: '{field}' not in API response (plan restriction?) - skipping {metric_type}")
                    self._warned_missing.add(metric_type)
                continue
            value = float(v0)
            v1 = h1.get(field) if h1 else None
            if self.interpolate and v1 is not None:
                value = value + (float(v1) - value) * frac
            records.append(
                {
                    "timestamp": timestamp,
                    "value": round(value, 2),
                    "metric_type": metric_type,
                    "sensor_id": loc["id"],
                    "location": loc["name"],
                    "unit": unit,
                    "latitude": loc["lat"],
                    "longitude": loc["lon"],
                }
            )
        return records

    # ---------------------------------------------------------------- Kafka
    def serialize_data(self, data: Dict[str, Any]) -> Optional[bytes]:
        try:
            return json.dumps(data).encode("utf-8")
        except (TypeError, ValueError) as e:
            print(f"Serialization error: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        payload = self.serialize_data(data)
        if not payload:
            return False
        try:
            self.producer.send(self.topic, key=data["sensor_id"].encode("utf-8"), value=payload)
            return True
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, interval: float = 60.0, duration: Optional[int] = None):
        """Poll every location every `interval` seconds and publish to Kafka."""
        print(f"Starting producer: polling {len(LOCATIONS)} locations every {interval:.0f}s, "
              f"duration: {duration or 'infinite'}")
        start = time.time()
        sent_total = 0
        try:
            while True:
                if duration and (time.time() - start) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break
                cycle_start = time.time()
                sent_cycle = 0
                for loc in LOCATIONS:
                    try:
                        for record in self.fetch_marine_records(loc):
                            if self.send_message(record):
                                sent_cycle += 1
                    except ApiAccessError as e:
                        print(f"FATAL API access problem: {e}")
                        return
                    except (requests.RequestException, RuntimeError, KeyError, ValueError) as e:
                        print(f"Skipping {loc['name']} this cycle: {e}")
                if self.producer:
                    self.producer.flush(timeout=10)
                sent_total += sent_cycle
                print(f"[{datetime.now().strftime('%H:%M:%S')}] sent {sent_cycle} messages "
                      f"(total {sent_total})")
                time.sleep(max(1.0, interval - (time.time() - cycle_start)))
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {sent_total}")

    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    p = argparse.ArgumentParser(description="WeatherAPI Marine -> Kafka producer")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="marine-weather")
    p.add_argument("--interval", type=float, default=60.0,
                   help="Seconds between API polling cycles (default 60)")
    p.add_argument("--duration", type=int, default=None, help="Run time in seconds (default: infinite)")
    p.add_argument("--api-key", default=None, help="WeatherAPI key (or set WEATHERAPI_KEY in .env)")
    p.add_argument("--no-interpolate", action="store_true",
                   help="Send the raw hourly value instead of interpolating between hours")
    return p.parse_args()


def main():
    args = parse_arguments()
    api_key = args.api_key or os.getenv("WEATHERAPI_KEY")
    if not api_key:
        print("ERROR: No API key. Put WEATHERAPI_KEY=... in .env or pass --api-key.")
        return
    print("=" * 60)
    print("MARINE WEATHER PRODUCER (WeatherAPI Marine -> Kafka)")
    print("=" * 60)
    producer = StreamingDataProducer(
        args.bootstrap_servers, args.topic, api_key, interpolate=not args.no_interpolate
    )
    if not producer.producer:
        return
    producer.produce_stream(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    main()