# kafka_simulator.py
from kafka import KafkaProducer
from pathlib import Path
import json, time, csv

# ---- file path (relative to this .py) ----
BASE = Path(__file__).resolve().parent
CSV_PATH = BASE / "data" / "daily" / "transactions_20250121.csv"

BOOTSTRAP = "kafka-event.servicebus.windows.net:9093"
TOPIC = "transactions_raw"
SASL_USERNAME = "$ConnectionString"
SASL_PASSWORD = "Endpoint=sb://kafka-event.servicebus.windows.net/;SharedAccessKeyName=manage-all;SharedAccessKey=W9n6HBunYaMyDxLlr+e5PcZJXbbWIr4hp+AEhP37JYE="

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

rate_sec = 0.05  # ~20 events/sec

with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    sent = 0
    for row in r:
        key = {"CUSTOMER_ID": row["CUSTOMER_ID"]}
        msg = {
            "TRANSACTION_ID": row["TRANSACTION_ID"],
            "TX_DATETIME": row["TX_DATETIME"],
            "CUSTOMER_ID": row["CUSTOMER_ID"],
            "TERMINAL_ID": row["TERMINAL_ID"],
            "TX_AMOUNT": float(row["TX_AMOUNT"]),
            "CUSTOMER_LAT": float(row["CUSTOMER_LAT"]),
            "CUSTOMER_LON": float(row["CUSTOMER_LON"]),
            "TERMINAL_LAT": float(row["TERMINAL_LAT"]),
            "TERMINAL_LON": float(row["TERMINAL_LON"]),
            "TX_FRAUD": int(row.get("TX_FRAUD", 0)),
        }
        producer.send(TOPIC, key=key, value=msg)
        sent += 1
        if sent % 100 == 0:
            print(f"sent {sent} events…")
        time.sleep(rate_sec)

producer.flush()
print(f"✅ done. total sent: {sent} from {CSV_PATH}")
