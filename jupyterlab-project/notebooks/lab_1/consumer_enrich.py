from __future__ import annotations

import json

from kafka import KafkaConsumer


def risk_level(amount: float) -> str:
    if amount > 3000:
        return "HIGH"
    if amount > 1000:
        return "MEDIUM"
    return "LOW"


def main() -> None:
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="broker:9092",
        group_id="lab1-enrich",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print("Listening for transactions and adding risk_level...")

    try:
        for message in consumer:
            transaction = message.value
            transaction["risk_level"] = risk_level(transaction["amount"])
            print(json.dumps(transaction, ensure_ascii=False))
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
