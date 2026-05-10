from __future__ import annotations

import json

from kafka import KafkaConsumer


def main() -> None:
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="broker:9092",
        group_id="lab1-filter",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print("Listening for high-value transactions (amount > 3000)...")

    try:
        for message in consumer:
            transaction = message.value
            if transaction["amount"] > 3000:
                print(
                    f"ALERT: {transaction['tx_id']} | "
                    f"{transaction['amount']:.2f} PLN | "
                    f"{transaction['store']} | {transaction['category']}"
                )
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
