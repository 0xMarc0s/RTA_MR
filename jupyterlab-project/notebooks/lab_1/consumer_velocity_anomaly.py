from __future__ import annotations

import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from kafka import KafkaConsumer


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def main() -> None:
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="broker:9092",
        group_id="lab1-velocity-anomaly",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    transactions_by_user = defaultdict(deque)
    window = timedelta(seconds=60)

    print(
        "Listening for velocity anomalies: "
        ">3 transactions per user in 60s."
    )

    try:
        for message in consumer:
            transaction = message.value
            user_id = transaction["user_id"]
            event_time = parse_timestamp(transaction["timestamp"])
            user_events = transactions_by_user[user_id]

            user_events.append(event_time)
            while user_events and event_time - user_events[0] > window:
                user_events.popleft()

            if len(user_events) > 3:
                print(
                    f"VELOCITY ALERT: {user_id} | "
                    f"{len(user_events)} transactions in 60s | "
                    f"last tx {transaction['tx_id']}"
                )
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
