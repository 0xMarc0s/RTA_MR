from __future__ import annotations

import json
from collections import Counter, defaultdict

from kafka import KafkaConsumer


def print_summary(store_counts: Counter, total_amount: dict[str, float]) -> None:
    print("\nSklep      | Liczba | Suma       | Srednia")
    print("---------------------------------------------")
    for store, count in store_counts.most_common():
        total = total_amount[store]
        average = total / count
        print(f"{store:<10} | {count:>6} | {total:>10.2f} | {average:>7.2f}")


def main() -> None:
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="broker:9092",
        group_id="lab1-count",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    store_counts = Counter()
    total_amount = defaultdict(float)
    msg_count = 0

    print("Counting transactions per store. Summary every 10 messages.")

    try:
        for message in consumer:
            transaction = message.value
            store = transaction["store"]
            amount = transaction["amount"]

            store_counts[store] += 1
            total_amount[store] += amount
            msg_count += 1

            if msg_count % 10 == 0:
                print_summary(store_counts, total_amount)
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
