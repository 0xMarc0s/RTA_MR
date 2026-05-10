from __future__ import annotations

import json
from collections import defaultdict

from kafka import KafkaConsumer


def empty_stats() -> dict:
    return {
        "count": 0,
        "total": 0.0,
        "min": None,
        "max": None,
    }


def update_stats(stats: dict, amount: float) -> None:
    stats["count"] += 1
    stats["total"] += amount
    stats["min"] = amount if stats["min"] is None else min(stats["min"], amount)
    stats["max"] = amount if stats["max"] is None else max(stats["max"], amount)


def print_summary(category_stats: dict[str, dict]) -> None:
    print("\nKategoria   | Liczba | Przychod   | Min      | Max")
    print("------------------------------------------------------")
    for category, stats in sorted(category_stats.items()):
        print(
            f"{category:<11} | "
            f"{stats['count']:>6} | "
            f"{stats['total']:>10.2f} | "
            f"{stats['min']:>8.2f} | "
            f"{stats['max']:>8.2f}"
        )


def main() -> None:
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="broker:9092",
        group_id="lab1-stats",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    category_stats = defaultdict(empty_stats)
    msg_count = 0

    print("Tracking category stats. Summary every 10 messages.")

    try:
        for message in consumer:
            transaction = message.value
            category = transaction["category"]
            amount = transaction["amount"]

            update_stats(category_stats[category], amount)
            msg_count += 1

            if msg_count % 10 == 0:
                print_summary(category_stats)
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
