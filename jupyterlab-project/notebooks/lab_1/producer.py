from __future__ import annotations

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


def generate_transaction(sequence_number: int) -> dict:
    return {
        "tx_id": f"TX{sequence_number:04d}",
        "user_id": random.choice([f"u{i:02d}" for i in range(1, 21)]),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warszawa", "Kraków", "Gdańsk", "Wrocław"]),
        "category": random.choice(["elektronika", "odzież", "żywność", "książki"]),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="broker:9092",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
            "utf-8"
        ),
    )

    sequence_number = 1
    print("Sending transactions to transactions on broker:9092...")

    try:
        while True:
            transaction = generate_transaction(sequence_number)
            producer.send("transactions", transaction)
            producer.flush()
            print(
                f"SENT: {transaction['tx_id']} | "
                f"{transaction['amount']:.2f} PLN | "
                f"{transaction['store']} | {transaction['category']}"
            )
            sequence_number += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
