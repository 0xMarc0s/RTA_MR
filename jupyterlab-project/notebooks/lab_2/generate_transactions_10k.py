from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_PATH = Path(__file__).resolve().parent / "data" / "transactions_10k.jsonl"
SEED = 2026
RECORD_COUNT = 10_000

USERS = [f"u{i:03d}" for i in range(1, 501)]
STORES = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
CATEGORIES = ["elektronika", "odzież", "żywność", "książki"]
START_TIME = datetime(2026, 1, 15, 8, 0, 0)
SECONDS_IN_RANGE = 3 * 60 * 60


def generate_transaction(sequence_number: int) -> dict:
    timestamp = START_TIME + timedelta(seconds=random.randrange(SECONDS_IN_RANGE))
    return {
        "tx_id": f"TX{sequence_number:05d}",
        "user_id": random.choice(USERS),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(STORES),
        "category": random.choice(CATEGORIES),
        "timestamp": timestamp.isoformat(timespec="seconds"),
    }


def main() -> None:
    random.seed(SEED)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as output_file:
        for sequence_number in range(1, RECORD_COUNT + 1):
            transaction = generate_transaction(sequence_number)
            output_file.write(json.dumps(transaction, ensure_ascii=False) + "\n")

    print(f"Generated {RECORD_COUNT} transactions in {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
