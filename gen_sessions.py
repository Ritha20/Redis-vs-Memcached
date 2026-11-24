"""Generate synthetic web-session records with Faker.

Each record mimics a server-side session object: identity, client metadata,
security tokens and a shopping cart that is used to pad the record to a
target serialized size (0.5 KB, 2 KB, 8 KB).

Usage: python gen_sessions.py --n 100000 --out data/
"""
import argparse
import json
import os
import random
import uuid

from faker import Faker

SIZES = {"small": 512, "medium": 2048, "large": 8192}


def make_session(fake: Faker, target_bytes: int) -> dict:
    created = fake.date_time_between(start_date="-2d", end_date="now")
    s = {
        "session_id": uuid.UUID(int=random.getrandbits(128)).hex,
        "user_id": fake.random_int(1, 5_000_000),
        "username": fake.user_name(),
        "email": fake.email(),
        "ip": fake.ipv4_public(),
        "user_agent": fake.user_agent(),
        "locale": fake.locale(),
        "roles": random.sample(["customer", "premium", "admin", "seller", "beta"], k=random.randint(1, 2)),
        "csrf_token": fake.sha256(),
        "created_at": created.isoformat(),
        "last_seen": created.isoformat(),
        "auth": {"mfa": fake.boolean(), "method": random.choice(["password", "oauth", "sso"])},
        "cart": [],
    }
    # Pad with realistic cart items until the JSON reaches the target size.
    while len(json.dumps(s)) < target_bytes:
        s["cart"].append({
            "sku": fake.ean13(),
            "name": fake.catch_phrase(),
            "qty": random.randint(1, 4),
            "price": round(random.uniform(1, 500), 2),
        })
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20000)
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    for name, size in SIZES.items():
        Faker.seed(args.seed)
        random.seed(args.seed)
        fake = Faker()
        path = os.path.join(args.out, f"sessions_{name}.jsonl")
        total = 0
        with open(path, "w") as f:
            for _ in range(args.n):
                line = json.dumps(make_session(fake, size))
                total += len(line)
                f.write(line + "\n")
        print(f"{path}: {args.n} records, mean {total / args.n:.0f} bytes")


if __name__ == "__main__":
    main()
