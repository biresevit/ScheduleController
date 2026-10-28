"""Reproducible offline 56/45 demonstration; no database or poker server access."""
import json
import random
from datetime import datetime, timezone
from decimal import Decimal

from .planner import Account, Entry, Machine, Settings, Table, plan
from .rules import nenad_bands


def main():
    now = datetime(2026, 10, 27, 9, tzinfo=timezone.utc)
    accounts, entries, tables = [], [], []
    for index in range(9):
        tier = ("low", "medium", "high")[index // 3]
        ids = frozenset(range(index * 5 + 1, index * 5 + 6))
        for account_id in ids:
            accounts.append(Account(account_id, 1, tier, enabled=False))
            entries.append(Entry(str(account_id), account_id, account_id % 2 + 1, str(index)))
        tables.append(Table(str(index), 1, (Decimal(".1"), Decimal(".5"), Decimal(2))[index // 3],
                            9, 5, now, observed_accounts=ids))
    for index in range(12):
        accounts.append(Account(100 + index, 1, ("low", "medium", "high")[index % 3],
                                synthetic=True, enabled=False, credentials_ready=False))
    result = plan(Settings(1, nenad_bands()), now, 56, accounts,
                  [Machine(1, 1, 40, now), Machine(2, 1, 40, now)], tables, entries,
                  random.Random(7), simulation=True)
    print(json.dumps({"mode": "OFFLINE SIMULATION - no actions executed", "target": 56,
                      "seated_before": 45, "planned": [vars(a) for a in result.actions],
                      "remaining_deficits": result.remaining_deficits}, default=str, indent=2))


if __name__ == "__main__":
    main()
