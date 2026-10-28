# Single-PC Evenbet pilot

The pilot is explicitly restricted to Nenad/Evenbet site 1, machine 1 and the eight usernames supplied by the customer. It uses the same planner as the website simulator and the existing PokerController/PDecision implementation. It is a bounded local integration test, not a deployed Azure timer or distributed queue worker.

Requirements: local PDecision ready on port 5005, Evenbet/EvenBet registration, Azure hand-history upload enabled, SQL connection environment, and PokerController installed in this project's venv. Its supplied demo password is reused through PokerController settings; passwords are not stored in the SQL journal or telemetry.

```powershell
# Read-only login and table probe; no seating or SQL mutations.
.\.venv\Scripts\python.exe -m schedule_controller.pilot --probe --output artifacts\probe

# Three-minute lifecycle smoke test, retaining assigned stake tiers.
.\.venv\Scripts\python.exe -m schedule_controller.pilot --target 3 --seconds 180 --output artifacts\smoke-new

# After approving the temporary high-tier test assignment:
.\.venv\Scripts\python.exe -m schedule_controller.pilot --target 24 --capacity 30 --seconds 1800 --high-only --output artifacts\eight-account-pilot
```

Maximum three tables per account in this pilot (the persistent account limit may be higher). This means reaching 24 entries requires all eight accounts. Joins use the configured pacing, reservations count toward capacity, buy-in choices use each account's weighted policy, and the first buy-in observes the configured post-BB probability. RIT preference is sent only where the table advertises RIT support.

Accounts are authenticated lazily. When no playing connection can provide lobby telemetry, one account logs in for the lobby scan and logs out immediately afterward. A playing account logs in only when its first join task is claimed, reuses that single connection for its assigned tables, and logs out after its final planned or active table closes. Starting the pilot therefore does not log in all configured users.

The pilot records session begin/planned end/actual end, publishes live table seats, and accumulates per-account session time separately from entry time. SQL AccountInterval IDs make retried accounting idempotent. Planned joins are committed before sending poker commands. Configuration version, machine epoch/capacity and allowed-site membership are rechecked before executing a join. Uncertain outcomes retain open entries/leases for reconciliation rather than silently claiming a clean exit.

The temporary schedule/settings are restored at the end if the customer has not edited the configuration during the run. Scheduling is paused at completion. Machine capacity remains at the chosen cap; readiness is cleared. Sessions finish current hands on exit where possible. A `stop` file in the run's output directory requests graceful shutdown; do not terminate the process to stop ordinary play.

`status.json` contains aggregate progress, account entry counts, action count, errors and sessions. `pilot.log` contains the execution log. Neither contains passwords or authentication tokens.

The curated table set excludes unsupported variants and stays inside configured stake ranges; it does not select by human/bot composition. Call Time tables are also excluded because Evenbet can refuse an early leave until their timer/profit condition clears, which is incompatible with bounded scheduled sessions. At the time of the probe there were no supported medium-tier tables. The 24-entry test therefore needs the explicitly selected temporary high-tier assignment; the three original stake boundaries remain unchanged.

The production path now has a deployed 30-second Azure timer plus a fenced machine command consumer exposed through `python -m poker_controller.worker`. Remaining work is restart reconciliation, durable credential management, recurring in-session breaks, and longer multi-account load testing.
