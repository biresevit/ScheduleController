# Foundation validation — 2026-10-26

## Result

`SCHEDULE_SQL_TEST=1 .venv/Scripts/python.exe -m pytest -q --tb=short`: **40 passed** (35 pure tests, 5 rollback-only Azure SQL integration tests).

Migration `001_foundation.sql` and the Nenad/Evenbet seed were committed to the existing Azure pokerdb and verified through a new connection. A subsequent dry run returned `migrations: []` and `policies_inserted: 0`, confirming repeatability and no default regeneration.

The migration runner was corrected during validation: with ODBC autocommit disabled, an extra explicit `BEGIN TRANSACTION` nested the driver transaction. The runner now relies on the ODBC transaction and asserts one transaction before commit. The failed first apply persisted no schema or customer reassignment. Independent read-back verified the corrected apply.

## Deployed data

- Customer: Nenad, CustomerId 14004. Identity gaps from rolled-back provisioning tests are harmless.
- Scheduler SiteId: 1, Casino: Evenbet, Platform: EvenBet, timezone: Europe/Budapest.
- Currency label: DEMO; the future adapter must validate/map the actual site currency before using a snapshot.
- Existing CustomerMachine 1 moved from Internal (Nenad dev) to Nenad. Scheduler capacity: six. Existing hand-history CustomerId values are retained.
- Six policies: Monica plus `__schedule_demo_01` through `__schedule_demo_05`.
- Exactly two accounts per stake tier, frequency tier, and daily preference bucket (120/240/360 minutes).
- Monica: high stakes, high frequency, 240-minute daily preference, PostBbChance 0.08108, RunItTwiceChance 0.81944.
- Strong defaults are within the requested probability intervals; all buy-in distributions sum to 100%.
- Low [0.02,0.5), medium [0.5,1), high [1,4]; target shares 33.34%, 33.33%, 33.33%.
- Synthetic accounts are inactive in dbo.BotAccount; SQL prevents enabling their scheduler policies or attaching credentials.
- Scheduling, machine assignment and account execution policies remain disabled; zero weekly schedule rows and zero commands.
- Simo/BluffPoker data was not changed.

## Behavior checked

- Target 56 with 45 current entries produces three joins at 20/40/60 seconds when those gap settings are selected; remaining deficit eight.
- Default jitter simulation can produce fewer than three when a later join would exceed the two-minute planning horizon. This is intentional.
- Already observed pending seats count once; unseen reservations still consume capacity.
- Empty tables and the last available seat are eligible without human/bot composition checks.
- Account ownership pins all its tables to one machine; same-table machine concentration outranks overall machine load.
- Synthetic/live isolation, customer/site scope, stale snapshots, unavailable currencies/stakes and hard capacities.
- Soft daily preference fallback, frequency weighting, sit-out occupancy, queued-join cancellation and existing drains.
- Weekly interval boundaries, overlap rejection, both repeated DST hours, missing spring-forward hours, 23/25-hour daily accounting.
- Feasible buy-in renormalization, insufficient balance rejection, and preservation of the 5% irregular-size category weight.

## Limits of this milestone

These results validate the specification, schema, defaults and offline planner. They do not validate live command execution, queue delivery, telemetry ingestion, account lease recovery, buy-in retries, automatic daily rotation, coordinated breaks, post-BB/RIT protocol commands, or an Azure Function deployment. Those integration stages are explicitly listed in README.md and SPECIFICATION.md. No poker sessions were started during this milestone.
