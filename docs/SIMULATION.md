# Timeline simulation

`schedule_controller.timeline.simulate(config, bands, currency)` is a pure discrete-event simulator around `planner.plan`. It does not call poker services, queues or SQL. PokerBackoffice exposes it on the Schedule simulator page via its customer-scoped API. Build the backoffice with `tools/build.py` after changing this shared code.

The target takes effect at `start_local` in the chosen IANA timezone. Existing occupied entries are grouped into account sessions, each with its own start and remaining duration. Optional `existing_sessions` overrides generated sessions and must sum to `current`.

At each timer tick, planner reservations count against pending machine/table capacity. Joins are paced and confirm after a configurable network delay. An account's first confirmed seat starts its session; all its tables share that session's planned end. At that time pending joins are cancelled and occupied seats finish hands before leaving. The planner then fills the shortage according to stake allocations and available accounts/capacity. Daily playtime counts once per account, not once per table. Session cooldown prevents an immediate rejoin.

Outputs include per-event relative seconds and absolute timestamps, occupancy chart points, session start/planned/actual ends, first target-reaching time, and time spent exactly at target. The seed makes runs reproducible. No failures, exhausted balances or outside player churn are modelled; results are scenarios, not promises of live throughput.

The default 20→56 scenario, seed 7, four 20-entry machines, two-minute checks, at most three joins/check, 20–60-second join gaps, five-second confirmations and initial sessions with 15–60 minutes remaining first reaches 56 after 30m34s. Over two hours it has 68 joins and 33 departures, ending on 55. Ongoing paced replacements explain temporary target shortfalls.

Migration `002_sessions.sql` adds separate per-account session duration/cooldown settings and `AccountSession`, with StartedUtc/PlannedEndUtc/ActualEndUtc and a unique open session per account. Entry.SessionId links all account tables to one machine-bound session. Nullable Entry.SessionId accommodates older entries during rollout. The live worker must populate this relationship and enforce planned ends before scheduling is enabled.
