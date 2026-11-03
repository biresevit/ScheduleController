# Failed join and registration limit

A session can have at most two failed ring-table joins or SNG/MTT registrations,
across all its tables and task IDs. Successful joins do not reset the failure
count. Cancelled/expired commands, leaves and unregistrations do not count.
Failures are read from persisted command outcomes; a worker restart does not
reset the budget. A new session after the existing account cooldown starts a
new budget. There is no automatic replay of an ambiguous poker-server send.

At the second failure the session's planned end becomes now, its reason becomes
`join_failure_limit`, and queued joins are cancelled. Already-claimed joins must
pass the same gate under the login's execution lock before contacting the poker
server. Ring seats finish their current hand and leave; existing tournament
entries play to completion. Uncertain seats retain their lease for reconciliation.
The session closes and its lease releases only when no entries remain. The
normal five-minute cooldown (or the account's configured value) applies afterward;
normal placement can choose another available PC for the new session.

The Azure scheduler applies the limit to persisted failures even for older
workers. Updated workers also enforce it immediately, including tasks already
claimed before the second failure. Deploy both the scheduler function and the
signed PC worker release. The backoffice change explains the end reason and
provides a readable fallback for historical KeyError tasks without ResultText.

Worker failure text records action, username, table, stage, exception type and
message (up to 1,000 characters). The full traceback remains in worker.log.

The old ring join code looked up `self.info[table_id]` after awaiting login. A
concurrent lobby scan can replace that dictionary, causing KeyError before the
join reaches the poker server. The join now retains the table snapshot and, if
missing, fetches the logged-in account's lobby. It also avoids looking up the
mutable dictionary again after a successful sit-down.

Validation: offline execution tests cover concurrent queued commands, a persisted
failure from an earlier worker, the shared ring/registration budget, graceful
draining, logout and the lobby-refresh race. The SQL test runs the real policy
statements against connection-local temporary tables; it never changes live rows.
