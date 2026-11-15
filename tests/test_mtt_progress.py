from schedule_controller.cloud_scheduler import mtt_progress


def test_repeated_entries_count_once_per_account_and_tournament():
    class DB:
        def rows(self, sql, site_id):
            return [
                dict(TableId='T1', BotAccountId=1, TournamentRebuy=False, TournamentAddon=None),
                dict(TableId='T1', BotAccountId=1, TournamentRebuy=True, TournamentAddon=True),
                dict(TableId='T1', BotAccountId=1, TournamentRebuy=True, TournamentAddon=True),
                dict(TableId='T1', BotAccountId=2, TournamentRebuy=True, TournamentAddon=False),
                dict(TableId='T2', BotAccountId=1, TournamentRebuy=True, TournamentAddon=True),
            ]

    progress = mtt_progress(DB(), 1006)
    first, second = progress['T1'], progress['T2']
    assert (first.registered, first.rebuy, first.addon, first.accounts) == (2, 2, 1, {1, 2})
    assert (second.registered, second.rebuy, second.addon, second.accounts) == (1, 1, 1, {1})
