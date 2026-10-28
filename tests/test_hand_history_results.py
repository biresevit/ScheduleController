from decimal import Decimal

import pytest

from schedule_controller.hand_history_results import player_results


HAND = """***** Hand History for Game 162968912 *****
$1/$2 USD PLO
Seat 1: monica ( $200 USD )
Seat 2: demoOpponent ( $200 USD )
monica posts big blind [$2 USD]
demoOpponent raises [$7 USD]
monica calls [$5 USD]
monica bets [$7 USD]
demoOpponent calls [$7 USD]
monica wins $28 USD
"""


def test_parses_every_player_in_one_pass():
    rows = {row["player_name"]: row for row in player_results(HAND)}
    assert rows["monica"] == {"player_name": "monica", "invested": Decimal(14),
                               "won": Decimal(28), "net": Decimal(14)}
    assert rows["demoOpponent"] == {"player_name": "demoOpponent", "invested": Decimal(14),
                                    "won": Decimal(0), "net": Decimal(-14)}


def test_rejects_incomplete_settlement_instead_of_recording_false_zeroes():
    with pytest.raises(ValueError, match="Incomplete hand settlement"):
        player_results(HAND.replace("monica wins $28 USD", ""))


def test_combined_history_accumulates_each_player():
    rows = {row["player_name"]: row for row in player_results(
        HAND + HAND.replace("162968912", "162968913"))}
    assert rows["monica"]["net"] == Decimal(28)
    assert rows["demoOpponent"]["net"] == Decimal(-28)
