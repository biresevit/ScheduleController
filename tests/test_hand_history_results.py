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


@pytest.mark.parametrize("hand_id", [
    "76da2b9c-881c-4506-a063-16642d54da08",
    "76da2b9c-881c-4506-a063-16642d54da08-27",
    "76da2b9c-881c-4506-a063-16642d",
])
def test_bluffpoker_ids_preserve_settlement_and_mixed_history_totals(hand_id):
    bluff = HAND.replace("162968912", hand_id).replace("\n", "\r\n")
    assert player_results(bluff) == player_results(HAND)
    rows = {r["player_name"]: r for r in player_results(HAND + bluff)}
    assert rows["monica"]["net"] == Decimal(28)
    assert rows["demoOpponent"]["net"] == Decimal(-28)


def test_localized_bluffpoker_currency_and_doubled_carriage_returns():
    hand = """***** Hand History for Game 6ab84bec-ddf7-4f60-a11a-9502d1e13ef0-753 *****
$0,50/$1,00 USD fastforward NL Texas Hold'em
Seat 1: player3 ( $100 USD )
Seat 5: moody ( $96,87 USD )
Seat 6: josh ( $208,57 USD )
moody posts small blind [$0.50 USD]
josh posts big blind [$1 USD]
player3 folds
moody folds
josh wins $1,5 USD
""".replace("\n", "\r\r\n")
    rows = {r["player_name"]: r for r in player_results(hand)}
    assert rows["moody"]["net"] == Decimal("-0.5")
    assert rows["josh"]["net"] == Decimal("0.5")
    assert rows["player3"]["net"] == 0
