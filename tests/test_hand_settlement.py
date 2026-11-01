"""Re-settling stored hand histories whose showdown was paid to the wrong player."""
from decimal import Decimal

from schedule_controller.hand_history_results import player_results
from schedule_controller.hand_settlement import correct_winnings, evaluate, parse_cards


def wins(text):
    lines = [l for l in text.splitlines() if " wins $" in l]
    return lines


def nets(text):
    return {r["player_name"]: r["net"] for r in player_results(text)}


# As PDecision wrote it: jeremy paid 476.3 although WashingtonS has a straight, and the 59.58
# jeremy could not call never returned to WashingtonS.
HAND_163026171 = """Game #163026171 starts.

#Game No : 163026171
***** Hand History for Game 163026171 *****
$1.00/$2.00 USD PL Omaha - Saturday, October 31, 00:40:55 CEST 2026
Table PLOmaha (Real Money)
Seat 1 is the button
Total number of players: 5/8
Seat 1: jeremy ( $193 USD )
Seat 3: WashingtonS ( $252.58 USD )
Seat 5: ScottocS ( $103.79 USD )
Seat 7: mason ( $99.2 USD )
Seat 8: monica ( $25.16 USD )
WashingtonS posts small blind [$1 USD]
ScottocS posts big blind [$2 USD]
** Dealing down cards **
Dealt to jeremy [  Ac As 3h 5c ]
mason folds
monica raises [$7 USD]
jeremy raises [$24 USD]
WashingtonS raises [$43.54 USD]
ScottocS raises [$63.14 USD]
monica calls [$18.16 USD]
jeremy raises [$169 USD]
WashingtonS raises [$208.04 USD]
ScottocS folds
** Dealing Flop ** [ 4s, 6c, Kh ]
** Dealing Turn ** [ 3s ]
** Dealing River ** [ 9d ]
jeremy does not show cards.
WashingtonS shows [  2h, 2d, 2c, 5d ]
monica shows [  7c, Ks, Qc, Ts ]
jeremy wins $476.3 USD
"""


def test_hand_163026171_pays_the_straight_every_pot_and_the_uncalled_bet():
    fixed = correct_winnings(HAND_163026171)
    assert wins(fixed) == ["WashingtonS wins $100.64 USD from the main pot",
                           "WashingtonS wins $375.66 USD from the side pot 1",
                           "WashingtonS wins $59.58 USD from the side pot 2"]
    assert nets(HAND_163026171) == {"jeremy": Decimal("-193"), "WashingtonS": Decimal("283.30"),
                                    "ScottocS": Decimal("-65.14"), "mason": Decimal(0),
                                    "monica": Decimal("-25.16")}


def test_correction_is_idempotent_and_keeps_everything_else():
    once = correct_winnings(HAND_163026171)
    assert correct_winnings(once) == once
    assert [l for l in once.splitlines() if " wins $" not in l] == \
           [l for l in HAND_163026171.splitlines() if " wins $" not in l]


NLH_THREE_WAY = """***** Hand History for Game 5 *****
$1.00/$2.00 USD fastforward NL Texas Hold'em - Sunday, November 01, 10:00:00 CEST 2026
Table T (Real Money)
Seat 1 is the button
Total number of players: 4/6
Seat 1: hero ( $200 USD )
Seat 2: sb ( $200 USD )
Seat 3: bb ( $200 USD )
Seat 4: utg ( $200 USD )
sb posts small blind [$1 USD]
bb posts big blind [$2 USD]
** Dealing down cards **
Dealt to hero [  Qd Qc ]
utg calls [$2 USD]
hero folds
sb calls [$1 USD]
bb checks
** Dealing Flop ** [ Kh, 7d, 2c ]
sb checks
bb checks
utg checks
** Dealing Turn ** [ 9s ]
sb checks
bb checks
utg checks
** Dealing River ** [ 3h ]
sb checks
bb checks
utg checks
sb shows [  Ah, Kd ]
bb shows [  7c, 7s ]
utg shows [  9d, 9c ]
sb wins $6 USD
"""


def test_nlh_three_way_best_hand_wins_even_when_shown_last():
    assert wins(correct_winnings(NLH_THREE_WAY)) == ["utg wins $6 USD"]


def test_hidden_hero_cards_still_win_with_the_better_hand():
    text = (NLH_THREE_WAY.replace("Dealt to hero [  Qd Qc ]", "Dealt to hero [  Kc Ks ]")
            .replace("hero folds", "hero calls [$2 USD]")
            .replace("utg shows [  9d, 9c ]\n", "hero does not show cards.\n"))
    # hero: set of kings beats sb's pair of kings and bb's set of sevens. Pot 2+2+2+2.
    assert wins(correct_winnings(text)) == ["hero wins $8 USD"]


def test_split_pot_odd_cent_goes_left_of_the_button():
    text = (NLH_THREE_WAY.replace("** Dealing Flop ** [ Kh, 7d, 2c ]", "** Dealing Flop ** [ Ah, Kd, Qc ]")
            .replace("** Dealing Turn ** [ 9s ]", "** Dealing Turn ** [ Js ]")
            .replace("** Dealing River ** [ 3h ]", "** Dealing River ** [ 2h ]")
            .replace("** Dealing down cards **", "hero posts ante [$0.01 USD]\n** Dealing down cards **")
            .replace("sb shows [  Ah, Kd ]", "sb shows [  Tc, 3d ]")
            .replace("bb shows [  7c, 7s ]", "bb shows [  Th, 4c ]")
            .replace("utg shows [  9d, 9c ]", "utg shows [  5d, 5c ]"))
    # sb and bb both play the Broadway straight; 6.01 is shared, sb is first after the button.
    assert wins(correct_winnings(text)) == ["sb wins $3.01 USD", "bb wins $3 USD"]


PLO5_THREE_WAY = """***** Hand History for Game 7 *****
$1.00/$2.00 USD fastforward NL Texas Hold'em - Sunday, November 01, 10:00:00 CEST 2026
Table T (Real Money)
Seat 1 is the button
Total number of players: 3/6
Seat 1: hero ( $100 USD )
Seat 2: sb ( $100 USD )
Seat 3: bb ( $40 USD )
sb posts small blind [$1 USD]
bb posts big blind [$2 USD]
** Dealing down cards **
Dealt to hero [  Ks Kd Qc 8h 7h ]
hero raises [$100 USD]
sb calls [$99 USD]
bb calls [$38 USD]
** Dealing Flop ** [ 9c, 9d, 5s ]
** Dealing Turn ** [ Jh ]
** Dealing River ** [ 2c ]
hero does not show cards.
sb does not show cards.
bb does not show cards.
hero wins $240 USD
"""


def test_plo5_written_with_a_holdem_header_is_settled_as_omaha_with_side_pots():
    text = (PLO5_THREE_WAY.replace("sb does not show cards.", "sb shows [  As, Ah, Qd, 6c, 3d ]")
            .replace("bb does not show cards.", "bb shows [  9s, 5d, 4c, 4h, 3s ]"))
    # bb: nines full of fives wins the main pot 3 x 40; sb's aces beat hero's kings for the side pot.
    assert wins(correct_winnings(text)) == ["bb wins $120 USD from the main pot",
                                            "sb wins $120 USD from the side pot 1"]


def test_hands_without_two_known_showdown_hands_are_left_alone():
    # Nobody but the hero is known here, so the hand stays exactly as written.
    assert correct_winnings(PLO5_THREE_WAY) == PLO5_THREE_WAY
    folded = NLH_THREE_WAY.split("** Dealing Flop **")[0] + "sb folds\nbb folds\nutg wins $5 USD\n"
    assert correct_winnings(folded) == folded


def test_every_hand_of_a_combined_file_is_corrected():
    text = HAND_163026171 + NLH_THREE_WAY
    fixed = correct_winnings(text)
    assert wins(fixed)[-1] == "utg wins $6 USD"
    assert sum(1 for l in wins(fixed) if l.startswith("WashingtonS")) == 3


def test_omaha_uses_exactly_two_hole_cards():
    board = parse_cards("Ah Kh Qh Jh 2c")
    one_heart = evaluate(parse_cards("Th 9s 3d 3c"), board, omaha=True)
    trips = evaluate(parse_cards("2d 2s 3d 4c"), board, omaha=True)
    assert one_heart > trips          # K-high straight, not a flush
    assert evaluate(parse_cards("Th 9s"), board, omaha=False)[0] == 8  # Hold'em: one heart is enough for the royal flush


def test_unknown_cards_are_not_a_hand():
    assert parse_cards("XxXx") is None
    assert parse_cards("10h Js") == [(10, 2), (11, 3)]


def test_stored_files_with_windows_line_endings_are_corrected_and_keep_them():
    text = HAND_163026171.replace("\n", "\r\n")
    fixed = correct_winnings(text)
    assert "WashingtonS wins $59.58 USD from the side pot 2\r\n" in fixed
    assert "jeremy wins" not in fixed and "\r\r" not in fixed


MISSING_RIVER_REPLY = """***** Hand History for Game 13890 *****
$0.50/$1.00 USD fastforward NL Texas Hold'em - Sunday, November 01, 10:00:00 CEST 2026
Seat 1 is the button
Seat 1: john ( $245.11 USD )
Seat 2: andrew ( $139.62 USD )
Seat 4: monica ( $220.71 USD )
Seat 7: BarberShop ( $49.32 USD )
Seat 8: kate ( $319.85 USD )
andrew posts small blind [$0.50 USD]
monica posts big blind [$1 USD]
Dealt to kate [  7s 8h ]
BarberShop folds
kate folds
john raises [$2.5 USD]
andrew raises [$8 USD]
monica folds
john calls [$6 USD]
** Dealing Flop ** [ Qh, Tc, 8s ]
andrew bets [$7 USD]
john raises [$22 USD]
andrew calls [$15 USD]
** Dealing Turn ** [ Jc ]
andrew checks
john bets [$40 USD]
andrew calls [$40 USD]
** Dealing River ** [ Kd ]
andrew checks
john bets [$174.61 USD]
john does not show cards.
andrew does not show cards.
andrew wins $316.61 USD
"""


def test_uncalled_bet_paid_to_the_wrong_player_is_returned_and_hand_balances():
    rows = {r["player_name"]: r for r in player_results(MISSING_RIVER_REPLY)}
    # The pot before the river bet is 70.5 + 70.5 + monica's 1 = 142; john's 174.61 was never called.
    assert rows["andrew"]["won"] == Decimal("142.00") and rows["andrew"]["net"] == Decimal("71.50")
    assert rows["john"]["net"] == Decimal("-70.5")
    assert sum(r["net"] for r in rows.values()) == 0


PLACEHOLDER_WINNER = """***** Hand History for Game 13489 *****
$1.00/$2.00 USD fastforward NL Texas Hold'em - Sunday, November 01, 10:00:00 CEST 2026
Seat 1 is the button
Seat 1: Yhannson ( $300 USD )
Seat 2: john ( $200 USD )
Seat 3: Hartginia ( $300 USD )
Seat 4: andrew ( $300 USD )
john posts small blind [$1 USD]
Hartginia posts big blind [$2 USD]
Dealt to john [  Ad 9h ]
Yhannson calls [$2 USD]
andrew raises [$10 USD]
john folds
Hartginia calls [$8 USD]
Yhannson raises [$33.09 USD]
andrew calls [$25.09 USD]
Hartginia raises [$64.45 USD]
Yhannson folds
andrew folds
Hartginia does not show cards.
player3 wins $145.63 USD
"""


def test_win_credited_to_a_placeholder_name_goes_to_the_only_player_left():
    rows = {r["player_name"]: r for r in player_results(PLACEHOLDER_WINNER)}
    assert rows["Hartginia"]["won"] == Decimal("145.63") and rows["Hartginia"]["net"] == Decimal("71.18")
    assert "player3" not in rows
    assert sum(r["net"] for r in rows.values()) == 0
