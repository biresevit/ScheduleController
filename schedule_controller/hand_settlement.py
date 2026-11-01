"""Re-settle stored hand histories whose showdown was paid to the wrong player.

PDecision's writer used to compare only the first two players who showed and ignored side pots
and uncalled bets. ``correct_winnings`` rebuilds every pot of a hand from the text itself (blinds,
bets, folds, board, shown hands and the hero's dealt cards) and rewrites its "wins" lines:

* the main pot and each side pot go to the best hand among the players who put in enough for it;
* ties split evenly, the odd cent going to the first player left of the button;
* the part of a bet nobody could call is paid back to its bettor as its own "wins" line.

A hand is changed only when its board is complete and at least two remaining players' hands are
known (the hero's dealt cards count when the table hid them). Anything else is returned as is.
Omaha hands use exactly two hole cards, whatever the number of hole cards. No rake is taken,
matching PDecision's writer.
"""
from __future__ import annotations

from decimal import Decimal
from itertools import combinations
import re

HEADER = re.compile(r"^\*{5} Hand History for Game \d+ \*{5}\r?$", re.M)
MONEY = r"(?P<amount>\d+(?:\.\d+)?)"
SEAT = re.compile(r"^Seat\s+\d+:\s+(?P<player>.*?)\s+\(\s*\$[\d.]+\s+USD\s*\)", re.I)
PUT = re.compile(r"^(?P<player>.+?) (?:posts (?:small blind|big blind|ante)|calls|bets|raises) \[\$" + MONEY + r" USD\]", re.I)
ACTED = re.compile(r"^(?P<player>.+?) (?P<verb>folds|checks|calls|bets|raises)\b", re.I)
WIN = re.compile(r"^(?P<player>.+?) wins \$" + MONEY + r" USD(?:\s+from .+)?$", re.I)
SHOWS = re.compile(r"^(?P<player>.+?) shows \[(?P<cards>[^\]]*)\]", re.I)
DEALT = re.compile(r"^Dealt to (?P<player>.+?) \[(?P<cards>[^\]]*)\]", re.I)
BOARD = re.compile(r"^\*\* Dealing (?:Flop|Turn|River) \*\* \[(?P<cards>[^\]]*)\]", re.I)
CARD = re.compile(r"(10|[2-9TJQKA])([cdhs])", re.I)
RANKS = "23456789TJQKA"


def parse_cards(text: str) -> list[tuple[int, int]] | None:
    """Cards such as "Ac, Kd" or "AcKd"; None when anything other than cards is present."""
    compact = re.sub(r"[\s,]", "", text or "")
    cards, position = [], 0
    for match in CARD.finditer(compact):
        if match.start() != position:
            return None
        rank = 10 if match[1] == "10" else RANKS.index(match[1].upper()) + 2
        cards.append((rank, "cdhs".index(match[2].lower())))
        position = match.end()
    return cards if position == len(compact) else None


def _rank5(cards) -> tuple:
    counts: dict[int, int] = {}
    for rank, _ in cards:
        counts[rank] = counts.get(rank, 0) + 1
    groups = sorted(counts.items(), key=lambda g: (g[1], g[0]), reverse=True)
    ranks = [rank for rank, _ in groups]
    flush = len({suit for _, suit in cards}) == 1
    straight = 0
    if len(groups) == 5:
        if max(ranks) - min(ranks) == 4:
            straight = max(ranks)
        elif sorted(ranks) == [2, 3, 4, 5, 14]:
            straight = 5
    shape = [count for _, count in groups]
    if straight and flush:
        return (8, straight)
    if shape[0] == 4:
        return (7, *ranks)
    if shape[:2] == [3, 2]:
        return (6, *ranks)
    if flush:
        return (5, *ranks)
    if straight:
        return (4, straight)
    if shape[0] == 3:
        return (3, *ranks)
    if shape[:2] == [2, 2]:
        return (2, *ranks)
    if shape[0] == 2:
        return (1, *ranks)
    return (0, *ranks)


def evaluate(hole, board, omaha: bool) -> tuple | None:
    """Best hand value (higher is better, equal ties), or None when the cards are not a hand."""
    if hole is None or board is None or len(board) != 5:
        return None
    if omaha:
        if len(hole) < 4:
            return None
        return max(_rank5(two + three) for two in combinations(hole, 2) for three in combinations(board, 3))
    if len(hole) != 2:
        return None
    return max(_rank5(five) for five in combinations(hole + board, 5))


def settle(contributions: dict[str, Decimal], live: set[str], strength: dict[str, tuple],
           seat_order: list[str]) -> list[tuple[int, str, Decimal]] | None:
    """(pot, player, amount) awards; pot 0 is the main pot. None if a contested pot has no known hand."""
    pots: list[list] = []
    previous = Decimal(0)
    for level in sorted({v for v in contributions.values() if v > 0}):
        amount = sum(min(v, level) - min(v, previous) for v in contributions.values())
        eligible = [p for p in seat_order if p in live and contributions.get(p, 0) >= level]
        previous = level
        if not eligible and pots:
            pots[-1][0] += amount
        elif pots and pots[-1][1] == eligible:
            pots[-1][0] += amount
        else:
            pots.append([amount, eligible])
    if not pots or not pots[0][1]:
        return None
    awards = []
    for index, (amount, eligible) in enumerate(pots):
        if len(eligible) == 1:
            winners = eligible
        else:
            shown = [p for p in eligible if p in strength]
            if not shown:
                return None
            best = max(strength[p] for p in shown)
            winners = [p for p in shown if strength[p] == best]
        cents = int((amount * 100).to_integral_value())
        each, odd = divmod(cents, len(winners))
        for i, player in enumerate(winners):
            share = each + (1 if i < odd else 0)
            if share:
                awards.append((index, player, Decimal(share) / 100))
    return awards


def _settle_hand(lines: list[str]) -> list[str] | None:
    seats, contributions, folded, acted = [], {}, set(), set()
    board, shows, dealt, omaha_header = [], {}, None, False
    for raw in lines:
        line = raw.strip()
        if "Omaha" in line and "$" in line and "/" in line:
            omaha_header = True
        if match := SEAT.match(line):
            seats.append(match["player"])
            continue
        if match := PUT.match(line):
            contributions[match["player"]] = contributions.get(match["player"], Decimal(0)) + Decimal(match["amount"])
        if match := ACTED.match(line):
            acted.add(match["player"])
            if match["verb"].lower() == "folds":
                folded.add(match["player"])
        if match := BOARD.match(line):
            board += parse_cards(match["cards"]) or [None]
        elif match := SHOWS.match(line):
            shows[match["player"]] = parse_cards(match["cards"])
        elif match := DEALT.match(line):
            dealt = (match["player"], parse_cards(match["cards"]))

    if not seats or len(board) != 5 or None in board:
        return None
    known = [cards for cards in shows.values() if cards] + ([dealt[1]] if dealt and dealt[1] else [])
    omaha = omaha_header or any(len(cards) >= 4 for cards in known)
    # The writer lists the button first; the odd cent goes to the first player after it.
    seat_order = seats[1:] + seats[:1]
    contributions = {p: contributions.get(p, Decimal(0)) for p in seat_order} | contributions
    live = {p for p in seat_order if p not in folded and (p in acted or contributions.get(p, 0) > 0)}
    if len(live) < 2:
        return None
    strength = {}
    for player in live:
        value = evaluate(shows.get(player), board, omaha)
        if value is None and dealt and dealt[0] == player:
            value = evaluate(dealt[1], board, omaha)
        if value is not None:
            strength[player] = value
    if len(strength) < 2:
        return None
    awards = settle(contributions, live, strength, seat_order)
    if awards is None:
        return None
    side_pots = any(pot > 0 for pot, _, _ in awards)
    result = []
    for pot, player, amount in awards:
        label = "" if not side_pots else " from the main pot" if pot == 0 else f" from the side pot {pot}"
        result.append(f"{player} wins ${amount.normalize():f} USD{label}")
    return result


def balance(contributions: dict, prizes: dict, seated: set, folded: set, big_blind: Decimal) -> tuple[dict, dict]:
    """Make one parsed hand pay out what was put in, for hands the bot captured incompletely.

    Keys are case-folded player names; both dicts are changed in place and returned.

    1. A win credited to a name that is not seated (a placeholder such as "player3") goes to the
       only player who put money in and did not fold, when there is exactly one.
    2. The top contributor who won nothing gets back the part of the bet nobody matched.
    3. A hand that still pays out more than was put in credited an uncalled bet to the wrong
       player: the largest prize is reduced by exactly the excess.
    """
    unknown = [p for p in prizes if p not in seated]
    if unknown:
        remaining = [p for p, v in contributions.items() if v > 0 and p in seated and p not in folded]
        if len(remaining) == 1:
            for p in unknown:
                prizes[remaining[0]] = prizes.get(remaining[0], Decimal(0)) + prizes.pop(p)
    ranked = sorted(contributions, key=contributions.get, reverse=True)
    if (len(ranked) >= 2 and ranked[0] not in prizes
            and contributions[ranked[0]] > contributions[ranked[1]] >= big_blind):
        contributions[ranked[0]] = contributions[ranked[1]]
    excess = sum(prizes.values(), Decimal(0)) - sum(contributions.values(), Decimal(0))
    if excess > 0 and prizes:
        top = max(prizes, key=prizes.get)
        prizes[top] = max(Decimal(0), prizes[top] - excess)
    return contributions, prizes


def correct_winnings(text: str) -> str:
    """Return the hand history with each settleable showdown's "wins" lines recomputed."""
    starts = [m.start() for m in HEADER.finditer(text)]
    if not starts:
        return text
    bounds = starts + [len(text)]
    pieces = [text[:starts[0]]]
    for start, end in zip(bounds, bounds[1:]):
        chunk = text[start:end]
        lines = chunk.splitlines(keepends=True)
        corrected = _settle_hand(lines)
        if corrected is None:
            pieces.append(chunk)
            continue
        newline = "\r\n" if "\r\n" in chunk else "\n"
        out, inserted = [], False
        for line in lines:
            if WIN.match(line.strip()):
                if not inserted:
                    out += [w + newline for w in corrected]
                    inserted = True
                continue
            out.append(line)
        if not inserted:
            # No wins line at all: add the settlement after the last showdown line.
            last = max((i for i, l in enumerate(out) if SHOWS.match(l.strip()) or "does not show" in l), default=len(out) - 1)
            out[last + 1:last + 1] = [w + newline for w in corrected]
        pieces.append("".join(out))
    return "".join(pieces)
