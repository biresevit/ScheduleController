"""One-time hand-history parsing and idempotent SQL result persistence."""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import logging
import os
import re

from .database import connection_string
from .hand_settlement import correct_winnings

log = logging.getLogger(__name__)
# 2: showdowns re-settled from the cards (correct_winnings); older results are reprocessed.
PARSER_VERSION = 2
MONEY = r"(?P<amount>\d+(?:\.\d+)?)"
ACTION = re.compile(r"^(?P<player>.+?) (?:posts (?:small blind|big blind|ante)|calls|bets|raises) \[\$" + MONEY + r" USD\]", re.I)
WIN = re.compile(r"^(?P<player>.+?) wins \$" + MONEY + r" USD(?:\s+from .+)?$", re.I)
SEAT = re.compile(r"^Seat\s+\d+:\s+(?P<player>.*?)\s+\(\s*\$[\d.]+\s+USD\s*\)", re.I)


def player_results(text: str) -> list[dict]:
    """Return every seated player's invested, won and net amounts in one pass."""
    text = correct_winnings(text)
    sections = re.split(r"\*{5} Hand History for Game \d+ \*{5}", text)
    if len(sections) == 1:
        raise ValueError("Unsupported hand-history format")
    combined: dict[str, dict] = {}
    for hand in sections[1:]:
        contributions, prizes, names = defaultdict(Decimal), defaultdict(Decimal), {}
        bb_match = re.search(r"^\$[\d.]+/\$([\d.]+)", hand, re.M)
        if not bb_match:
            raise ValueError("Missing big blind")
        bb = Decimal(bb_match[1])
        for raw in hand.splitlines():
            line = raw.strip()
            if match := SEAT.match(line):
                names.setdefault(match["player"].casefold(), match["player"])
            if match := ACTION.match(line):
                contributions[match["player"].casefold()] += Decimal(match["amount"])
                continue
            if match := WIN.match(line):
                prizes[match["player"].casefold()] += Decimal(match["amount"])
        if not names or not contributions or not prizes:
            raise ValueError("Incomplete hand settlement")
        # Preserve PDecision's correction for an unmatched all-in overbet.
        ranked = sorted(contributions, key=contributions.get, reverse=True)
        if (len(ranked) >= 2 and ranked[0] not in prizes
                and contributions[ranked[0]] > contributions[ranked[1]] >= bb):
            contributions[ranked[0]] = contributions[ranked[1]]
        for key, display_name in names.items():
            row = combined.setdefault(key, {"player_name": display_name,
                                            "invested": Decimal(0), "won": Decimal(0)})
            row["invested"] += contributions[key]
            row["won"] += prizes[key]
    return [{**row, "net": row["won"] - row["invested"]} for row in combined.values()]


class HandHistoryProcessor:
    def __init__(self, connect=None, container=None):
        self._connect = connect
        self._container_client = container

    def connect(self):
        if self._connect:
            return self._connect()
        import pyodbc
        return pyodbc.connect(connection_string(), autocommit=False, timeout=15)

    def container(self):
        if self._container_client is not None:
            return self._container_client
        from azure.storage.blob import ContainerClient
        url = os.environ.get("HAND_HISTORY_CONTAINER_URL")
        if not url:
            raise RuntimeError("HAND_HISTORY_CONTAINER_URL is not configured")
        self._container_client = ContainerClient.from_container_url(
            url, connection_timeout=5, read_timeout=15, retry_total=2)
        return self._container_client

    def claim(self, limit=100) -> list[dict]:
        conn = self.connect()
        try:
            c = conn.cursor()
            c.execute("SET XACT_ABORT ON")
            # Materialize status rows incrementally; this is also the automatic
            # backfill path for histories uploaded before this feature existed.
            c.execute("""INSERT reporting.HandProcessing(HandHistoryId,ParserVersion)
                SELECT TOP (?) h.Id,? FROM dbo.CustomerHandHistory h
                WHERE NOT EXISTS(SELECT 1 FROM reporting.HandProcessing p WITH(UPDLOCK,HOLDLOCK)
                                 WHERE p.HandHistoryId=h.Id)
                ORDER BY h.Id""", limit * 2, PARSER_VERSION)
            # Rows recorded by an older parser are claimed again, whatever their status, so a
            # parser fix corrects existing results; they start a fresh attempt count.
            ids = [r[0] for r in c.execute(""";WITH work AS(
                    SELECT TOP (?) p.* FROM reporting.HandProcessing p WITH(UPDLOCK,READPAST,ROWLOCK)
                    WHERE ((p.Status='pending' OR (p.Status='failed' AND p.AttemptCount<5)) AND p.ParserVersion=?)
                       OR (p.ParserVersion<? AND p.Status<>'processing')
                       OR (p.Status='processing' AND p.LeaseUntilUtc<SYSUTCDATETIME())
                    ORDER BY p.HandHistoryId)
                UPDATE work SET Status='processing',
                    AttemptCount=CASE WHEN ParserVersion<? THEN 1 ELSE AttemptCount+1 END,ParserVersion=?,
                    LeaseUntilUtc=DATEADD(minute,5,SYSUTCDATETIME()),LastError=NULL,UpdatedUtc=SYSUTCDATETIME()
                OUTPUT inserted.HandHistoryId""", limit, PARSER_VERSION, PARSER_VERSION,
                PARSER_VERSION, PARSER_VERSION).fetchall()]
            if not ids:
                conn.commit()
                return []
            placeholders = ",".join("?" for _ in ids)
            c.execute(f"""SELECT h.Id,h.CustomerId,h.PokerSite,h.GameType,h.BlobPath,
                    s.SiteId,s.Casino
                FROM dbo.CustomerHandHistory h
                OUTER APPLY(SELECT TOP(1) x.SiteId,x.Casino FROM scheduler.Site x
                    WHERE x.CustomerId=h.CustomerId
                      AND h.BlobPath LIKE CONVERT(varchar(20),h.CustomerId)+'/'+x.Casino+'/%') s
                WHERE h.Id IN ({placeholders}) ORDER BY h.Id""", *ids)
            names = [d[0] for d in c.description]
            rows = [dict(zip(names, row)) for row in c.fetchall()]
            conn.commit()
            return rows
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bot_accounts(self, site_ids) -> dict[int, dict[str, int]]:
        site_ids = sorted({int(i) for i in site_ids if i is not None})
        if not site_ids:
            return {}
        conn = self.connect()
        try:
            placeholders = ",".join("?" for _ in site_ids)
            c = conn.cursor().execute(f"""SELECT p.SiteId,b.BotAccountId,b.Username
                FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId
                WHERE p.SiteId IN ({placeholders})""", *site_ids)
            result: dict[int, dict[str, int]] = defaultdict(dict)
            for site_id, account_id, username in c.fetchall():
                result[int(site_id)][str(username).casefold()] = int(account_id)
            return dict(result)
        finally:
            conn.close()

    def _download_and_parse(self, row):
        payload = self.container().get_blob_client(row["BlobPath"]).download_blob().readall()
        if len(payload) > 262144:
            raise ValueError("Hand-history file exceeds 256 KiB")
        return player_results(payload.decode("utf-8-sig"))

    def complete(self, row, results, bots):
        bot_map = bots.get(row.get("SiteId"), {})
        conn = self.connect()
        try:
            c = conn.cursor(); c.execute("SET XACT_ABORT ON")
            c.execute("DELETE reporting.HandPlayerResult WHERE HandHistoryId=?", row["Id"])
            humans = 0
            for result in results:
                account_id = bot_map.get(result["player_name"].casefold())
                is_bot = account_id is not None
                humans += 0 if is_bot else 1
                c.execute("""INSERT reporting.HandPlayerResult
                    (HandHistoryId,PlayerName,BotAccountId,IsBot,AmountInvested,AmountWon)
                    VALUES(?,?,?,?,?,?)""", row["Id"], result["player_name"], account_id,
                          is_bot, result["invested"], result["won"])
            c.execute("""UPDATE reporting.HandProcessing SET Status='completed',LeaseUntilUtc=NULL,
                PlayerCount=?,HumanCount=?,ProcessedUtc=SYSUTCDATETIME(),UpdatedUtc=SYSUTCDATETIME(),LastError=NULL
                WHERE HandHistoryId=?""", len(results), humans, row["Id"])
            conn.commit()
        except BaseException:
            conn.rollback(); raise
        finally:
            conn.close()

    def fail(self, history_id, error):
        conn = self.connect()
        try:
            conn.cursor().execute("""UPDATE reporting.HandProcessing SET Status='failed',LeaseUntilUtc=NULL,
                LastError=?,UpdatedUtc=SYSUTCDATETIME() WHERE HandHistoryId=?""",
                str(error)[:500], history_id)
            conn.commit()
        finally:
            conn.close()

    def tick(self, limit=100):
        rows = self.claim(limit)
        if not rows:
            return {"claimed": 0, "completed": 0, "failed": 0}
        bots = self.bot_accounts(row.get("SiteId") for row in rows)
        self.container()  # construct once before worker threads
        with ThreadPoolExecutor(max_workers=12) as pool:
            parsed = list(pool.map(self._safe_parse, rows))
        completed, failed = self._persist_batch(parsed, bots)
        return {"claimed": len(rows), "completed": completed, "failed": failed}

    def _persist_batch(self, parsed, bots):
        """Write the whole claimed batch with one SQL connection/transaction."""
        conn = self.connect(); completed = failed = 0
        try:
            c = conn.cursor(); c.execute("SET XACT_ABORT ON")
            for row, results, error in parsed:
                if error is not None:
                    c.execute("""UPDATE reporting.HandProcessing SET Status='failed',LeaseUntilUtc=NULL,
                        LastError=?,UpdatedUtc=SYSUTCDATETIME() WHERE HandHistoryId=?""",
                        str(error)[:500], row["Id"])
                    failed += 1
                    continue
                bot_map = bots.get(row.get("SiteId"), {})
                c.execute("DELETE reporting.HandPlayerResult WHERE HandHistoryId=?", row["Id"])
                humans = 0
                for result in results:
                    account_id = bot_map.get(result["player_name"].casefold())
                    is_bot = account_id is not None
                    humans += 0 if is_bot else 1
                    c.execute("""INSERT reporting.HandPlayerResult
                        (HandHistoryId,PlayerName,BotAccountId,IsBot,AmountInvested,AmountWon)
                        VALUES(?,?,?,?,?,?)""", row["Id"], result["player_name"], account_id,
                              is_bot, result["invested"], result["won"])
                c.execute("""UPDATE reporting.HandProcessing SET Status='completed',LeaseUntilUtc=NULL,
                    PlayerCount=?,HumanCount=?,ProcessedUtc=SYSUTCDATETIME(),UpdatedUtc=SYSUTCDATETIME(),LastError=NULL
                    WHERE HandHistoryId=?""", len(results), humans, row["Id"])
                completed += 1
            conn.commit()
            return completed, failed
        except BaseException:
            conn.rollback(); raise
        finally:
            conn.close()

    def _safe_parse(self, row):
        try:
            return row, self._download_and_parse(row), None
        except Exception as exc:
            log.warning("Could not process hand %s: %s", row["Id"], exc)
            return row, None, exc
