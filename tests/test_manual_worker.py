"""Manual actions the table refuses or ignores free the turn; sit commands reach the table."""
import asyncio
import copy
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from poker_controller.bot import Bot
from poker_controller.client import EvenBetClient
from poker_controller.manual import options
from poker_controller.transport import ProtocolError
from schedule_controller import manual_worker
from schedule_controller.manual_worker import ManualBridge

FIXTURE = Path(__file__).parents[2] / "PokerController/tests/fixtures/hero_turn.json"
HERO = 2


class Repo:
    def __init__(self, commands=()):
        self.finished = []
        self.released = []
        self.commands = list(commands)
    def claim_table_action(self, *args): return True
    def finish_manual_action(self, action_id, status, detail): self.finished.append((status, detail))
    def release_turn(self, aid, tid, key): self.released.append(key)
    def claim_table_command(self, aid, tid): return self.commands.pop(0) if self.commands else None


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(manual_worker, "APPLY_SECONDS", 0.3)
    state = json.loads(FIXTURE.read_text())
    client = EvenBetClient()
    client.player_id = 73582719
    client.states[state["ti"]] = state
    repo = Repo()
    bridge = ManualBridge(repo, 1, state["ti"])
    bot = Bot(client, object(), state["ti"], control=bridge)
    key = bot.mapper.turn_key(state, HERO)
    command = {"ActionId": "a", "RequestId": "r", "TurnKey": key, "Action": "call"}
    return state, client, repo, bridge, bot, key, command


def test_refused_action_frees_the_turn_for_a_new_choice(setup):
    state, client, repo, bridge, bot, key, command = setup
    client.play = AsyncMock(side_effect=ProtocolError({"t": "Result", "text": "Wrong bet amount", "resultCode": 1}))
    asyncio.run(bridge.play(bot, state, HERO, key, command))
    assert repo.finished == [("failed", "The table refused this action: Wrong bet amount. Choose again.")]
    assert repo.released == [key] and key not in bot.submitted


def test_action_the_table_ignores_is_reported_and_freed(setup):
    state, client, repo, bridge, bot, key, command = setup
    client.play = AsyncMock()
    asyncio.run(bridge.play(bot, state, HERO, key, command))
    assert repo.finished[0][0] == "failed" and "did not apply" in repo.finished[0][1]
    assert repo.released == [key] and key not in bot.submitted


def test_applied_action_is_confirmed(setup):
    state, client, repo, bridge, bot, key, command = setup
    async def play(*args):
        moved = copy.deepcopy(state)
        moved["m"]["ai"] = 4
        client.states[state["ti"]] = moved
    client.play = play
    asyncio.run(bridge.play(bot, state, HERO, key, command))
    assert repo.finished == [("sent", "The table acknowledged the action.")]
    assert not repo.released and key in bot.submitted


def test_network_doubt_is_never_retried(setup):
    state, client, repo, bridge, bot, key, command = setup
    client.play = AsyncMock(side_effect=TimeoutError())
    asyncio.run(bridge.play(bot, state, HERO, key, command))
    assert repo.finished[0][0] == "uncertain" and not repo.released and key in bot.submitted


def test_detached_tournament_table_never_sends_a_queued_action(setup):
    state,client,repo,bridge,bot,key,command=setup
    bot.table_detached=True;client.play=AsyncMock()
    asyncio.run(bridge.play(bot,state,HERO,key,command))
    client.play.assert_not_awaited()
    assert repo.finished[0][0]=='expired'


def test_tournament_move_while_claiming_an_action_prevents_the_send(setup):
    state,client,repo,bridge,bot,key,command=setup
    def claim(*args):bot.table_detached=True;return True
    repo.claim_table_action=claim;client.play=AsyncMock()
    asyncio.run(bridge.play(bot,state,HERO,key,command))
    client.play.assert_not_awaited()
    assert repo.finished[0][0]=='expired'


def test_tournament_raise_uses_native_chips_and_the_tournament_entry_index(setup):
    state,client,repo,bridge,bot,key,command=setup
    client.tables[state['ti']]={'tournament':{'format':'SNG'}}
    client.tournament_entry_indices[state['ti']]=3
    client.connection.request=AsyncMock(return_value={})
    command.update(Action='raise',TargetMinor=600)
    bridge.applied=AsyncMock(return_value=True)
    asyncio.run(bridge.play(bot,state,HERO,key,command))
    client.connection.request.assert_awaited_once_with('Play',tableId=state['ti'],playerEntryIdx=3,action=9,funds=500)


def test_sit_out_and_im_back_commands_reach_the_table(setup):
    state, client, repo, bridge, bot, key, command = setup
    repo.commands = [{"ActionId": "s", "Action": "sit_out"}, {"ActionId": "b", "Action": "sit_in"}]
    client.sit_out_next_hand = AsyncMock()
    client.sit_back = AsyncMock()
    asyncio.run(bridge.table_commands(bot))
    client.sit_out_next_hand.assert_awaited_once_with(state["ti"])
    client.sit_back.assert_awaited_once_with(state["ti"])
    assert [s for s, _ in repo.finished] == ["sent", "sent"] and not bot.sit_out_requested


def test_refused_sit_command_is_reported(setup):
    state, client, repo, bridge, bot, key, command = setup
    repo.commands = [{"ActionId": "b", "Action": "sit_in"}]
    client.sit_back = AsyncMock(side_effect=ProtocolError({"t": "Result", "text": "Not away", "resultCode": 3}))
    asyncio.run(bridge.table_commands(bot))
    assert repo.finished == [("failed", "The table refused: Not away")]
