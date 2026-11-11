import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from schedule_controller.chat_worker import run_chat


def test_uuid_chat_is_persisted_once_and_explicit_send_uses_same_table_key():
    async def go():
        table='c64012de-493e-4a35-a760-1923f1369b70'
        ready=asyncio.Event();ready.set()
        clients=[]
        for _ in range(2):
            events=asyncio.Queue();events.put_nowait({'table_id':table,'body':'hello'})
            clients.append(SimpleNamespace(hero_seat=lambda _:0,states={},read_chat=AsyncMock(),
                chat_events=events,chat_overflow=False,send_chat=AsyncMock()))
        persisted=[];claims=iter([{'BotAccountId':2,'TableId':table,'Body':'hi','RequestId':'request'}])
        def exchange(messages,*,claim=False):
            persisted.extend(messages)
            assert claim
            try:return next(claims)
            except StopIteration:raise asyncio.CancelledError()
        repo=SimpleNamespace(chat_exchange=exchange,finish_chat=lambda *args:None)
        pilot=SimpleNamespace(repo=repo,clients={i:SimpleNamespace(ready=ready,client=c,queues={table:None}) for i,c in enumerate(clients,1)})
        try:await run_chat(pilot)
        except asyncio.CancelledError:pass
        assert len(persisted)==1
        clients[1].send_chat.assert_awaited_once_with(table,'hi')
    asyncio.run(go())
