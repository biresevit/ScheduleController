"""Chat runs independently of poker decisions and never retries a claimed send."""
import asyncio
import logging

from poker_controller.sites import table_key

log=logging.getLogger(__name__)


async def run_chat(pilot):
    watched=set()
    pending=[]
    while True:
        try:
            readers={}
            for aid,session in sorted(pilot.clients.items()):
                if not session.ready.is_set(): continue
                client=session.client
                for tid in session.queues:
                    if client.hero_seat(client.states.get(tid,{})) is not None:
                        readers.setdefault(tid,(aid,client))
            for tid,(_,client) in readers.items():
                key=(id(client),tid)
                if key not in watched:
                    await client.read_chat(tid)
                    watched.add(key)
            for aid,session in list(pilot.clients.items()):
                client=session.client
                if client.chat_overflow:
                    if getattr(client,'supports_chat_history',True):
                        log.warning('Chat buffer overflow for account %s; requesting history',aid)
                    else:
                        log.warning('Live chat buffer overflow for account %s; dropped messages cannot be recovered (no server history)',aid)
                    client.chat_overflow=False
                    watched={key for key in watched if key[0]!=id(client)}
                while not client.chat_events.empty() and len(pending)<500:
                    message=client.chat_events.get_nowait()
                    reader=readers.get(table_key(message['table_id']))
                    if reader and reader[0]==aid:
                        pending.append(message)
            command=await asyncio.to_thread(pilot.repo.chat_exchange,pending,claim=True)
            pending.clear()
            if command:
                session=pilot.clients.get(command['BotAccountId'])
                try:
                    if not session or not session.ready.is_set():
                        raise ValueError('The selected bot is not connected to this table.')
                    await session.client.send_chat(table_key(command['TableId']),command['Body'])
                    status,detail='sent','Confirmed in table chat.'
                except ValueError as exc:
                    status,detail='failed',str(exc)[:1000]
                except Exception:
                    status,detail='uncertain','No chat confirmation was received. Check the table chat before trying again.'
                    log.exception('Chat send was not confirmed: %s',command['RequestId'])
                await asyncio.to_thread(pilot.repo.finish_chat,command['RequestId'],status,detail)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('Chat synchronization failed')
        await asyncio.sleep(3)
