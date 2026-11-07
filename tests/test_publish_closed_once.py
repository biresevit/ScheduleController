import asyncio
import tempfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from types import SimpleNamespace

from schedule_controller.pilot import Pilot


class Repo:
    def __init__(self):self.calls=[]
    def publish(self,tables,seats,entries,sessions,spans,**kw):self.calls.append(([e['id'] for e in entries],[s['id'] for s in sessions]))


def test_closed_entries_and_sessions_are_written_once_active_ones_every_time():
    start=datetime(2026,11,6,tzinfo=timezone.utc)
    repo=Repo()
    p=SimpleNamespace(publish_lock=asyncio.Lock(),accrue=lambda:None,intervals=[],repo=repo,tables=[],seats={},
        entries={'a':{'id':'a','account':1,'status':'seated'},'b':{'id':'b','account':2,'status':'closed'}},
        sessions=[{'id':'s1','account':1},{'id':'s2','account':2,'closed':start}],
        next_join=None,next_leave=None,last_plan=None,output=Path(tempfile.mkdtemp()),
        args=SimpleNamespace(target=1),epoch='e',policies={},clients={},errors=0,end=start+timedelta(hours=1))
    run=lambda:asyncio.run(Pilot.publish(p))
    run();run()
    assert repo.calls==[(['a','b'],['s1','s2']),(['a'],['s1'])]
    p.entries['a']['status']='closed'
    run();run()
    assert repo.calls[2:]==[(['a'],['s1']),([],['s1'])]
