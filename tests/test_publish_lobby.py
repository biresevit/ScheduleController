"""Lobby rows are shared by every PC, so they are written in one batch outside the journal."""
import json
from contextlib import contextmanager
from datetime import datetime,timezone
from decimal import Decimal

import pytest

from schedule_controller.pilot_repository import LOBBY_SQL,Repository

EPOCH='0f0e0d0c-0b0a-0908-0706-050403020100'


class Recorder:
    def __init__(self,epoch=EPOCH):self.calls=[];self.epoch=epoch;self.transactions=0
    def one(self,sql,*args):
        if 'MachineAssignment' in sql:return {'WorkerEpoch':self.epoch.upper()}
        return None
    def rows(self,sql,*args):return []
    def execute(self,sql,*args):self.calls.append((sql,args))


def repository(db):
    r=Repository(EPOCH,1,7)
    @contextmanager
    def transaction():
        db.transactions+=1
        yield db
    r.transaction=transaction
    return r


def lobby():
    now=datetime(2026,10,31,17,0,tzinfo=timezone.utc)
    tables=[dict(id=128016534,name='T',game='NLH',bb=Decimal('0.02'),minimum=1,maximum=4,seats=6,occupied=2,open=True,observed=now,snapshot=None)]
    seats={128016534:[dict(seat=2,native_id=55,name='andrew',account=14536,stack=Decimal('2.10'),bet=0,in_game=True,away=False,hand=99,observed=now)]}
    return tables,seats


def test_publish_writes_the_lobby_in_one_batch_before_the_journal():
    db=Recorder();tables,seats=lobby()
    repository(db).publish(tables,seats,[],[],[])
    assert db.transactions==2
    batches=[args for sql,args in db.calls if sql==LOBBY_SQL]
    assert len(batches)==1 and not any('LobbyTable' in sql or 'TableSeat' in sql for sql,_ in db.calls if sql!=LOBBY_SQL)
    epoch,rows,seat_tables,players=batches[0]
    assert epoch==EPOCH and json.loads(seat_tables)==['128016534']
    assert json.loads(rows)==[dict(id='128016534',name='T',game='NLH',type='RING',bb='0.02',min=1,max=4,seats=6,occupied=2,open=True,
                                   observed='2026-10-31T17:00:00',snapshot=None,start=None,state=None)]
    assert json.loads(players)[0]=={'table':'128016534','seat':2,'native':'55','name':'andrew','account':14536,'stack':'2.10','bet':0,
                                        'in_game':True,'away':False,'hand':'99','observed':'2026-10-31T17:00:00'}


def test_publish_without_lobby_data_skips_the_lobby_transaction():
    db=Recorder()
    repository(db).publish([],{},[],[],[])
    assert db.transactions==1 and all(sql!=LOBBY_SQL for sql,_ in db.calls)


def test_lobby_write_is_fenced_by_the_worker_epoch():
    db=Recorder(epoch='11111111-1111-1111-1111-111111111111');tables,seats=lobby()
    with pytest.raises(RuntimeError,match='epoch'):repository(db).publish_lobby(tables,seats)
    assert all(sql!=LOBBY_SQL for sql,_ in db.calls)
