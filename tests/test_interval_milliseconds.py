from datetime import datetime,timezone
from unittest.mock import patch
from schedule_controller.pilot import Pilot


def test_accruals_within_one_millisecond_make_no_empty_interval():
    p=Pilot.__new__(Pilot);p.sequence=0;p.intervals=[]
    p.entries={'e':{'account':1,'status':'seated'}}
    p.accounting={1:(datetime(2026,11,7,20,58,39,95000,tzinfo=timezone.utc),1)}
    stamps=iter([datetime(2026,11,7,20,58,39,95400,tzinfo=timezone.utc),datetime(2026,11,7,20,58,39,95900,tzinfo=timezone.utc),
                 datetime(2026,11,7,20,58,40,0,tzinfo=timezone.utc)])
    with patch('schedule_controller.pilot.now',lambda:next(stamps)):
        p.accrue();p.accrue();p.accrue()
    assert len(p.intervals)==1
    span=p.intervals[0]
    assert span['start'].microsecond%1000==0 and span['end'].microsecond%1000==0 and span['end']>span['start']
