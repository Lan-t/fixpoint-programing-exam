from dataclasses import dataclass
from datetime import datetime, timedelta
from time import time
from typing import List

from numpy import true_divide

LOG_DATETIME_FORMAT = '%Y%m%d%H%M%S'


@dataclass
class Device:
    sub: int
    dev: int
    overload: bool
    failer: bool


failing_sub: List[int] = []
sub1dev1 = Device(1, 1, False, False)
sub1dev2 = Device(1, 2, False, False)
sub1dev3 = Device(1, 3, False, False)
sub2dev1 = Device(2, 1, False, False)
sub2dev2 = Device(2, 2, False, False)
sub2dev3 = Device(2, 3, False, False)
sub3dev1 = Device(3, 1, False, False)
sub3dev2 = Device(3, 2, False, False)
sub3dev3 = Device(3, 3, False, False)


step = timedelta(seconds=10)

current_time = datetime.strptime('20220101000000', LOG_DATETIME_FORMAT)


def ping(sd: Device):
    def adr(sub, dev):
        if sub == 1:
            return f'10.10.10.{dev}/24'
        if sub == 2:
            return f'20.20.20.{dev}/24'
        if sub == 3:
            return f'30.30.0.{dev}/16'
    if sd.sub in failing_sub:
        p = '-'
    elif sd.failer:
        p = '-'
    elif sd.overload:
        p = '100'
    else:
        p = '5'
    print(f'{current_time.strftime(LOG_DATETIME_FORMAT)},{adr(sd.sub, sd.dev)},{p}')


def ping_all():
    ping(sub1dev1)
    ping(sub1dev2)
    ping(sub1dev3)
    ping(sub2dev1)
    ping(sub2dev2)
    ping(sub2dev3)
    ping(sub3dev1)
    ping(sub3dev2)
    ping(sub3dev3)


ping_all()

current_time += step
sub1dev1.failer = True
sub1dev3.failer = True
ping_all()

current_time += step
sub1dev2.failer = True
sub1dev3.failer = False
ping_all()

current_time += step
sub1dev1.failer = False
ping_all()

current_time += step
sub1dev2.failer = False
ping_all()

current_time += step
sub2dev3.failer = True
ping_all()

current_time += step
sub2dev1.failer = True
ping_all()

current_time += step
failing_sub.append(2)
ping_all()

current_time += step
ping_all()
current_time += step
ping_all()
current_time += step
ping_all()

current_time += step
sub1dev1.overload = True
sub1dev2.overload = True
sub1dev3.failer = True
sub2dev1.failer = False
sub2dev3.failer = False
sub2dev2.overload = True
ping_all()

current_time += step
ping_all()
current_time += step
sub1dev2.failer = True
ping_all()
current_time += step
failing_sub.remove(2)
ping_all()

current_time += step
sub1dev2.failer = False
ping_all()

current_time += step
sub2dev2.overload = False
sub2dev3.failer = False
ping_all()

current_time += step
sub1dev1.overload = False
sub1dev2.overload = False
sub1dev3.failer = False
failing_sub.append(3)
ping_all()

current_time += step
sub1dev1.failer = True
sub1dev2.overload = True
ping_all()

current_time += step
ping_all()

current_time += step
ping_all()
