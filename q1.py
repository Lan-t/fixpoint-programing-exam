from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
import sys
from typing import DefaultDict, Dict, Iterator, List, Optional, TextIO


LOG_DATETIME_FORMAT = '%Y%m%d%H%M%S'


@dataclass
class LogRecord:
    logtime: datetime
    ip_addr: str
    ping: Optional[int]


@dataclass
class ServerEventLog:
    start_time: datetime
    end_time: Optional[datetime]
    ip_addr: str


def log_reader(file: TextIO) -> Iterator[LogRecord]:
    while (line := file.readline().strip()) != '':
        log_datetime_str, ip_addr, ping_str = line.split(',')
        log_datetime = datetime.strptime(log_datetime_str, LOG_DATETIME_FORMAT)
        if ping_str == '-':
            ping = None
        else:
            ping = int(ping_str)

        yield LogRecord(log_datetime, ip_addr, ping)


def check_server_event(log: Iterator[LogRecord]):
    failings: Dict[str, ServerEventLog] = {}
    failer_events: List[ServerEventLog] = []

    for current in log:
        server_key = current.ip_addr
        if current.ping is None:
            if (failing := failings.get(server_key)) is None:
                failings[server_key] = ServerEventLog(
                    current.logtime, None, current.ip_addr)

        elif (failing := failings.get(server_key)) is not None:
            # タイムアウト解消
            failing.end_time = current.logtime
            failer_events.append(failing)
            del failings[server_key]

    # 出力

    events_dict: DefaultDict[str, List[ServerEventLog]] = defaultdict(list)
    for ev in [*failer_events, *failings.values()]:
        events_dict[ev.ip_addr].append(ev)

    for ip_addr, events in events_dict.items():
        print(f'--- {ip_addr} ---')
        for ev in events:
            start_time_str = str(ev.start_time)
            if ev.end_time is None:
                end_time_str = '-'
                delta = '-'
            else:
                end_time_str = str(ev.end_time)
                delta = ev.end_time - ev.start_time
            print(f'{start_time_str} - {end_time_str} ({delta})')


if __name__ == '__main__':
    try:
        logfile = open('log.txt')
    except FileNotFoundError:
        sys.stderr.write('ログファイルがありません ( ./log.txt )\n')
        sys.exit(1)

    reader = log_reader(logfile)
    check_server_event(reader)
