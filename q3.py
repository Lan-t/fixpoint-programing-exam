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
    count: int = 1


def log_reader(file: TextIO) -> Iterator[LogRecord]:
    while (line := file.readline().strip()) != '':
        log_datetime_str, ip_addr, ping_str = line.split(',')
        log_datetime = datetime.strptime(log_datetime_str, LOG_DATETIME_FORMAT)
        if ping_str == '-':
            ping = None
        else:
            ping = int(ping_str)

        yield LogRecord(log_datetime, ip_addr, ping)


def check_server_event(log: Iterator[LogRecord], to_tol: int, ol_tol: int, ol_std: int):
    failings: Dict[str, ServerEventLog] = {}
    failer_events: List[ServerEventLog] = []
    overloadings: Dict[str, ServerEventLog] = {}
    overload_events: List[ServerEventLog] = []

    for current in log:
        server_key = current.ip_addr
        if current.ping is None:  # タイムアウト中
            if (failing := failings.get(server_key)) is not None:
                failing.count += 1
            else:
                failings[server_key] = ServerEventLog(
                    current.logtime, None, current.ip_addr)

        else:  # ping が返って来ている
            if (failing := failings.get(server_key)) is not None:
                # 前にタイムアウトしていた時
                if failing.count >= to_tol:
                    failing.end_time = current.logtime
                    failer_events.append(failing)
                del failings[server_key]

            if current.ping > ol_std:  # 過不可状態
                if (overloading := overloadings.get(server_key)) is not None:
                    # すでに過不可状態
                    overloading.count += 1
                else:  # current から過不可状態に入る
                    overloadings[server_key] = ServerEventLog(
                        current.logtime, None, current.ip_addr)
            else:  # not 過不可状態
                if (overloading := overloadings.get(server_key)) is not None:
                    # 前にタイムアウトしていた時
                    if overloading.count >= ol_tol:
                        overloading.end_time = current.logtime
                        overload_events.append(overloading)
                    del overloadings[server_key]

    # 出力

    failer_event_dict: DefaultDict[str,
                                   List[ServerEventLog]] = defaultdict(list)
    for ev in [*failer_events, *failings.values()]:
        failer_event_dict[ev.ip_addr].append(ev)

    overload_event_dict: DefaultDict[str,
                                     List[ServerEventLog]] = defaultdict(list)
    for ev in [*overload_events, *overloadings.values()]:
        overload_event_dict[ev.ip_addr].append(ev)

    ip_addrs = set([*failer_event_dict.keys(), *overload_event_dict.keys()])

    for ip_addr in ip_addrs:
        print(f'=== {ip_addr} ===')
        print('  --- 故障期間 ---')
        for ev in failer_event_dict[ip_addr]:
            start_time_str = str(ev.start_time)
            if ev.end_time is None:
                end_time_str = '-'
                delta = '-'
            else:
                end_time_str = str(ev.end_time)
                delta = ev.end_time - ev.start_time
            print(f'  {start_time_str} - {end_time_str} ({delta})')
        print('  --- 過不可状態期間 ---')
        for ev in overload_event_dict[ip_addr]:
            start_time_str = str(ev.start_time)
            if ev.end_time is None:
                end_time_str = '-'
                delta = '-'
            else:
                end_time_str = str(ev.end_time)
                delta = ev.end_time - ev.start_time
            print(f'  {start_time_str} - {end_time_str} ({delta})')


if __name__ == '__main__':
    try:
        logfile = open('log.txt')
    except FileNotFoundError:
        sys.stderr.write('ログファイルがありません ( ./log.txt )\n')
        sys.exit(1)

    reader = log_reader(logfile)

    try:
        n = int(sys.argv[1])
        m = int(sys.argv[2])
        t = int(sys.argv[3])
    except IndexError:
        sys.stderr.write(
            f'usage: {sys.argv[0]} {{timeout_limit}} {{overload_limit}} {{overload_std}}\n')
        sys.exit(1)
    except ValueError:
        sys.stderr.write(f'パラメータは整数で指定してください\n')
        sys.exit(1)

    check_server_event(reader, n, m, t)
