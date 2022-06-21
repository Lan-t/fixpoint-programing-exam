from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
import ipaddress
import itertools
import sys
from tarfile import RECORDSIZE
from tracemalloc import start
from typing import DefaultDict, Dict, Iterator, List, NewType, Optional, Set, TextIO, Tuple, overload
from wsgiref.simple_server import server_version


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


def interface_to_network(ip_str: str):
    interface = ipaddress.ip_interface(ip_str)
    network = interface.network
    return str(network)


def in_range(n, r: Tuple):
    if n is None:
        return r[1] is None
    elif r[1] is None:
        return r[0] <= n
    else:
        return r[0] <= n <= r[1]


def ranges_mask(a: List[Tuple], b: List[Tuple]):
    result = []
    for r in a:
        for r2 in b:
            if in_range(r[0], r2):
                if in_range(r[1], r2):
                    result.append(r)
                else:
                    result.append((r[0], r2[1]))
                break
            elif in_range(r2[0], r):
                if in_range(r2[1], r):
                    result.append(r2)
                else:
                    result.append((r2[0], r[1]))
                break

    return result


def ranges_mask_all(ranges: List[List[Tuple]]):
    if len(ranges) < 2:
        return ranges

    return [*reduce(ranges_mask, ranges[2:], ranges_mask(ranges[0], ranges[1]))]


def failer_events_to_network_event(event_dict: Dict[str, List[ServerEventLog]], ip_addrs: List[str]) -> DefaultDict[str, List[ServerEventLog]]:
    ip_dict: DefaultDict[str, List[str]] = defaultdict(list)
    for ip in ip_addrs:
        ip_dict[interface_to_network(ip)].append(ip)

    result: DefaultDict[str, List[ServerEventLog]] = defaultdict(list)

    for network_ip, servers in ip_dict.items():
        event_range = []
        for server_ip in servers:
            server_events = event_dict[server_ip]
            event_range.append([(e.start_time, e.end_time)
                               for e in server_events])

        masked = ranges_mask_all(event_range)

        for e in masked:
            result[network_ip].append(ServerEventLog(e[0], e[1], network_ip))

    return result


def check_server_event(log: Iterator[LogRecord], to_tol: int, ol_tol: int, ol_std: int):
    failings: Dict[str, ServerEventLog] = {}
    failer_events: List[ServerEventLog] = []
    overloadings: Dict[str, ServerEventLog] = {}
    overload_events: List[ServerEventLog] = []

    ip_addrs: Set[str] = set()

    for current in log:
        server_key = current.ip_addr
        ip_addrs.add(current.ip_addr)
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

    failer_event_dict: DefaultDict[str,
                                   List[ServerEventLog]] = defaultdict(list)
    for ev in [*failer_events, *failings.values()]:
        failer_event_dict[ev.ip_addr].append(ev)

    overload_event_dict: DefaultDict[str,
                                     List[ServerEventLog]] = defaultdict(list)
    for ev in [*overload_events, *overloadings.values()]:
        overload_event_dict[ev.ip_addr].append(ev)

    network_event_dict = failer_events_to_network_event(
        failer_event_dict, ip_addrs)

    # 出力

    print('# サーバ障害')
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

    print()

    network_ip_addrs = set([interface_to_network(ip) for ip in ip_addrs])
    print('# ネットワーク障害')
    for ip_addr in network_ip_addrs:
        print(f'=== {ip_addr} ===')
        for ev in network_event_dict[ip_addr]:
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
