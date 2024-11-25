import os
import hashlib
from collections.abc import Sequence
from uuid import getnode
from urllib.parse import urlparse, ParseResult
from datetime import date, datetime, timedelta
from time import time, gmtime, strftime


DAYS = {
    0: ('Sunday', 'Sun', 'U'),
    1: ('Monday', 'Mon', 'M'),
    2: ('Tuesday', 'Tue', 'T', 'Tu', 'Tues'),
    3: ('Wednesday', 'Wed', 'W'),
    4: ('Thursday', 'Thu', 'R', 'Th'),
    5: ('Friday', 'Fri', 'F', 'Fr'),   
    6: ('Saturday', 'Sat', 'S', 'St'), 
}

def iter_lowered(iterable: Sequence):
    return type(iterable)(map(str.lower, iterable))

def to_list(data, str_sep: str=",", slice_stop: int=None):
    if isinstance(data, str):
        return [s.strip() for s in data.split(str_sep)]
    elif isinstance(data, (int, float)):
        return [data]
    elif isinstance(data, (list, tuple, set, dict)):
        return list(data)
    elif isinstance(data, slice):
        # return = list(range(data.start or 0, data.stop or slice_stop, data.step or 1))
        return list(range(*data.indices(slice_stop or data.stop)))
    # elif isinstance(data, list):
    #     return data
    else:
        return []

def iter_in_str(iter: Sequence, text: str) -> bool: 
    return any(item in text for item in iter)

def is_matrix(data: list|tuple) -> bool:
    return (not (not data) and isinstance(data, (list, tuple)) and isinstance(data[0], (list, tuple, dict, set)))

def yield_list(iterable: Sequence, n: int):
    for i in range(0, len(iterable), n):  
        yield iterable[i:i + n]

def file_extension(path: str, lowered: bool=True) -> str:
    ext: str = os.path.splitext(path)[1][1:]
    if lowered:
        return ext.lower()
    return ext

def str_to_date(text: str, format="%Y-%m-%d") -> datetime:
    try:
        return datetime.strptime(text, format).date()
    except ValueError:
        return None

# Date utils  
def date_to_str(date=datetime.now(), format="%Y-%m-%d %H:%M:%S") -> str:
    return date.strftime(format)

def is_date(text: str, format="%Y-%m-%d") -> bool:
    return str_to_date(text, format) is not None

def duration_time(start_time: time) -> str: 
    return strftime("%H:%M:%S", gmtime(time() - start_time))

def prev_date(days: int=1, dt: date=date.today()) -> date:
    return dt - timedelta(days=days)

def end_of_month(dt: date=date.today()) -> date:
    return (dt.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)

def last_day_of_month(day: int|str=-1, dt: date=date.today()) -> date:
    # eom = (dt.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    eom = end_of_month(dt)
    if isinstance(day, str):
        day = [key for key, val in DAYS.items() if any(day.upper() == s.upper() for s in val)]
        day = day[0] if day else -1
    return eom if day == -1 else eom - (timedelta(days=(eom.weekday() - day + 1) % 7))

def prev_month(months_ago: int=1, dt: date=date.today()) -> date:
    return date(int(dt.year + (((dt.month - 1) - months_ago) / 12)), (((dt.month - 1) - months_ago) % 12) + 1, 1)

# def prev_day_of_week(day: int, dt: date=date.today()) -> date:
#     return end_of_month(dt) - timedelta(days=(end_of_month(dt).weekday() - day + 1) % 7)

def date_range(start: datetime, end: datetime) -> list:
    return [start + timedelta(days=delta) for delta in range((end - start).days + 1)]

def month_range(months_ago: int=1, dt: date=date.today(), one_month:bool=False) -> list:
    return date_range(prev_month(months_ago, dt), end_of_month(prev_month(months_ago, dt)) if one_month else dt.replace(day=1) - timedelta(days=1))

def prev_week_range(dt: date=date.today()) -> list:
    return [dt - timedelta(days=dt.weekday(), weeks=1) + timedelta(day) for day in range(7)]
 
def from_monday_range(dt: date=date.today()) -> list: 
    return date_range(dt - timedelta(days=dt.weekday(), weeks=1 if dt.weekday() == 0 else 0), dt - timedelta(days=1))

def boolify(val: str|int|bool) -> bool:
    val = val.lower() if isinstance(val, str) else val
    if val in ("y", "yes", "t", "true", "on", "1", 1, True):
        return True
    elif val in ("n", "no", "f", "false", "off", "0", 0, False):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))
    
def hasher(obj, method="md5") -> str:
    hash_method = getattr(hashlib, method)()
    if isinstance(obj, str):
        hash_method.update(obj.encode())
    elif isinstance(obj, (int, float)):
        hash_method.update(str(obj).encode())
    elif isinstance(obj, (tuple, list)):
        for item in obj:
            hash_method.update(str(item).encode())
    elif isinstance(obj, dict):
        keys = obj.keys()
        for k in sorted(keys):
            hash_method.update(str(k).encode())
            hash_method.update(str(obj[k]).encode())
    return hash_method.hexdigest()

def md5sum(obj) -> str:
    return hasher(obj, "md5")

def encode(text: str, key: str=None) -> str:
    key = key or hex(getnode())
    """use key to encrypt text"""
    array = []
    for idx, c in enumerate(text):
        array.append(chr(ord(c) + ord(key[idx % len(key)])))
    return "".join(array)

def decode(text: str, key: str=None) -> str:
    key = key or hex(getnode())
    """use key to dencrypt text"""
    array = []
    for idx, c in enumerate(text):
        array.append(chr(ord(c) - ord(key[idx % len(key)])))
    return "".join(array)

def params_to_url(params: dict|str) -> ParseResult:
    if isinstance(params, str):
        return urlparse(params)
    else:
        params_keys = ("scheme", "netloc", "path", "params", "query", "fragment")
        result = {param: params.get(param, "") for param in params_keys}
        if not result.get("netloc"):
            username = params.get("username")
            password = params.get("password")
            hostname = params.get("hostname")
            port = f':{params["port"]}' if params.get("port") else ""
            result["netloc"] = f"{username}:{password}@{hostname}{port}"
        return ParseResult(**result)
    
def progress_bar(
    iteration,
    total,
    prefix="Progress:",
    suffix="Complete",
    decimals=2,
    length=60,
    fill="■",
    printEnd="\r",
):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    # print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    print(f"\r{prefix} |{bar}| {iteration}/{total} ({percent}%) {suffix}", end=printEnd)
    if iteration == total:
        print()

def dict_lowered(data, all_keys: bool = False):
    if isinstance(data, dict):
        return {k.lower() if not isinstance(v, dict) or all_keys else k: dict_lowered(v, all_keys) for k, v in data.items()}
    return data

    
class CaseInsensitiveDict(dict):
    """Basic case insensitive dict with strings only keys."""
    proxy: dict = {}

    def __init__(self, data: dict):
        self.proxy = dict((k.lower(), k) for k in data)
        for k in data:
            self[k] = data[k]

    def __contains__(self, k) -> bool:
        return k.lower() in self.proxy

    def __delitem__(self, k):
        key = self.proxy[k.lower()]
        super(CaseInsensitiveDict, self).__delitem__(key)
        del self.proxy[k.lower()]

    def __getitem__(self, k):
        key = self.proxy[k.lower()]
        return super(CaseInsensitiveDict, self).__getitem__(key)

    def get(self, k, default=None):
        return self[k] if k in self else default

    def __setitem__(self, k, v):
        super(CaseInsensitiveDict, self).__setitem__(k, v)
        self.proxy[k.lower()] = k