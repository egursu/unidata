from datetime import datetime
from time import time, gmtime, strftime


def duration_time(func):
    def wrapper():
        start_time = time()
        log = f'Function: {func.__name__}\nRun on: {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}'
        func()
        print(f"{log}\nDuration: {strftime("%H:%M:%S", gmtime(time() - start_time))}.")
        print(f'{"-"*27}')
    return wrapper