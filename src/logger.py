import logging.config
from functools import wraps
from typing import Union


# LOG_FILE = 'logs.log'
LOG_FILE = None

FILE_CONFIG = {
    'file': {  
        'class': 'logging.handlers.RotatingFileHandler',
        'formatter': 'standard',
        'level': 'INFO',
        'filename': LOG_FILE,
        'mode': 'a',
        'encoding': 'utf-8',
        'maxBytes': 500000,
        'backupCount': 4
    } 
} if LOG_FILE else None

FILE_HANDLER = ['file'] if FILE_CONFIG else []

LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'standard': {
            'class': 'logging.Formatter',
            'format': '[%(levelname)s] %(asctime)s - %(message)s',
            # 'format': '%(asctime)s\t%(levelname)s\t%(filename)s\t%(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers' : {
        '': { 
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False
        },
        '__main__': {
            'handlers': ['console'] + FILE_HANDLER,
            'level': 'DEBUG',
            'propagate': False
        }
    },
    'root' : {
        'level': 'DEBUG',
        'handlers': FILE_HANDLER
    }    
}

if FILE_CONFIG:
    LOGGING_CONFIG['handlers'].update(FILE_CONFIG)
    
logging.config.dictConfig(LOGGING_CONFIG)

# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__')
# logger.debug("Logging is configured.")


class Logger:
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)

    def get_logger(self, name=None):
        return logging.getLogger(name)
    

def log(func=None, *, logger: Union[Logger, logging.Logger] = None):
    def decorator_log(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = Logger().get_logger()
            try:
                if logger is None:
                    first_args = next(iter(args), None)
                    logger_params = [x for x in kwargs.values() if isinstance(x, logging.Logger) or isinstance(x, Logger) ] + [x for x in args if isinstance(x, logging.Logger) or isinstance(x, Logger)]
                    if hasattr(first_args, "__dict__"):
                        logger_params = logger_params + [x for x in first_args.__dict__.values() if isinstance(x, logging.Logger) or isinstance(x, Logger)]
                    h_logger = next(iter(logger_params), Logger())
                else:
                    h_logger = logger
                if isinstance(h_logger, Logger):
                    _logger = h_logger.get_logger(func.__name__)
                else:
                    _logger = h_logger

                args_repr = [repr(a) for a in args]
                kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
                signature = ", ".join(args_repr + kwargs_repr)
                _logger.debug(f"function {func.__name__} called with args {signature}")
            except Exception:
                pass
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                _logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
                raise e
        return wrapper

    if func is None:
        return decorator_log
    else:
        return decorator_log(func)