from functools import wraps
from lib.mylog import log
import time


class CatchError:
    '''
    专门用来捕捉异常的装饰器，同时也是为了能够安全执行，不会中断
    '''

    def __init__(self, level='ae'):
        self.level = str(level)
        if not isinstance(level, str):
            log.info('catcherror has wrong level')

    def __call__(self, func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            try:
                begin = time.time()
                result = func(*args, **kwargs)
                dur = time.time() - begin
                if 'a' in self.level:
                    log.info(f'{func.__name__} job is done,take {dur:.2f}s')
                return result
            except Exception as e:
                if 'e' in self.level:
                    log.error(f'FUNC:{func.__name__}; ARGS: {str(args)}, {str(kwargs)}; EXCEP: {e}')

        return wrapped_function


class CostTime:
    '''
    专门用来计时的装饰器
    '''

    def __call__(self, func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            time_cost = time.time() - start_time
            log.info(f'{func.__name__} cost {time_cost:.2f} second')
            return result

        return wrapped_function
