import logging
import time

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

class timeblock:
    def __init__(self, logger: logging.Logger, msg: str):
        self.logger = logger
        self.msg = msg
    def __enter__(self):
        self.start = time.perf_counter()
    def __exit__(self, exc_type, exc, tb):
        dt = (time.perf_counter() - self.start)*1000
        self.logger.info(f"{self.msg} took {dt:.1f} ms")