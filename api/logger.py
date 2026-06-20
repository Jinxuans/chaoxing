import os
import sys
import threading

from loguru import logger
from tqdm import tqdm

tqdm_stream = sys.stderr
_context = threading.local()
_worker_id = "-"

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level:<8}</level> | "
    "worker={extra[worker_id]} account={extra[account]} | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

os.makedirs("logs", exist_ok=True)


def set_log_account(username: str | None) -> None:
    _context.account = str(username or "").strip() or "-"


def get_log_account() -> str:
    return getattr(_context, "account", "-")


def set_log_worker_id(worker_id: str | None) -> None:
    global _worker_id
    _worker_id = str(worker_id or "").strip() or "-"


def get_log_worker_id() -> str:
    return _worker_id


def inject_log_context(record):
    record["extra"]["account"] = get_log_account()
    record["extra"]["worker_id"] = get_log_worker_id()


def tqdm_sink(msg):
    tqdm.write(msg.rstrip(), file=tqdm_stream)
    tqdm_stream.flush()


logger.configure(patcher=inject_log_context)
logger.remove()
logger.add(tqdm_sink, colorize=True, enqueue=True, format=LOG_FORMAT)
logger.add(
    f"logs/chaoxing-{os.getpid()}.log",
    rotation="10 MB",
    retention="7 days",
    level="TRACE",
    enqueue=True,
    format=LOG_FORMAT,
)
