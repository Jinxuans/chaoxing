import hashlib
import re
import time
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Iterator, TypeVar

import pymysql

from api.logger import logger


TRANSIENT_OPERATIONAL_ERRORS = {
    1205,  # lock wait timeout
    1213,  # deadlock
    2003,  # can't connect
    2006,  # server has gone away
    2013,  # lost connection
}

T = TypeVar("T")


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_process(value: Any) -> str:
    text = str(value or "").strip()
    ratio_match = re.fullmatch(r"(\d+)\s*/\s*(\d+)", text)
    if ratio_match:
        done = int(ratio_match.group(1))
        total = max(int(ratio_match.group(2)), 1)
        return f"{min(done, total)}/{total}"

    percent_match = re.fullmatch(r"(\d+(?:\.\d+)?)%", text)
    if percent_match:
        percent = min(float(percent_match.group(1)), 100.0)
        if percent.is_integer():
            return f"{int(percent)}%"
        return f"{percent:.2f}%"

    try:
        number = min(float(text), 100.0)
    except ValueError:
        return text
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}"


def compact_text(value: Any, limit: int = 80) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 1)] + "…"


class Heartbeat:
    def __init__(self, queue: "CourseXOrderQueue", oid: int, token: str, interval: float):
        self.queue = queue
        self.oid = oid
        self.token = token
        self.interval = interval
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stopped.set()
        self.thread.join(timeout=2)

    def run(self) -> None:
        while not self.stopped.wait(self.interval):
            try:
                self.queue.heartbeat(self.oid, self.token)
            except Exception as exc:
                logger.warning("订单心跳写回失败 oid={} error={}", self.oid, exc)


class OrderProgressReporter:
    def __init__(self, queue: "CourseXOrderQueue", oid: int, token: str, min_interval: float):
        self.queue = queue
        self.oid = oid
        self.token = token
        self.min_interval = max(float(min_interval), 1.0)
        self.last_update_at = 0.0
        self.lock = threading.Lock()

    def __call__(self, process: str, message: str) -> None:
        now = time.time()
        force = process == "100%"
        with self.lock:
            if not force and now - self.last_update_at < self.min_interval:
                return
            self.last_update_at = now

        try:
            self.queue.update_order(
                self.oid,
                self.token,
                status="进行中",
                process=normalize_process(process),
                remarks=message[:255],
                runner_heartbeat_at=now_text(),
            )
        except Exception as exc:
            logger.warning("订单摘要进度写回失败 oid={} error={}", self.oid, exc)


class RunnerDatabase:
    """Per-process MySQL adapter for runner scheduling writes."""

    def __init__(self, config: dict[str, Any], worker_id: str):
        self.config = config
        self.worker_id = worker_id
        self._conn = None
        self._lock = threading.RLock()

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        finally:
            self._conn = None

    def _new_connection(self):
        return pymysql.connect(
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            password=self.config["password"],
            database=self.config["database"],
            charset=self.config["charset"],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=self.config["db_connect_timeout"],
            read_timeout=self.config["db_read_timeout"],
            write_timeout=self.config["db_write_timeout"],
        )

    def _ensure_connection(self):
        if self._conn is None:
            self._conn = self._new_connection()
            return self._conn
        try:
            self._conn.ping(reconnect=True)
        except pymysql.err.OperationalError:
            self.close()
            self._conn = self._new_connection()
        return self._conn

    @contextmanager
    def connect(self) -> Iterator[Any]:
        with self._lock:
            conn = self._ensure_connection()
            try:
                yield conn
            except pymysql.err.OperationalError as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if self.is_transient_error(exc):
                    self.close()
                raise
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

    @staticmethod
    def is_transient_error(exc: BaseException) -> bool:
        if not isinstance(exc, pymysql.err.OperationalError):
            return False
        code = exc.args[0] if exc.args else None
        return code in TRANSIENT_OPERATIONAL_ERRORS

    def execute_with_retry(
        self,
        operation: Callable[[], T],
        attempts: int = 3,
        base_sleep_seconds: float = 0.5,
    ) -> T:
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            try:
                return operation()
            except pymysql.err.OperationalError as exc:
                last_error = exc
                if not self.is_transient_error(exc) or attempt >= attempts:
                    raise
                self.close()
                sleep_seconds = base_sleep_seconds * attempt
                logger.warning(
                    "Runner数据库操作失败，{:.1f}s后重试 worker_id={} attempt={}/{} error={}: {}",
                    sleep_seconds,
                    self.worker_id,
                    attempt,
                    attempts,
                    type(exc).__name__,
                    exc,
                )
                time.sleep(sleep_seconds)

        raise last_error or RuntimeError("Runner数据库操作失败")


class CourseXOrderQueue:
    def __init__(self, config: dict[str, Any], platform: str, worker_id: str):
        self.config = config
        self.platform = platform
        self.worker_id = worker_id
        self.db = RunnerDatabase(config, worker_id)

    def route_clause(self) -> tuple[str, list[Any]]:
        clauses = ["runner_platform=%s"]
        params: list[Any] = [self.platform]
        cids = self.config["cids"]
        if cids:
            placeholders = ", ".join(["%s"] * len(cids))
            clauses.append(f"cid IN ({placeholders})")
            params.extend(cids)
        return "(" + " OR ".join(clauses) + ")", params

    def connect(self):
        return self.db.connect()

    def recover_stale_orders(self) -> int:
        route_clause, route_params = self.route_clause()
        stale_at_sql = """
            COALESCE(
                NULLIF(runner_heartbeat_at, ''),
                STR_TO_DATE(SUBSTRING_INDEX(NULLIF(uptime, ''), '--', 1), '%%Y-%%m-%%d %%H:%%i:%%s'),
                STR_TO_DATE(SUBSTRING_INDEX(NULLIF(addtime, ''), '--', 1), '%%Y-%%m-%%d %%H:%%i:%%s')
            )
        """
        stale_conditions = [
            f"""
            (runner_status IN ('claimed', 'running')
             AND ({stale_at_sql} IS NULL OR TIMESTAMPDIFF(SECOND, {stale_at_sql}, NOW()) > %s))
            """,
            f"""
            ((runner_status='' OR runner_status IS NULL)
             AND status IN ('进行中', '排队中')
             AND ({stale_at_sql} IS NULL OR TIMESTAMPDIFF(SECOND, {stale_at_sql}, NOW()) > %s))
            """,
        ]
        stale_params: list[Any] = [self.config["claim_timeout"], self.config["claim_timeout"]]
        stale_where = " OR ".join(stale_conditions)

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE qingka_wangke_order
                    SET runner_status='retrying', runner_worker_id='', runner_claim_token='',
                        runner_error=CONCAT(IFNULL(runner_error, ''), '\nstale claim recovered at ', %s),
                        status='待处理', remarks='Runner心跳超时，等待重新领取'
                    WHERE {route_clause}
                      AND dockstatus IN ('98', '99')
                      AND runner_attempts < %s
                      AND ({stale_where})
                    """,
                    [now_text()] + route_params + [self.config["max_attempts"]] + stale_params,
                )
                recovered = cursor.rowcount
                cursor.execute(
                    f"""
                    UPDATE qingka_wangke_order
                    SET runner_status='failed', runner_worker_id='', runner_claim_token='',
                        runner_error=CONCAT(IFNULL(runner_error, ''), '\nstale claim failed at ', %s),
                        status='异常', dockstatus='2', remarks='Runner心跳超时，已达最大重试次数',
                        runner_finished_at=%s, uptime=%s
                    WHERE {route_clause}
                      AND dockstatus IN ('98', '99')
                      AND runner_attempts >= %s
                      AND ({stale_where})
                    """,
                    [now_text(), now_text(), now_text()] + route_params + [self.config["max_attempts"]] + stale_params,
                )
                recovered += cursor.rowcount
            conn.commit()
            return recovered

    def account_lock_name(self, account: str) -> str:
        account_hash = hashlib.sha1(account.encode("utf-8")).hexdigest()[:24]
        return f"coursex:{self.platform}:{account_hash}"

    def maintenance_lock_name(self) -> str:
        return f"coursex:{self.platform}:maintenance"

    def release_lock(self, cursor, lock_name: str) -> None:
        try:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        except Exception as exc:
            logger.warning("释放Runner锁失败 lock={} worker_id={} error={}", lock_name, self.worker_id, exc)

    def try_run_maintenance(self) -> tuple[int, int, bool]:
        lock_name = self.maintenance_lock_name()
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT GET_LOCK(%s, 0) AS locked", (lock_name,))
                lock_row = cursor.fetchone() or {}
                if int(lock_row.get("locked") or 0) != 1:
                    conn.commit()
                    return 0, 0, False

                try:
                    recovered = self.recover_stale_orders()
                    queued = self.mark_active_accounts_queued()
                    return recovered, queued, True
                finally:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                    conn.commit()

    def mark_account_queued(self, cursor, account: str, exclude_oid: int | None = None) -> None:
        route_clause, route_params = self.route_clause()
        cursor.execute(
            f"""
            SELECT oid, kcname, process, remarks
            FROM qingka_wangke_order
            WHERE {route_clause}
              AND user=%s
              AND runner_status IN ('claimed', 'running')
              AND dockstatus IN ('98', '99')
              AND status!='已取消'
            ORDER BY runner_claimed_at DESC, oid DESC
            LIMIT 1
            """,
            route_params + [account],
        )
        active_order = cursor.fetchone() or {}
        if active_order:
            active_course = compact_text(active_order.get("kcname"), 34)
            active_process = normalize_process(active_order.get("process") or "0%")
            active_remark = compact_text(active_order.get("remarks"), 82)
            queued_remark = (
                f"排队中，当前进行课程:[{active_order.get('oid')}] {active_course}，"
                f"当前进度:{active_process}，请勿同时登陆。实时进度:{now_text()} {active_remark}"
            )
        else:
            queued_remark = f"排队中，同账号已有课程运行，请勿同时登陆。更新时间:{now_text()}"

        params: list[Any] = [queued_remark[:255], now_text()] + route_params + [account]
        exclude_clause = ""
        if exclude_oid is not None:
            exclude_clause = "AND oid<>%s"
            params.append(exclude_oid)

        cursor.execute(
            f"""
            UPDATE qingka_wangke_order
            SET runner_status='queued', status='排队中', remarks=%s, uptime=%s
            WHERE {route_clause}
              AND user=%s
              {exclude_clause}
              AND (runner_status='' OR runner_status='retrying')
              AND dockstatus IN ('98', '99')
              AND status!='已取消'
            """,
            params,
        )

    def mark_active_accounts_queued(self) -> int:
        route_clause, route_params = self.route_clause()
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT DISTINCT user FROM qingka_wangke_order
                    WHERE {route_clause}
                      AND runner_status IN ('claimed', 'running')
                      AND dockstatus IN ('98', '99')
                      AND status!='已取消'
                      AND user!=''
                    """,
                    route_params,
                )
                active_accounts = [str(row.get("user") or "") for row in cursor.fetchall()]
                queued = 0
                for account in active_accounts:
                    if not account:
                        continue
                    self.mark_account_queued(cursor, account)
                    if cursor.rowcount >= 0:
                        queued += cursor.rowcount
            conn.commit()
            return queued

    def claim_next(self) -> dict[str, Any] | None:
        token = uuid.uuid4().hex
        route_clause, route_params = self.route_clause()
        claimed_lock = None
        with self.connect() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        f"""
                        SELECT oid, user FROM qingka_wangke_order
                        WHERE {route_clause}
                          AND (runner_status='' OR runner_status='queued' OR runner_status='retrying')
                          AND dockstatus IN ('98', '99')
                          AND status!='已取消'
                          AND runner_attempts < %s
                          AND (runner_next_run_at='' OR runner_next_run_at IS NULL OR runner_next_run_at <= NOW())
                          AND NOT EXISTS (
                              SELECT 1 FROM qingka_wangke_order active_order
                              WHERE active_order.user=qingka_wangke_order.user
                                AND active_order.runner_status IN ('claimed', 'running')
                                AND active_order.dockstatus IN ('98', '99')
                                AND active_order.status!='已取消'
                              LIMIT 1
                          )
                        ORDER BY oid ASC
                        LIMIT 20
                        FOR UPDATE
                        """,
                        route_params + [self.config["max_attempts"]],
                    )
                    rows = cursor.fetchall()
                    if not rows:
                        conn.commit()
                        return None

                    claimed_order = None
                    for row in rows:
                        account = str(row.get("user") or "")
                        lock_name = self.account_lock_name(account)
                        cursor.execute("SELECT GET_LOCK(%s, 0) AS locked", (lock_name,))
                        lock_row = cursor.fetchone() or {}
                        if int(lock_row.get("locked") or 0) != 1:
                            self.mark_account_queued(cursor, account)
                            continue

                        cursor.execute(
                            f"""
                            SELECT oid FROM qingka_wangke_order
                            WHERE {route_clause}
                              AND user=%s
                              AND runner_status IN ('claimed', 'running')
                              AND dockstatus IN ('98', '99')
                              AND status!='已取消'
                            LIMIT 1
                            """,
                            route_params + [account],
                        )
                        active_order = cursor.fetchone()
                        if active_order:
                            self.mark_account_queued(cursor, account)
                            self.release_lock(cursor, lock_name)
                            continue

                        claimed_order = row
                        claimed_lock = lock_name
                        break

                    if not claimed_order:
                        conn.commit()
                        return None

                    cursor.execute(
                        """
                        UPDATE qingka_wangke_order
                        SET runner_platform=IF(runner_platform='', %s, runner_platform),
                            runner_status='claimed', runner_claimed_at=%s, runner_heartbeat_at=%s,
                            runner_worker_id=%s, runner_claim_token=%s, runner_attempts=runner_attempts+1,
                            status='排队中', remarks='Runner已领取'
                        WHERE oid=%s
                          AND (runner_status='' OR runner_status='queued' OR runner_status='retrying')
                        """,
                        (self.platform, now_text(), now_text(), self.worker_id, token, claimed_order["oid"]),
                    )
                    if cursor.rowcount != 1:
                        conn.rollback()
                        if claimed_lock:
                            self.release_lock(cursor, claimed_lock)
                            claimed_lock = None
                        return None

                    self.mark_account_queued(cursor, str(claimed_order.get("user") or ""), int(claimed_order["oid"]))
                    cursor.execute("SELECT * FROM qingka_wangke_order WHERE oid=%s", (claimed_order["oid"],))
                    order = cursor.fetchone()
                    conn.commit()
                    if claimed_lock:
                        self.release_lock(cursor, claimed_lock)
                        claimed_lock = None
                    order["runner_claim_token"] = token
                    return order
                finally:
                    if claimed_lock:
                        self.release_lock(cursor, claimed_lock)

    def heartbeat(self, oid: int, token: str) -> None:
        self.update_order(oid, token, runner_heartbeat_at=now_text())

    def execute_with_retry(self, operation, attempts: int = 3) -> None:
        self.db.execute_with_retry(operation, attempts=attempts)

    def mark_running(self, oid: int, token: str) -> None:
        self.update_order(
            oid,
            token,
            runner_status="running",
            runner_heartbeat_at=now_text(),
            status="进行中",
            remarks=f"Runner运行中: {self.worker_id}",
            process="0%",
        )

    def mark_done(self, oid: int, token: str) -> None:
        self.update_order(
            oid,
            token,
            runner_status="done",
            status="已完成",
            process="100%",
            remarks=f"Runner已完成: {self.worker_id}",
            dockstatus="1",
            runner_finished_at=now_text(),
            runner_heartbeat_at=now_text(),
        )

    def mark_failed(self, oid: int, token: str, error: str) -> None:
        def operation() -> None:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE qingka_wangke_order
                        SET runner_status=IF(runner_attempts < %s, 'retrying', 'failed'),
                            status=IF(runner_attempts < %s, '待处理', '异常'),
                            dockstatus=IF(runner_attempts < %s, '98', '2'),
                            remarks=IF(runner_attempts < %s, 'Runner执行失败，等待重试', %s),
                            runner_finished_at=IF(runner_attempts < %s, '', %s),
                            runner_heartbeat_at=%s,
                            runner_error=%s,
                            runner_worker_id=IF(runner_attempts < %s, '', runner_worker_id),
                            runner_claim_token=IF(runner_attempts < %s, '', runner_claim_token),
                            runner_next_run_at=IF(runner_attempts < %s, DATE_ADD(NOW(), INTERVAL 2 MINUTE), ''),
                            uptime=%s
                        WHERE oid=%s AND runner_claim_token=%s AND runner_worker_id=%s
                        """,
                        (
                            self.config["max_attempts"],
                            self.config["max_attempts"],
                            self.config["max_attempts"],
                            self.config["max_attempts"],
                            f"Runner执行失败: {self.worker_id}",
                            self.config["max_attempts"],
                            now_text(),
                            now_text(),
                            error[:4000],
                            self.config["max_attempts"],
                            self.config["max_attempts"],
                            self.config["max_attempts"],
                            now_text(),
                            oid,
                            token,
                            self.worker_id,
                        ),
                    )
                conn.commit()

        self.execute_with_retry(operation)

    def mark_manual_required(self, oid: int, token: str, message: str, error: str = "") -> None:
        def operation() -> None:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE qingka_wangke_order
                        SET runner_status='manual_required',
                            status='异常',
                            dockstatus='2',
                            remarks=%s,
                            runner_finished_at=%s,
                            runner_heartbeat_at=%s,
                            runner_error=%s,
                            runner_next_run_at='',
                            uptime=%s
                        WHERE oid=%s AND runner_claim_token=%s AND runner_worker_id=%s
                        """,
                        (
                            message[:255],
                            now_text(),
                            now_text(),
                            (error or message)[:4000],
                            now_text(),
                            oid,
                            token,
                            self.worker_id,
                        ),
                    )
                conn.commit()

        self.execute_with_retry(operation)

    def mark_interrupted(self, oid: int, token: str) -> None:
        def operation() -> None:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE qingka_wangke_order
                        SET runner_status='retrying',
                            status='待处理',
                            dockstatus='98',
                            remarks=%s,
                            runner_worker_id='',
                            runner_claim_token='',
                            runner_attempts=IF(runner_attempts > 0, runner_attempts - 1, 0),
                            runner_heartbeat_at=%s,
                            runner_next_run_at=DATE_ADD(NOW(), INTERVAL 1 MINUTE),
                            uptime=%s
                        WHERE oid=%s AND runner_claim_token=%s AND runner_worker_id=%s
                        """,
                        (
                            f"Runner手动中断，等待重试: {self.worker_id}",
                            now_text(),
                            now_text(),
                            oid,
                            token,
                            self.worker_id,
                        ),
                    )
                conn.commit()

        self.execute_with_retry(operation)

    def mark_network_retry(self, oid: int, token: str, error: str, delay_seconds: int) -> None:
        delay_seconds = max(int(delay_seconds), 30)

        def operation() -> None:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        UPDATE qingka_wangke_order
                        SET runner_status='retrying',
                            status='待处理',
                            dockstatus='98',
                            remarks=%s,
                            runner_worker_id='',
                            runner_claim_token='',
                            runner_attempts=IF(runner_attempts > 0, runner_attempts - 1, 0),
                            runner_heartbeat_at=%s,
                            runner_error=%s,
                            runner_next_run_at=DATE_ADD(NOW(), INTERVAL {delay_seconds} SECOND),
                            uptime=%s
                        WHERE oid=%s AND runner_claim_token=%s AND runner_worker_id=%s
                        """,
                        (
                            f"代理/网络异常，等待重试: {self.worker_id}",
                            now_text(),
                            error[:4000],
                            now_text(),
                            oid,
                            token,
                            self.worker_id,
                        ),
                    )
                conn.commit()

        self.execute_with_retry(operation)

    def mark_workers_interrupted(self, worker_ids: list[str]) -> int:
        if not worker_ids:
            return 0
        placeholders = ", ".join(["%s"] * len(worker_ids))

        def operation() -> int:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        UPDATE qingka_wangke_order
                        SET runner_status='retrying',
                            status='待处理',
                            dockstatus='98',
                            remarks='Runner手动中断，等待重试',
                            runner_worker_id='',
                            runner_claim_token='',
                            runner_attempts=IF(runner_attempts > 0, runner_attempts - 1, 0),
                            runner_heartbeat_at=%s,
                            runner_next_run_at=DATE_ADD(NOW(), INTERVAL 1 MINUTE),
                            uptime=%s
                        WHERE runner_worker_id IN ({placeholders})
                          AND runner_status IN ('claimed', 'running')
                        """,
                        [now_text(), now_text()] + worker_ids,
                    )
                    affected = cursor.rowcount
                conn.commit()
                return affected

        result = 0

        def wrapped_operation() -> None:
            nonlocal result
            result = operation()

        self.execute_with_retry(wrapped_operation)
        return result

    def update_order(self, oid: int, token: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"`{key}`=%s" for key in fields)

        def operation() -> None:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        UPDATE qingka_wangke_order
                        SET {assignments}, uptime=%s
                        WHERE oid=%s AND runner_claim_token=%s AND runner_worker_id=%s
                        """,
                        list(fields.values()) + [now_text(), oid, token, self.worker_id],
                    )
                conn.commit()

        self.execute_with_retry(operation)
