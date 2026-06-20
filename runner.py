import argparse
import configparser
import random
import socket
import time
import traceback
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import pymysql
from requests import exceptions as requests_exceptions

from api.answer import Tiku
from api.base import Account, Chaoxing, SessionManager
from api.cookies import set_cookie_account
from api.exceptions import LoginError, ManualVerificationRequired
from api.logger import logger, set_log_worker_id
from api.notification import Notification
from api.proxy import apply_proxy_runtime_config, check_proxy_health, load_proxy_config, proxy_for_slot, set_current_proxy
from main import load_config_from_file, process_course
from runner_scheduler import CourseXOrderQueue, Heartbeat, OrderProgressReporter


def str_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_cids(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def is_transient_network_error(exc: BaseException) -> bool:
    network_types = (
        requests_exceptions.Timeout,
        requests_exceptions.ProxyError,
        requests_exceptions.ConnectionError,
    )
    current: BaseException | None = exc
    seen: set[int] = set()
    while current and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, network_types):
            return True
        current = current.__cause__ or current.__context__
    return False


def load_coursex_config(config_path: str) -> dict[str, Any]:
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf8")
    if not config.has_section("coursex"):
        raise RuntimeError("config.ini 缺少 [coursex] 配置")

    section = dict(config.items("coursex"))
    required = ["host", "user", "database"]
    missing = [key for key in required if not section.get(key)]
    if missing:
        raise RuntimeError(f"[coursex] 缺少配置: {', '.join(missing)}")

    return {
        "host": section["host"],
        "port": int(section.get("port", 3306)),
        "user": section["user"],
        "password": section.get("password", ""),
        "database": section["database"],
        "charset": section.get("charset", "utf8mb4"),
        "poll_interval": float(section.get("poll_interval", 10)),
        "poll_jitter": float(section.get("poll_jitter", 2)),
        "idle_log_interval": float(section.get("idle_log_interval", 60)),
        "maintenance_interval": float(section.get("maintenance_interval", 30)),
        "heartbeat_interval": float(section.get("heartbeat_interval", 30)),
        "progress_min_interval": float(section.get("progress_min_interval", 20)),
        "shutdown_release_timeout": float(section.get("shutdown_release_timeout", 2)),
        "claim_timeout": int(section.get("claim_timeout", 600)),
        "db_connect_timeout": int(section.get("db_connect_timeout", 10)),
        "db_read_timeout": int(section.get("db_read_timeout", 30)),
        "db_write_timeout": int(section.get("db_write_timeout", 30)),
        "db_error_backoff_max": float(section.get("db_error_backoff_max", 60)),
        "max_attempts": int(section.get("max_attempts", 3)),
        "concurrency": int(section.get("concurrency", 1)),
        "once": str_to_bool(section.get("once", False)),
        "worker_id": section.get("worker_id", ""),
        "cids": parse_cids(section.get("cids", "")),
    }


def check_database_connection(config: dict[str, Any], platform: str, worker_id: str) -> None:
    queue = CourseXOrderQueue(config, platform, worker_id)
    try:
        with queue.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.commit()
    except pymysql.err.OperationalError as exc:
        code = exc.args[0] if exc.args else "unknown"
        detail = exc.args[1] if len(exc.args) > 1 else str(exc)
        raise RuntimeError(
            "CourseX数据库连接失败: "
            f"mysql_error={code}, host={config['host']}:{config['port']}, "
            f"user={config['user']}, database={config['database']}, detail={detail}. "
            "请检查 config.ini 的 [coursex] host/user/password/database，"
            "以及 MySQL 用户是否允许当前设备IP远程连接。"
        ) from exc


def build_chaoxing_for_order(order: dict[str, Any], tiku_config: dict[str, Any]) -> Chaoxing:
    set_cookie_account(order["user"])
    SessionManager.get_session().cookies.clear()
    tiku = Tiku()
    tiku.config_set(tiku_config)
    tiku = tiku.get_tiku_from_config()
    tiku.init_tiku()
    return Chaoxing(account=Account(order["user"], order["pass"]), tiku=tiku, query_delay=tiku_config.get("delay", 0))


def run_order(
    order: dict[str, Any],
    common_config: dict[str, Any],
    tiku_config: dict[str, Any],
    progress_callback=None,
) -> None:
    config = dict(common_config)
    config["username"] = order["user"]
    config["password"] = order["pass"]
    config["course_list"] = [str(order["kcid"])] if order.get("kcid") else None
    config["speed"] = min(2.0, max(1.0, float(config.get("speed", 1.0))))
    config["jobs"] = int(config.get("jobs", 4) or 4)
    config["notopen_action"] = config.get("notopen_action", "retry")

    chaoxing = build_chaoxing_for_order(order, tiku_config)
    login_state = chaoxing.login(login_with_cookies=False)
    if not login_state["status"]:
        raise LoginError(login_state["msg"])

    all_courses = chaoxing.get_course_list()
    course_task = [course for course in all_courses if str(course.get("courseId")) == str(order.get("kcid"))]
    if not course_task:
        order_name = str(order.get("kcname") or "").strip()
        course_task = [course for course in all_courses if course.get("title") == order_name]
    if not course_task:
        raise RuntimeError(f"未找到订单课程: kcid={order.get('kcid')} kcname={order.get('kcname')}")

    for course in course_task:
        process_course(chaoxing, course, config, progress_callback=progress_callback)


def parse_args():
    parser = argparse.ArgumentParser(description="CourseX distributed runner for chaoxing orders")
    parser.add_argument("-c", "--config", required=True, help="配置文件路径")
    parser.add_argument("--platform", default="chaoxing", help="Runner平台标识")
    parser.add_argument("--worker-id", default="", help="当前设备Runner ID")
    parser.add_argument("--concurrency", type=int, default=None, help="当前设备同时跑单数量")
    parser.add_argument("--once", action="store_true", help="只领取一个订单后退出")
    return parser.parse_args()


def stop_executor_workers(executor: ProcessPoolExecutor, timeout: float = 5.0) -> None:
    processes = list(getattr(executor, "_processes", {}).values())
    for process in processes:
        if process.is_alive():
            process.terminate()

    deadline = time.time() + timeout
    for process in processes:
        remaining = max(deadline - time.time(), 0)
        if remaining:
            process.join(remaining)

    for process in processes:
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()


def run_claimed_order(
    queue: CourseXOrderQueue,
    order: dict[str, Any],
    common_config: dict[str, Any],
    tiku_config: dict[str, Any],
    notification: Notification,
    heartbeat_interval: float,
    progress_min_interval: float,
    proxy_config: dict[str, Any],
) -> None:
    oid = int(order["oid"])
    token = order["runner_claim_token"]
    set_cookie_account(order.get("user"))
    heartbeat = Heartbeat(queue, oid, token, heartbeat_interval)
    heartbeat.start()
    try:
        logger.info(
            "领取CourseX订单 oid={} worker={} user={} kcname={}",
            oid,
            queue.worker_id,
            order.get("user"),
            order.get("kcname"),
        )
        queue.mark_running(oid, token)
        progress_reporter = OrderProgressReporter(queue, oid, token, progress_min_interval)
        progress_reporter("0%", "Runner开始执行")
        run_order(order, common_config, tiku_config, progress_callback=progress_reporter)
        queue.mark_done(oid, token)
        notification.send(f"CourseX订单完成: {oid} {order.get('kcname')}")
    except (KeyboardInterrupt, SystemExit):
        logger.warning("Runner收到退出信号，释放订单等待重试 oid={} worker={}", oid, queue.worker_id)
        try:
            queue.mark_interrupted(oid, token)
        except Exception as exc:
            logger.warning("订单中断状态写回失败 oid={} error={}", oid, exc)
        raise
    except ManualVerificationRequired as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        logger.warning("订单需要人工验证 oid={} worker={} error={}", oid, queue.worker_id, exc)
        queue.mark_manual_required(oid, token, str(exc), error)
        try:
            notification.send(f"CourseX订单需要人工验证: {oid} {exc}")
        except Exception:
            pass
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        logger.error(error)
        if is_transient_network_error(exc):
            delay_seconds = int(proxy_config.get("network_error_backoff", 180))
            logger.warning(
                "代理/网络异常，订单退回等待重试 oid={} worker={} delay={}s error={}: {}",
                oid,
                queue.worker_id,
                delay_seconds,
                type(exc).__name__,
                exc,
            )
            queue.mark_network_retry(oid, token, error, delay_seconds)
            return
        queue.mark_failed(oid, token, error)
        try:
            notification.send(f"CourseX订单失败: {oid} {type(exc).__name__}: {exc}")
        except Exception:
            pass
    finally:
        heartbeat.stop()


def worker_loop(
    slot: int,
    args: argparse.Namespace,
    coursex_config: dict[str, Any],
    common_config: dict[str, Any],
    tiku_config: dict[str, Any],
    notification_config: dict[str, Any],
    proxy_config: dict[str, Any],
    base_worker_id: str,
) -> None:
    worker_id = f"{base_worker_id}-{slot}"
    set_log_worker_id(worker_id)
    apply_proxy_runtime_config(proxy_config)
    proxy = proxy_for_slot(proxy_config, slot)
    set_current_proxy(proxy)
    if proxy and proxy_config.get("health_check", True):
        check_proxy_health(proxy, proxy_config["health_check_urls"], proxy_config["health_check_timeout"])
    queue = CourseXOrderQueue(coursex_config, args.platform, worker_id)
    notification = Notification()
    notification.config_set(notification_config)
    notification = notification.get_notification_from_config()
    notification.init_notification()
    once = args.once or coursex_config["once"]

    if proxy:
        logger.info("CourseX runner槽位启动 platform={} worker_id={} proxy={}", args.platform, worker_id, proxy.label)
    else:
        logger.info("CourseX runner槽位启动 platform={} worker_id={}", args.platform, worker_id)
    last_idle_log_at = 0.0
    last_maintenance_at = 0.0
    db_error_count = 0
    while True:
        try:
            now = time.time()
            if now - last_maintenance_at >= coursex_config["maintenance_interval"]:
                recovered, queued, ran_maintenance = queue.try_run_maintenance()
                if recovered:
                    logger.warning("回收超时订单: {} worker_id={}", recovered, worker_id)
                if queued:
                    logger.info("同账号运行中，已标记排队订单: {} worker_id={}", queued, worker_id)
                if ran_maintenance:
                    last_maintenance_at = now

            order = queue.claim_next()
        except Exception as exc:
            db_error_count += 1
            retry_seconds = min(
                coursex_config["db_error_backoff_max"],
                coursex_config["poll_interval"] * (2 ** min(db_error_count - 1, 4)),
            )
            if coursex_config["poll_jitter"] > 0:
                retry_seconds += random.uniform(0, coursex_config["poll_jitter"])
            logger.warning(
                "Runner轮询数据库失败，{:.1f}秒后重试 worker_id={} error_count={} error={}: {}",
                retry_seconds,
                worker_id,
                db_error_count,
                type(exc).__name__,
                exc,
            )
            time.sleep(retry_seconds)
            continue

        db_error_count = 0
        if not order:
            if once:
                logger.info("未领取到订单，once模式退出 worker_id={}", worker_id)
                return
            now = time.time()
            if coursex_config["idle_log_interval"] > 0 and now - last_idle_log_at >= coursex_config["idle_log_interval"]:
                logger.info(
                    "暂无可领取订单，{}秒后继续轮询 worker_id={}",
                    coursex_config["poll_interval"],
                    worker_id,
                )
                last_idle_log_at = now
            sleep_seconds = coursex_config["poll_interval"]
            if coursex_config["poll_jitter"] > 0:
                sleep_seconds += random.uniform(0, coursex_config["poll_jitter"])
            time.sleep(sleep_seconds)
            continue

        run_claimed_order(
            queue,
            order,
            common_config,
            tiku_config,
            notification,
            coursex_config["heartbeat_interval"],
            coursex_config["progress_min_interval"],
            proxy_config,
        )

        if once:
            return


def main():
    args = parse_args()
    common_config, tiku_config, notification_config = load_config_from_file(args.config)
    coursex_config = load_coursex_config(args.config)
    base_worker_id = args.worker_id or coursex_config["worker_id"] or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    concurrency = max(1, args.concurrency or coursex_config["concurrency"])
    proxy_config = load_proxy_config(args.config, concurrency)
    logger.info("CourseX runner启动 platform={} worker_id={} concurrency={}", args.platform, base_worker_id, concurrency)
    if proxy_config.get("enabled"):
        logger.info("Runner代理已启用，可用代理数={}", len(proxy_config["entries"]))
    check_database_connection(coursex_config, args.platform, base_worker_id)

    executor = ProcessPoolExecutor(max_workers=concurrency)
    futures = [
        executor.submit(
            worker_loop,
            slot,
            args,
            coursex_config,
            common_config,
            tiku_config,
            notification_config,
            proxy_config,
            base_worker_id,
        )
        for slot in range(1, concurrency + 1)
    ]
    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        logger.warning("收到Ctrl+C，正在停止Runner进程...")
        worker_ids = [f"{base_worker_id}-{slot}" for slot in range(1, concurrency + 1)]
        for future in futures:
            future.cancel()
        stop_executor_workers(executor)

        release_config = dict(coursex_config)
        release_timeout = max(1, int(coursex_config["shutdown_release_timeout"]))
        release_config["db_connect_timeout"] = min(int(release_config["db_connect_timeout"]), release_timeout)
        release_config["db_read_timeout"] = min(int(release_config["db_read_timeout"]), release_timeout)
        release_config["db_write_timeout"] = min(int(release_config["db_write_timeout"]), release_timeout)
        try:
            queue = CourseXOrderQueue(release_config, args.platform, base_worker_id)
            released = queue.mark_workers_interrupted(worker_ids)
            if released:
                logger.warning("已释放当前设备运行中的订单数量: {}", released)
        except KeyboardInterrupt:
            logger.warning("第二次Ctrl+C，跳过订单释放；未完成订单将依赖心跳超时回收")
        except BaseException as exc:
            logger.warning("释放当前设备订单失败，将依赖心跳超时回收 error={}: {}", type(exc).__name__, exc)
        executor.shutdown(wait=False, cancel_futures=True)
        logger.warning("Runner停止信号已发送，已领取但未完成的订单会回到待处理/重试队列")
        raise SystemExit(130)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
