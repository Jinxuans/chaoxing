import argparse
import configparser
import traceback
from typing import Any

from flask import Flask, jsonify, request

from api.answer import Tiku
from api.base import Account, Chaoxing, SessionManager
from api.cookies import set_cookie_account
from api.logger import logger


def load_query_config(config_path: str) -> dict[str, Any]:
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf8")
    section = dict(config.items("query_server")) if config.has_section("query_server") else {}
    return {
        "host": section.get("host", "127.0.0.1"),
        "port": int(section.get("port", 8765)),
        "token": section.get("token", ""),
    }


def load_tiku_config(config_path: str) -> dict[str, Any]:
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf8")
    if not config.has_section("tiku"):
        return {}
    tiku_config: dict[str, Any] = dict(config.items("tiku"))
    for key in ["delay", "cover_rate"]:
        if key in tiku_config:
            tiku_config[key] = float(tiku_config[key])
    return tiku_config


def build_chaoxing(username: str, password: str, tiku_config: dict[str, Any]) -> Chaoxing:
    set_cookie_account(username)
    SessionManager.get_session().cookies.clear()
    tiku = Tiku()
    tiku.config_set(tiku_config)
    tiku = tiku.get_tiku_from_config()
    tiku.init_tiku()
    return Chaoxing(account=Account(username, password), tiku=tiku, query_delay=tiku_config.get("delay", 0))


def create_app(config_path: str) -> Flask:
    app = Flask(__name__)
    server_config = load_query_config(config_path)
    tiku_config = load_tiku_config(config_path)

    @app.post("/query")
    def query_courses():
        payload = request.get_json(silent=True) or request.form.to_dict()

        if server_config["token"]:
            token = (
                request.headers.get("X-Runner-Token", "")
                or request.headers.get("token", "")
                or str(payload.get("token", ""))
            )
            if token != server_config["token"]:
                return jsonify({"code": -1, "msg": "token错误", "data": []}), 403

        username = str(payload.get("user") or payload.get("username") or "").strip()
        password = str(payload.get("pass") or payload.get("password") or "").strip()
        if not username or not password:
            return jsonify({"code": -1, "msg": "账号密码不能为空", "data": []})

        try:
            chaoxing = build_chaoxing(username, password, tiku_config)
            login_state = chaoxing.login(login_with_cookies=False)
            if not login_state["status"]:
                return jsonify({"code": -1, "msg": login_state["msg"], "data": []})

            courses = chaoxing.get_course_list()
            data = [
                {
                    "id": course.get("courseId", ""),
                    "name": course.get("title", ""),
                    "clazzId": course.get("clazzId", ""),
                    "teacher": course.get("teacher", ""),
                    "credit": course.get("teacher", ""),
                }
                for course in courses
            ]
            return jsonify({"code": 1, "msg": "查询成功", "data": data})
        except Exception as exc:
            logger.error("chaoxing查课失败: {}\n{}", exc, traceback.format_exc())
            return jsonify({"code": -1, "msg": f"查询失败: {type(exc).__name__}: {exc}", "data": []})

    return app


def parse_args():
    parser = argparse.ArgumentParser(description="Chaoxing course query server for CourseX")
    parser.add_argument("-c", "--config", required=True, help="配置文件路径")
    return parser.parse_args()


def main():
    args = parse_args()
    server_config = load_query_config(args.config)
    app = create_app(args.config)
    app.run(host=server_config["host"], port=server_config["port"])


if __name__ == "__main__":
    main()
