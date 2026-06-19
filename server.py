#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import ipaddress
import re
import sqlite3
import threading
import time
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from html import unescape
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib import request, error, parse

try:
    from ddgs import DDGS
    HAS_DDGS = True
except ImportError:
    HAS_DDGS = False

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "prediction_history.sqlite3"
MODEL = os.getenv("OPENAI_MODEL", "gpt-5.5")
BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com").rstrip("/")
SEARCH_API_URL = os.getenv("SEARCH_API_URL", "").strip()
APIFOOTBALL_API_KEY = os.getenv("APIFOOTBALL_API_KEY", "").strip()
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
FOOTBALL_DATA_TOKEN = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
THESPORTSDB_API_KEY = os.getenv("THESPORTSDB_API_KEY", "3").strip()
APIFOOTBALL_BASE_URL = os.getenv("APIFOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
ODDS_API_BASE_URL = os.getenv("ODDS_API_BASE_URL", "https://api.the-odds-api.com/v4").rstrip("/")
FOOTBALL_DATA_BASE_URL = os.getenv("FOOTBALL_DATA_BASE_URL", "https://api.football-data.org/v4").rstrip("/")
THESPORTSDB_BASE_URL = os.getenv("THESPORTSDB_BASE_URL", "https://www.thesportsdb.com/api/v1/json").rstrip("/")
SPORTTERY_PROXY_URL = os.getenv("SPORTTERY_PROXY_URL", "").strip()
SPORTTERY_PROXY_URLS = [item.strip() for item in os.getenv("SPORTTERY_PROXY_URLS", "").split(",") if item.strip()]
SPORTTERY_OFFICIAL_URL = "https://webapi.sporttery.cn/gateway/uniform/football/getMatchListV1.qry?clientCode=3001"
SPORTTERY_CALCULATOR_URL = "https://webapi.sporttery.cn/gateway/uniform/football/getMatchCalculatorV1.qry?channel=c&poolCode=hhad,had"
SPORTTERY_HTTP_PROXY = os.getenv("SPORTTERY_HTTP_PROXY", "http://127.0.0.1:7890").strip()
BAIDU_WORLDCUP_URL = "https://tiyu.baidu.com/al/match"
CACHE_TTL_SECONDS = 600
PROMPT_VERSION = "casual-v3"
PREDICTION_CACHE: dict[str, dict] = {}
DATA_CACHE: dict[str, dict] = {}
RATE_LIMITS: dict[str, list[float]] = {}
RATE_LIMIT_LOCK = threading.Lock()
SIM_STATE_LOCK = threading.Lock()
SNAPSHOT_CACHE = {
    "odds": {"payload": None, "ts": 0.0, "error": ""},
    "calculator": {"payload": None, "ts": 0.0, "error": ""},
    "results": {"payload": None, "ts": 0.0, "error": ""},
}
SNAPSHOT_LOCK = threading.Lock()
BEIJING_TZ = timezone(timedelta(hours=8))
AUTO_BET_STRATEGY_VERSION = "research-v4-backend"
AUTO_WORKER_ENABLED = os.getenv("AUTO_WORKER_ENABLED", "1") != "0"
AUTO_WORKER_INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_WORKER_INTERVAL_SECONDS", "600")))
AUTO_WORKER_ACCOUNT_ID = os.getenv("AUTO_WORKER_ACCOUNT_ID", "global")
AUTO_WORKER_STARTED = False
SNAPSHOT_WORKER_STARTED = False
PREDICT_RATE_LIMIT_COUNT = max(1, int(os.getenv("PREDICT_RATE_LIMIT_COUNT", "8")))
PREDICT_RATE_LIMIT_SECONDS = max(10, int(os.getenv("PREDICT_RATE_LIMIT_SECONDS", "300")))
PREDICTION_REUSE_SECONDS = max(CACHE_TTL_SECONDS, int(os.getenv("PREDICTION_REUSE_SECONDS", "3600")))
AUTO_BET_RATE_LIMIT_SECONDS = max(60, int(os.getenv("AUTO_BET_RATE_LIMIT_SECONDS", "900")))
AUTO_BET_RETRY_SECONDS = max(300, int(os.getenv("AUTO_BET_RETRY_SECONDS", "3600")))
SIM_SAVE_RATE_LIMIT_COUNT = max(5, int(os.getenv("SIM_SAVE_RATE_LIMIT_COUNT", "20")))
SIM_SAVE_RATE_LIMIT_SECONDS = max(10, int(os.getenv("SIM_SAVE_RATE_LIMIT_SECONDS", "60")))
SIM_SETTLE_RATE_LIMIT_SECONDS = max(10, int(os.getenv("SIM_SETTLE_RATE_LIMIT_SECONDS", "60")))
ODDS_REFRESH_SECONDS = max(30, int(os.getenv("ODDS_REFRESH_SECONDS", "120")))
RESULTS_REFRESH_SECONDS = max(60, int(os.getenv("RESULTS_REFRESH_SECONDS", "300")))
TEAM_EN = {
    "墨西哥": "Mexico",
    "南非": "South Africa",
    "加拿大": "Canada",
    "卡塔尔": "Qatar",
    "美国": "United States",
    "巴拉圭": "Paraguay",
    "德国": "Germany",
    "库拉索": "Curacao",
    "乌拉圭": "Uruguay",
    "沙特阿拉伯": "Saudi Arabia",
    "日本": "Japan",
    "荷兰": "Netherlands",
    "比利时": "Belgium",
    "埃及": "Egypt",
    "瑞士": "Switzerland",
    "奥地利": "Austria",
    "法国": "France",
    "塞内加尔": "Senegal",
    "阿根廷": "Argentina",
    "阿尔及利亚": "Algeria",
    "意大利": "Italy",
    "澳大利亚": "Australia",
    "挪威": "Norway",
    "加纳": "Ghana",
    "葡萄牙": "Portugal",
    "乌兹别克斯坦": "Uzbekistan",
    "摩洛哥": "Morocco",
    "海地": "Haiti",
    "英格兰": "England",
    "克罗地亚": "Croatia",
    "丹麦": "Denmark",
    "突尼斯": "Tunisia",
    "苏格兰": "Scotland",
    "科特迪瓦": "Ivory Coast",
    "巴西": "Brazil",
    "阿联酋": "United Arab Emirates",
    "西班牙": "Spain",
    "佛得角": "Cape Verde",
    "塞尔维亚": "Serbia",
    "新西兰": "New Zealand",
    "哥伦比亚": "Colombia",
    "北马其顿": "North Macedonia",
    "伊朗": "Iran",
    "牙买加": "Jamaica",
}

SYSTEM_PROMPT = """你正在按 Codex skill `worldcup-2026-predictor` 的方法做 2026 世界杯赛前预测。

硬性规则：
1. 严格区分“已知快照/来源事实”和“模型推断”。
2. 不得编造官方大名单、伤病、球员年龄身高、赛果、小组排名、新闻或更衣室消息。
3. 如果页面快照没有提供某项实时资料，必须写“当前快照未提供，需赛前联网补充”，不能用假事实填空。
4. 仍然要给出最终预测，但置信度必须因缺失资料而下调或说明风险。
5. 输出中文，结构清晰，面向页面展开阅读。
6. 直接给结论，不要输出“好的”“我将按照”“下面是分析”“作为AI”等开场白或过程声明。
7. 语气自然、口语化一点，但不要牺牲判断严谨性。"""

RULES = """worldcup-2026-predictor skill 分析流程：
1. 搜集小组赛赛程、已结束比赛结果和小组赛排名。
2. 搜集指定球队信息：官方宣布大名单；球员年龄、身高、近期伤病及影响；更衣室影响球队的消息；其他负面消息；当家球星状态；球队综合实力；球队整体状态。
3. 基于球队信息和对手信息预测该场比赛。
4. 基于小组赛排名预测淘汰赛首轮对阵，但不计算最佳第三名。
5. 考虑球队确保晋级时，是否可能为了淘汰赛路径更有利而放弃第一、选择第二；不得断言故意输球，只能表述为轮换/风险管理/名次动机。
6. 综合以上因素给出最终预测结果。

当前页面基础评分规则：
- 基本面轨道50%：球队实力层级25%，球队/球员状态15%，赛程场地与晋级动机10%。
- 市场/舆情轨道35%：赔率与盘口方向20%，新闻导向10%，公众热度/异常信号5%。
- 裁判与比赛控制15%：裁判执法严谨度、牌点倾向、犯规尺度、VAR/点球风险。
- 如果在线情报里存在 sporttery_calculator_snapshot，必须把其中的胜平负/让球胜平负固定奖金、让球数、单关/串关支持、赔率更新时间作为市场/投注轨道依据；它不是赛果事实。

双轨背离预测：
- 基本面轨道和市场/舆情轨道同向：提高置信度。
- 基本面支持一方，但赔率或新闻导向支持另一方：标记“背离”，降低置信度并解释可能原因。
- 裁判严谨度高时，提高红黄牌、点球、定位球和弱队守平概率的权重。

- 平局触发判断：
  1. 平局综合概率高于或贴近主胜/客胜最高值时，必须把“平局”作为独立候选，而不是只写冷门风险。
  2. 强弱差距小、盘口接近均势、静态平局概率约 29% 以上，且最高胜率低于约 43% 时，优先考虑平局。
  3. 若静态判断为“中”置信度、最高胜率不超过约 46%、平局概率达到约 29%，且平局距离最高胜率不超过约 13 个百分点，应进入低比分平局候选；边界值允许半个百分点容差，避免四舍五入后漏判。
  4. 小组赛首战、低比分预期、防守纪律强、临场阵容不明或裁判尺度偏严时，上调平局权重。
  5. 如果最终不选平局，必须解释为什么平局风险不足以压过主胜/客胜。

置信度分档：极高=胜率约75%以上；高=63%-74%；中高=55%-62%；中=低于55%或平局权重高。"""


def beijing_time() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M 北京时间")


def client_ip(handler: SimpleHTTPRequestHandler) -> str:
    remote = handler.client_address[0]
    try:
        remote_ip = ipaddress.ip_address(remote)
    except ValueError:
        return remote
    if remote_ip.is_loopback or remote_ip.is_private:
        forwarded = handler.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if forwarded:
            return forwarded
    return remote


def is_local_request(handler: SimpleHTTPRequestHandler) -> bool:
    return handler.client_address[0] in ("127.0.0.1", "::1", "localhost")


def rate_limited(bucket: str, limit: int, window_seconds: int) -> bool:
    now = time.monotonic()
    cutoff = now - window_seconds
    with RATE_LIMIT_LOCK:
        hits = [ts for ts in RATE_LIMITS.get(bucket, []) if ts >= cutoff]
        if len(hits) >= limit:
            RATE_LIMITS[bucket] = hits
            return True
        hits.append(now)
        RATE_LIMITS[bucket] = hits
        return False


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                home TEXT,
                away TEXT,
                match_date TEXT,
                match_time TEXT,
                static_score TEXT,
                summary TEXT,
                analysis TEXT NOT NULL,
                sources_json TEXT,
                model TEXT,
                prompt_version TEXT,
                generated_at TEXT NOT NULL,
                created_ts REAL NOT NULL
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(predictions)").fetchall()}
        if "prompt_version" not in columns:
            conn.execute("ALTER TABLE predictions ADD COLUMN prompt_version TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_match_id ON predictions(match_id, created_ts DESC)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sim_accounts (
                account_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                updated_ts REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS odds_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_key TEXT NOT NULL,
                home TEXT,
                away TEXT,
                match_time TEXT,
                match_no TEXT,
                decimal_json TEXT NOT NULL,
                source TEXT,
                captured_at TEXT NOT NULL,
                captured_ts REAL NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_odds_snapshots_match ON odds_snapshots(match_key, captured_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_odds_snapshots_teams ON odds_snapshots(home, away, captured_ts DESC)")


def cache_key(match: dict) -> str:
    base_key = str(match.get("id") or f"{match.get('home', '')}-{match.get('away', '')}-{match.get('d', '')}-{match.get('time', '')}")
    return f"{PROMPT_VERSION}:{base_key}"


def match_id_key(match: dict) -> str:
    return str(match.get("id") or f"{match.get('home', '')}-{match.get('away', '')}-{match.get('d', '')}-{match.get('time', '')}")


def prediction_reuse_seconds(match: dict) -> int:
    kickoff = parse_beijing_kickoff(match)
    if not kickoff:
        return PREDICTION_REUSE_SECONDS
    seconds_to_kickoff = (kickoff - datetime.now(BEIJING_TZ)).total_seconds()
    if seconds_to_kickoff <= 0:
        return PREDICTION_REUSE_SECONDS
    if seconds_to_kickoff <= 30 * 60:
        return max(CACHE_TTL_SECONDS, 10 * 60)
    if seconds_to_kickoff <= 2 * 60 * 60:
        return max(CACHE_TTL_SECONDS, 30 * 60)
    return PREDICTION_REUSE_SECONDS


def get_prediction(api_key: str, match: dict) -> dict:
    key = cache_key(match)
    match_id = match_id_key(match)
    finished = finished_result_for_match(match)
    if finished:
        score = f"{finished['home_score']}-{finished['away_score']}"
        return {
            "id": match.get("id"),
            "analysis": f"这场比赛已经完赛，赛果 {score}，结果为{finished['pick']}。已完赛场次不再生成赛前预测，也不会进入自动下注。",
            "summary": f"已完赛：{score}，{finished['pick']}。不再预测。",
            "actual_score": score,
            "actual_pick": finished["pick"],
            "finished": True,
            "source": finished.get("source", ""),
            "model": MODEL,
            "generated_at": beijing_time(),
            "cached": False,
        }
    reuse_seconds = prediction_reuse_seconds(match)
    now = datetime.now(timezone.utc).timestamp()
    cached = PREDICTION_CACHE.get(key)
    if cached and now - cached["ts"] < reuse_seconds:
        payload = dict(cached["payload"])
        payload["cached"] = True
        payload["cache_source"] = "memory"
        payload["cache_ttl_seconds"] = reuse_seconds
        return payload
    persisted = get_recent_prediction_payload(match_id, reuse_seconds, prompt_version=PROMPT_VERSION)
    if persisted:
        PREDICTION_CACHE[key] = {"ts": now, "payload": persisted}
        payload = dict(persisted)
        payload["cached"] = True
        payload["cache_source"] = "sqlite"
        payload["cache_ttl_seconds"] = reuse_seconds
        return payload
    intelligence = collect_match_intelligence(match)
    analysis = call_openai(api_key, match, intelligence)
    payload = {
        "id": match.get("id"),
        "analysis": analysis,
        "summary": summarize_analysis(analysis),
        "sources": intelligence,
        "model": MODEL,
        "generated_at": beijing_time(),
        "cache_ttl_seconds": reuse_seconds,
        "cached": False,
    }
    save_prediction(match, payload)
    PREDICTION_CACHE[key] = {"ts": now, "payload": payload}
    return payload


def get_recent_prediction_payload(match_id: str, max_age_seconds: int, prompt_version: str | None = None) -> dict | None:
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_seconds
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM predictions
            WHERE match_id = ? AND created_ts >= ? AND (? IS NULL OR prompt_version = ?)
            ORDER BY created_ts DESC
            LIMIT 1
            """,
            (str(match_id), cutoff, prompt_version, prompt_version),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    try:
        sources = json.loads(item.get("sources_json") or "[]")
    except json.JSONDecodeError:
        sources = []
    return {
        "id": int(match_id) if str(match_id).isdigit() else match_id,
        "analysis": item.get("analysis", ""),
        "summary": item.get("summary", ""),
        "sources": sources,
        "model": item.get("model", MODEL),
        "generated_at": item.get("generated_at", ""),
        "cached": True,
    }


def save_prediction(match: dict, payload: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO predictions (
                match_id, home, away, match_date, match_time, static_score,
                summary, analysis, sources_json, model, prompt_version, generated_at, created_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(match.get("id", "")),
                match.get("home", ""),
                match.get("away", ""),
                match.get("d", ""),
                match.get("time", ""),
                match.get("score", ""),
                payload.get("summary", ""),
                payload.get("analysis", ""),
                json.dumps(payload.get("sources", []), ensure_ascii=False),
                payload.get("model", ""),
                PROMPT_VERSION,
                payload.get("generated_at", ""),
                datetime.now(timezone.utc).timestamp(),
            ),
        )


def get_history(match_id: str | None = None, limit: int = 50) -> list[dict]:
    limit = max(1, min(limit, 100))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if match_id:
            rows = conn.execute(
                """
                SELECT * FROM predictions
                WHERE match_id = ?
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (str(match_id), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM predictions
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        analysis = item.pop("analysis", "") or ""
        try:
            item["sources"] = json.loads(item.pop("sources_json") or "[]")[:5]
        except json.JSONDecodeError:
            item["sources"] = []
        item["predicted_score"] = normalize_score_text(extract_predicted_score(analysis))
        item["predicted_pick"] = pick_from_score(item["predicted_score"]) or extract_predicted_pick(f"{analysis} {item.get('summary', '')}")
        result.append(item)
    return result


def get_prediction_matches(limit: int = 5000) -> list[dict]:
    limit = max(1, min(limit, 5000))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT match_id, home, away, static_score, summary, analysis, model, prompt_version, generated_at, created_ts
            FROM predictions
            ORDER BY created_ts DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        analysis = item.pop("analysis", "") or ""
        item["predicted_score"] = normalize_score_text(extract_predicted_score(analysis))
        item["predicted_pick"] = pick_from_score(item["predicted_score"]) or extract_predicted_pick(f"{analysis} {item.get('summary', '')}")
        result.append(item)
    return result


def build_prediction_snapshot() -> dict:
    latest_by_fixture = {}
    for item in get_prediction_matches(limit=5000):
        key = strict_fixture_key(item.get("home", ""), item.get("away", ""))
        if not key or key in latest_by_fixture:
            continue
        if not item.get("predicted_score") and not item.get("predicted_pick"):
            continue
        latest_by_fixture[key] = {
            "match_id": item.get("match_id", ""),
            "home": item.get("home", ""),
            "away": item.get("away", ""),
            "predicted_score": item.get("predicted_score", ""),
            "predicted_pick": item.get("predicted_pick", ""),
            "summary": item.get("summary", ""),
            "model": item.get("model", ""),
            "prompt_version": item.get("prompt_version", ""),
            "generated_at": item.get("generated_at", ""),
        }
    return {
        "predictions": list(latest_by_fixture.values()),
        "total_predictions": len(latest_by_fixture),
        "generated_at": beijing_time(),
        "source": "SQLite 严格同场赛前预测",
    }


def default_sim_state(account_id: str) -> dict:
    return {
        "account_id": account_id,
        "account": {"initial": 10000, "cash": 10000, "bets": []},
        "history": [],
        "post_reviews": [],
        "stats": {},
        "settlement": {},
        "last_signature": "",
        "updated_at": beijing_time(),
    }


def get_sim_state(account_id: str | None) -> dict:
    account_id = re.sub(r"[^a-zA-Z0-9_-]", "", account_id or "")[:48] or uuid.uuid4().hex[:12]
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT state_json FROM sim_accounts WHERE account_id = ?", (account_id,)).fetchone()
    if not row:
        state = default_sim_state(account_id)
        save_sim_state(state)
        return state
    try:
        state = json.loads(row["state_json"])
    except json.JSONDecodeError:
        state = default_sim_state(account_id)
    state["account_id"] = account_id
    return state


def save_sim_state(state: dict) -> dict:
    account_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(state.get("account_id") or ""))[:48] or uuid.uuid4().hex[:12]
    account = state.get("account") if isinstance(state.get("account"), dict) else {"initial": 10000, "cash": 10000, "bets": []}
    history = state.get("history") if isinstance(state.get("history"), list) else []
    post_reviews = state.get("post_reviews") if isinstance(state.get("post_reviews"), list) else []
    worker_log = state.get("worker_log") if isinstance(state.get("worker_log"), list) else []
    safe_state = {
        "account_id": account_id,
        "account": account,
        "history": history[:50],
        "post_reviews": post_reviews[:100],
        "stats": state.get("stats") if isinstance(state.get("stats"), dict) else calculate_sim_stats(account),
        "settlement": state.get("settlement") if isinstance(state.get("settlement"), dict) else {},
        "worker": state.get("worker") if isinstance(state.get("worker"), dict) else {},
        "worker_log": worker_log[:50],
        "last_signature": str(state.get("last_signature") or "")[:8000],
        "updated_at": beijing_time(),
    }
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO sim_accounts (account_id, state_json, updated_at, updated_ts)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at,
                updated_ts = excluded.updated_ts
            """,
            (
                account_id,
                json.dumps(safe_state, ensure_ascii=False),
                safe_state["updated_at"],
                datetime.now(timezone.utc).timestamp(),
            ),
        )
    return safe_state


def item_identity(item: dict) -> str:
    if item.get("key"):
        return f"key:{item.get('key')}"
    if item.get("id") or item.get("match") or item.get("pick"):
        return f"bet:{item.get('id')}:{item.get('match')}:{item.get('pick')}:{item.get('placedAt')}"
    return json.dumps(item, ensure_ascii=False, sort_keys=True)[:500]


def merge_unique_items(primary: list, secondary: list, limit: int) -> list:
    merged = []
    seen = set()
    for item in primary + secondary:
        if not isinstance(item, dict):
            continue
        identity = item_identity(item)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(item)
        if len(merged) >= limit:
            break
    return merged


def merge_client_account(current_account: dict, incoming_account: dict) -> dict:
    if not isinstance(current_account, dict):
        current_account = {"initial": 10000, "cash": 10000, "bets": []}
    if not isinstance(incoming_account, dict):
        incoming_account = {"initial": 10000, "cash": 10000, "bets": []}
    merged = dict(incoming_account)
    current_bets = current_account.get("bets") if isinstance(current_account.get("bets"), list) else []
    incoming_bets = incoming_account.get("bets") if isinstance(incoming_account.get("bets"), list) else []
    merged["bets"] = merge_unique_items(incoming_bets, current_bets, 500)
    try:
        merged["cash"] = min(float(incoming_account.get("cash") or 0), float(current_account.get("cash") or 0))
    except (TypeError, ValueError):
        merged["cash"] = incoming_account.get("cash", current_account.get("cash", 10000))
    return merged


def save_client_sim_state(state: dict) -> dict:
    with SIM_STATE_LOCK:
        account_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(state.get("account_id") or ""))[:48] or uuid.uuid4().hex[:12]
        current = get_sim_state(account_id)
        if not state.get("reset"):
            state["account"] = merge_client_account(current.get("account") or {}, state.get("account") or {})
            state["history"] = merge_unique_items(
                state.get("history") if isinstance(state.get("history"), list) else [],
                current.get("history") if isinstance(current.get("history"), list) else [],
                50,
            )
            state["post_reviews"] = merge_unique_items(
                state.get("post_reviews") if isinstance(state.get("post_reviews"), list) else [],
                current.get("post_reviews") if isinstance(current.get("post_reviews"), list) else [],
                100,
            )
        for key in ("settlement", "worker", "worker_log"):
            if key in current:
                state[key] = current[key]
        return save_sim_state(state)


def normalize_team_text(value: str) -> str:
    folded = "".join(ch for ch in unicodedata.normalize("NFKD", str(value or "").lower()) if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", folded)
    aliases = {
        "沙特阿拉伯": "沙特",
        "刚果金": "刚果金",
        "刚果民主共和国": "刚果金",
        "乌兹别克斯坦": "乌兹别克",
        "curacao": "库拉索",
        "democraticrepublicofthecongo": "刚果金",
        "drcongo": "刚果金",
    }
    return aliases.get(text, text)


EXTRA_CN_TO_EN = {
    "韩国": "South Korea",
    "捷克": "Czech Republic",
    "厄瓜多尔": "Ecuador",
    "土耳其": "Turkey",
    "瑞典": "Sweden",
    "约旦": "Jordan",
    "巴拿马": "Panama",
    "伊拉克": "Iraq",
    "波黑": "Bosnia and Herzegovina",
    "库拉索": "Curacao",
    "刚果金": "Democratic Republic of the Congo",
}
EN_TO_CN = {v.lower(): k for k, v in {**TEAM_EN, **EXTRA_CN_TO_EN}.items()}


def team_name_candidates(name: str) -> list[str]:
    raw = str(name or "").strip()
    candidates = [raw]
    if raw in TEAM_EN:
        candidates.append(TEAM_EN[raw])
    if raw in EXTRA_CN_TO_EN:
        candidates.append(EXTRA_CN_TO_EN[raw])
    cn = EN_TO_CN.get(raw.lower())
    if cn:
        candidates.append(cn)
    seen = set()
    result = []
    for item in candidates:
        normalized = normalize_team_text(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(item)
    return result


def canonical_fixture_team(name: str) -> str:
    raw = str(name or "").strip()
    cn = EN_TO_CN.get(raw.lower())
    return normalize_team_text(cn or raw)


def strict_fixture_key(home: str, away: str) -> str:
    home_key = canonical_fixture_team(home)
    away_key = canonical_fixture_team(away)
    if not home_key or not away_key:
        return ""
    return f"{home_key}-{away_key}"


def match_result_for_teams(home: str, away: str, results: dict) -> dict | None:
    for home_try in team_name_candidates(home):
        for away_try in team_name_candidates(away):
            key = f"{normalize_team_text(home_try)}-{normalize_team_text(away_try)}"
            if key in results:
                return results[key]
    return None


def finished_result_for_match(match: dict) -> dict | None:
    if match.get("finished_result"):
        return match.get("finished_result")
    return match_result_for_teams(match.get("home", ""), match.get("away", ""), fetch_worldcup_results())


def legacy_odds_match_key(item: dict) -> str:
    if item.get("match_no"):
        return f"no:{item.get('match_no')}"
    return f"{normalize_team_text(item.get('home'))}-{normalize_team_text(item.get('away'))}-{normalize_team_text(item.get('match_time'))}"


def odds_match_key(item: dict) -> str:
    market_code = str(item.get("market_code") or item.get("marketCode") or "HAD").strip().upper() or "HAD"
    goal_line = str(item.get("goal_line") or item.get("goalLine") or "").strip()
    market_suffix = f":{market_code}:{goal_line}" if goal_line else f":{market_code}"
    if item.get("match_no"):
        return f"no:{item.get('match_no')}{market_suffix}"
    return (
        f"{normalize_team_text(item.get('home'))}-"
        f"{normalize_team_text(item.get('away'))}-"
        f"{normalize_team_text(item.get('match_time'))}{market_suffix}"
    )


def save_odds_snapshots(matches: list[dict], source: str) -> None:
    if not matches:
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    now_text = beijing_time()
    rows = []
    with sqlite3.connect(DB_PATH) as conn:
        for item in matches:
            key = odds_match_key(item)
            decimal = item.get("decimal") or []
            if len(decimal) < 3:
                continue
            previous = conn.execute(
                "SELECT decimal_json FROM odds_snapshots WHERE match_key = ? ORDER BY captured_ts DESC LIMIT 1",
                (key,),
            ).fetchone()
            current_json = json.dumps([round(float(value), 4) for value in decimal], ensure_ascii=False)
            if previous and previous[0] == current_json:
                continue
            rows.append((
                key,
                item.get("home", ""),
                item.get("away", ""),
                item.get("match_time", ""),
                item.get("match_no", ""),
                current_json,
                source,
                now_text,
                now_ts,
            ))
        if rows:
            conn.executemany(
                """
                INSERT INTO odds_snapshots (
                    match_key, home, away, match_time, match_no, decimal_json, source, captured_at, captured_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )


def get_odds_trends(matches: list[dict], limit: int = 12) -> dict:
    trends = {}
    if not matches:
        return trends
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        for item in matches:
            key = odds_match_key(item)
            rows = conn.execute(
                """
                SELECT decimal_json, captured_at, captured_ts
                FROM odds_snapshots
                WHERE match_key = ?
                ORDER BY captured_ts DESC
                LIMIT ?
                """,
                (key, limit),
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    """
                    SELECT decimal_json, captured_at, captured_ts
                    FROM odds_snapshots
                    WHERE match_key = ?
                    ORDER BY captured_ts DESC
                    LIMIT ?
                    """,
                    (legacy_odds_match_key(item), limit),
                ).fetchall()
            points = []
            for row in reversed(rows):
                try:
                    decimal = json.loads(row["decimal_json"])
                except json.JSONDecodeError:
                    continue
                points.append({
                    "decimal": decimal,
                    "captured_at": row["captured_at"],
                    "captured_ts": row["captured_ts"],
                })
            trends[key] = points
    return trends


def result_pick(home_score: int, away_score: int) -> str:
    if home_score > away_score:
        return "主胜"
    if home_score == away_score:
        return "平局"
    return "客胜"


def fetch_baidu_text_cached(url: str, ttl_seconds: int = 300, headers: dict | None = None) -> str:
    cached = DATA_CACHE.get(url)
    now = time.monotonic()
    if cached and now - cached["ts"] < ttl_seconds:
        return cached["data"]
    req = request.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
    with request.urlopen(req, timeout=12) as resp:
        text = resp.read().decode("utf-8", "ignore")
    DATA_CACHE[url] = {"ts": now, "data": text}
    return text


def baidu_schedule_url(date_text: str) -> str:
    return f"{BAIDU_WORLDCUP_URL}?{parse.urlencode({'match': '世界杯', 'date_time': date_text, 'tab': '赛程', 'from': 'baidu_aladdin'})}"


def parse_baidu_schedule_days(html: str) -> list[dict]:
    decoder = json.JSONDecoder()
    days = []
    seen_starts = set()
    for match in re.finditer(r'\{"time":"20\d{2}-\d{2}-\d{2}"', html):
        start = match.start()
        if start in seen_starts:
            continue
        seen_starts.add(start)
        try:
            day, _ = decoder.raw_decode(html[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(day, dict) and isinstance(day.get("list"), list):
            days.append(day)
    return days


def parse_baidu_results(html: str) -> dict:
    results = {}
    for day in parse_baidu_schedule_days(html):
        for game in day.get("list", []):
            if not isinstance(game, dict):
                continue
            status_text = str(game.get("matchStatusText") or game.get("dataSourceText") or "")
            status = str(game.get("status") or game.get("matchStatus") or "")
            if "已结束" not in status_text and status != "2":
                continue
            left = game.get("leftLogo") if isinstance(game.get("leftLogo"), dict) else {}
            right = game.get("rightLogo") if isinstance(game.get("rightLogo"), dict) else {}
            home = str(left.get("name") or "").strip()
            away = str(right.get("name") or "").strip()
            try:
                hs = int(str(left.get("score") or "").strip())
                away_s = int(str(right.get("score") or "").strip())
            except (TypeError, ValueError):
                continue
            if not home or not away:
                continue
            item = {
                "home": home,
                "away": away,
                "home_score": hs,
                "away_score": away_s,
                "pick": result_pick(hs, away_s),
                "source": "百度体育",
            }
            for home_try in team_name_candidates(home):
                for away_try in team_name_candidates(away):
                    key = f"{normalize_team_text(home_try)}-{normalize_team_text(away_try)}"
                    if key != "-":
                        results[key] = item
    return results


def fetch_baidu_worldcup_results() -> dict:
    today = datetime.now(BEIJING_TZ).date()
    start = datetime(2026, 6, 12, tzinfo=BEIJING_TZ).date()
    if today < start:
        dates = [today]
    else:
        days = min((today - start).days, 45)
        dates = [start + timedelta(days=offset) for offset in range(days + 1)]
    results = {}
    for date_value in dates:
        try:
            html = fetch_baidu_text_cached(baidu_schedule_url(date_value.isoformat()), ttl_seconds=300, headers={
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
                "Referer": "https://www.baidu.com/",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            })
            results.update(parse_baidu_results(html))
        except Exception:
            continue
    return results


def fetch_worldcup_results() -> dict:
    results = fetch_baidu_worldcup_results()
    try:
        data = fetch_json_cached("https://worldcup26.ir/get/games", ttl_seconds=300)
    except Exception:
        return results
    for game in data.get("games", []):
        if str(game.get("finished", "")).upper() != "TRUE":
            continue
        try:
            hs = int(game.get("home_score"))
            away_s = int(game.get("away_score"))
        except (TypeError, ValueError):
            continue
        home_en = game.get("home_team_name_en", "")
        away_en = game.get("away_team_name_en", "")
        home_fa = game.get("home_team_fa", "")
        away_fa = game.get("away_team_fa", "")
        item = {
            "home": home_en,
            "away": away_en,
            "home_score": hs,
            "away_score": away_s,
            "pick": result_pick(hs, away_s),
            "source": "worldcup26.ir",
        }
        for home, away in ((home_en, away_en), (home_fa, away_fa)):
            key = f"{normalize_team_text(home)}-{normalize_team_text(away)}"
            if key != "-" and key not in results:
                results[key] = item
    return results


def match_result_for_bet(bet: dict, results: dict) -> dict | None:
    text = bet.get("match", "")
    parts = re.split(r"\s+vs\s+", text, flags=re.I)
    if len(parts) != 2:
        return None
    return match_result_for_teams(parts[0], parts[1], results)


def calculate_sim_stats(account: dict) -> dict:
    bets = account.get("bets") if isinstance(account.get("bets"), list) else []
    settled = [bet for bet in bets if bet.get("status") in ("已中", "未中")]
    pending = [bet for bet in bets if bet.get("status") not in ("已中", "未中", "已取消")]
    won = [bet for bet in settled if bet.get("status") == "已中"]
    singles = [bet for bet in bets if bet.get("type") != "parlay"]
    parlays = [bet for bet in bets if bet.get("type") == "parlay"]
    settled_stake = sum(float(bet.get("stake") or 0) for bet in settled)
    pending_stake = sum(float(bet.get("stake") or 0) for bet in pending)
    realized = sum(float(bet.get("settledProfit") or 0) for bet in settled)
    total_stake = sum(float(bet.get("stake") or 0) for bet in bets)
    initial = float(account.get("initial") or 0)
    cash = float(account.get("cash") or 0)
    return {
        "initial": round(initial),
        "cash": round(cash),
        "total_bets": len(bets),
        "pending_bets": len(pending),
        "settled_bets": len(settled),
        "won_bets": len(won),
        "lost_bets": len(settled) - len(won),
        "single_bets": len(singles),
        "parlay_bets": len(parlays),
        "pending_stake": round(pending_stake),
        "total_stake": round(total_stake),
        "settled_stake": round(settled_stake),
        "realized_profit": round(realized),
        "equity": round(cash + pending_stake),
        "win_rate": round((len(won) / len(settled)) * 100, 1) if settled else 0,
        "roi": round((realized / settled_stake) * 100, 1) if settled_stake else 0,
    }


def bet_review_key(bet: dict) -> str:
    return str(bet.get("key") or bet.get("id") or bet.get("match") or "")


def build_rule_based_post_review(bet: dict, history: list[dict]) -> dict:
    matched_history = None
    bet_ids = {str(bet.get("id"))}
    if bet.get("type") == "parlay":
        bet_ids = {str(leg.get("id")) for leg in bet.get("legs", []) if leg.get("id") is not None}
    for item in history:
        decision_ids = {str(decision.get("id")) for decision in item.get("decisions", []) if decision.get("id") is not None}
        if bet_ids & decision_ids:
            matched_history = item
            break
    status = bet.get("status")
    hit_text = "命中" if status == "已中" else "未命中"
    profit = round(float(bet.get("settledProfit") or 0))
    reason = bet.get("reason") or "原始下注理由未记录。"
    mode = matched_history.get("planMode") if matched_history else ""
    summary = f"{hit_text}，已实现盈亏 {profit}。"
    if bet.get("type") == "parlay":
        legs = bet.get("legResults") or []
        detail = "；".join(f"{leg.get('match')} 选{leg.get('pick')}，赛果{leg.get('result')}，实际{leg.get('actual')}" for leg in legs) or "串关分项赛果暂缺。"
        lesson = "串关需要全部条件同时成立，后续应降低相关性不明的多关组合仓位。"
    else:
        detail = f"{bet.get('match')} 选择{bet.get('pick')}，赛果{bet.get('result', '未记录')}。"
        lesson = "后续应复核赛前概率、赔率水位和临场阵容新闻是否被高估或低估。"
    return {
        "key": bet_review_key(bet),
        "time": beijing_time(),
        "match": bet.get("match"),
        "type": "串关" if bet.get("type") == "parlay" else "单关",
        "status": status,
        "stake": bet.get("stake"),
        "odds": bet.get("odds"),
        "profit": profit,
        "summary": summary,
        "detail": f"下注理由：{reason} {detail}",
        "lesson": f"下注模式：{mode or bet.get('pick') or '未记录'}。{lesson}",
        "model": "rule-review",
    }


def generate_missing_post_reviews(state: dict) -> int:
    account = state.get("account") if isinstance(state.get("account"), dict) else {}
    history = state.get("history") if isinstance(state.get("history"), list) else []
    post_reviews = state.get("post_reviews") if isinstance(state.get("post_reviews"), list) else []
    existing = {str(item.get("key")) for item in post_reviews}
    added = 0
    for bet in account.get("bets", []) if isinstance(account.get("bets"), list) else []:
        if bet.get("status") not in ("已中", "未中"):
            continue
        key = bet_review_key(bet)
        if not key or key in existing:
            continue
        post_reviews.insert(0, build_rule_based_post_review(bet, history))
        existing.add(key)
        added += 1
    state["post_reviews"] = post_reviews[:100]
    return added


def result_pick_for_market(result: dict, market_code: str | None = None, goal_line=None) -> str:
    if str(market_code or "HAD").upper() != "HHAD":
        return result.get("pick") or result_pick(int(result.get("home_score", 0)), int(result.get("away_score", 0)))
    try:
        adjusted_home = float(result.get("home_score", 0)) + float(goal_line or 0)
        away = float(result.get("away_score", 0))
    except (TypeError, ValueError):
        return result.get("pick") or ""
    if adjusted_home > away:
        return "主胜"
    if adjusted_home == away:
        return "平局"
    return "客胜"


def market_label_for_bet(item: dict) -> str:
    if str(item.get("marketCode") or "HAD").upper() != "HHAD":
        return "胜平负"
    goal_line = str(item.get("goalLine") or "").strip()
    return f"让球胜平负({goal_line})" if goal_line else "让球胜平负"


def settle_single_bet(bet: dict, result: dict, account: dict) -> bool:
    bet["result"] = f"{result['home_score']}-{result['away_score']}"
    actual_pick = result_pick_for_market(result, bet.get("marketCode"), bet.get("goalLine"))
    bet["actualPick"] = actual_pick
    if actual_pick == bet.get("pick"):
        bet["status"] = "已中"
        payout = round(float(bet.get("stake", 0)) * float(bet.get("odds", 1)))
        profit = payout - round(float(bet.get("stake", 0)))
        bet["settledProfit"] = profit
        account["cash"] = float(account.get("cash", 0)) + payout
    else:
        bet["status"] = "未中"
        bet["settledProfit"] = -round(float(bet.get("stake", 0)))
    return True


def settle_parlay_bet(bet: dict, results: dict, account: dict) -> bool:
    legs = bet.get("legs") if isinstance(bet.get("legs"), list) else []
    if len(legs) < 2:
        return False
    settled_legs = []
    for leg in legs:
        result = match_result_for_bet(leg, results)
        if not result:
            return False
        settled_legs.append((leg, result))
    bet["legResults"] = [
        {
            "match": leg.get("match"),
            "pick": leg.get("pick"),
            "result": f"{result['home_score']}-{result['away_score']}",
            "actual": result_pick_for_market(result, leg.get("marketCode"), leg.get("goalLine")),
            "market": market_label_for_bet(leg),
        }
        for leg, result in settled_legs
    ]
    all_hit = all(result_pick_for_market(result, leg.get("marketCode"), leg.get("goalLine")) == leg.get("pick") for leg, result in settled_legs)
    if all_hit:
        bet["status"] = "已中"
        payout = round(float(bet.get("stake", 0)) * float(bet.get("odds", 1)))
        bet["settledProfit"] = payout - round(float(bet.get("stake", 0)))
        account["cash"] = float(account.get("cash", 0)) + payout
    else:
        bet["status"] = "未中"
        bet["settledProfit"] = -round(float(bet.get("stake", 0)))
    return True


def settle_sim_account(account_id: str | None) -> dict:
    results = fetch_worldcup_results()
    with SIM_STATE_LOCK:
        state = get_sim_state(account_id or "global")
        account = state.get("account") or {"initial": 10000, "cash": 10000, "bets": []}
        bets = account.get("bets") if isinstance(account.get("bets"), list) else []
        changed = 0
        for bet in bets:
            if bet.get("status") in ("已中", "未中", "已取消"):
                continue
            if bet.get("type") == "parlay":
                if settle_parlay_bet(bet, results, account):
                    changed += 1
                continue
            result = match_result_for_bet(bet, results)
            if not result:
                continue
            if settle_single_bet(bet, result, account):
                changed += 1
        state["account"] = account
        state["stats"] = calculate_sim_stats(account)
        state["settlement"] = {
            "last_checked_at": beijing_time(),
            "last_settled": changed,
            "results_count": len(results),
            "source": "worldcup26.ir",
        }
        reviews_added = generate_missing_post_reviews(state)
        save_sim_state(state)
        return {
            "account_id": state.get("account_id"),
            "settled": changed,
            "reviews_added": reviews_added,
            "results_count": len(results),
            "account": account,
            "history": state.get("history", []),
            "post_reviews": state.get("post_reviews", []),
            "stats": state.get("stats", {}),
            "settlement": state.get("settlement", {}),
            "worker": state.get("worker", {}),
            "worker_log": state.get("worker_log", []),
            "updated_at": beijing_time(),
        }


def get_sim_snapshot(account_id: str | None, auto_settle: bool = True) -> dict:
    if auto_settle:
        return settle_sim_account(account_id or "global")
    state = get_sim_state(account_id or "global")
    state["stats"] = calculate_sim_stats(state.get("account") or {})
    return state


def bet_key_for_pick(match: dict, pick: str) -> str:
    market = str(match.get("marketCode") or "HAD")
    goal_line = str(match.get("goalLine") or "")
    return f"{match.get('id')}:{match.get('home')}-{match.get('away')}:{market}:{goal_line}:{pick}"


def bet_leg_key(leg: dict) -> str:
    market = str(leg.get("marketCode") or "HAD")
    goal_line = str(leg.get("goalLine") or "")
    return f"{leg.get('id')}:{market}:{goal_line}:{leg.get('pick')}"


def apply_auto_bet_plan_to_state(state: dict, matches: list[dict], plan: dict, signature: str, trigger: str) -> dict:
    account = state.get("account") if isinstance(state.get("account"), dict) else {"initial": 10000, "cash": 10000, "bets": []}
    bets = account.get("bets") if isinstance(account.get("bets"), list) else []
    account["bets"] = bets
    match_by_id = {str(match.get("id")): match for match in matches}
    existing = {str(bet.get("key")) for bet in bets if bet.get("key")}
    placed = 0
    for item in plan.get("bets", []) if isinstance(plan.get("bets"), list) else []:
        mid = str(item.get("id"))
        match = match_by_id.get(mid)
        pick = item.get("pick")
        if not match or pick not in ("主胜", "平局", "客胜"):
            continue
        key = bet_key_for_pick(match, pick)
        if key in existing:
            continue
        try:
            stake = min(round(float(item.get("stake") or 0)), int(float(account.get("cash") or 0)))
        except (TypeError, ValueError):
            stake = 0
        if stake <= 0:
            continue
        odds = decimal_odds_for_pick(match, pick)
        if not odds:
            continue
        account["cash"] = float(account.get("cash") or 0) - stake
        bets.append({
            "key": key,
            "id": match.get("id"),
            "match": f"{match.get('home')} vs {match.get('away')}",
            "pick": pick,
            "marketCode": match.get("marketCode") or "HAD",
            "marketName": match.get("marketName") or "胜平负",
            "goalLine": match.get("goalLine") or "",
            "sportteryPlay": item.get("sporttery_play") or sporttery_play_label(match),
            "passType": item.get("pass_type") or "单关",
            "odds": round(odds, 2),
            "edge": item.get("edge_pct", 0),
            "probability": item.get("probability", 0),
            "stake": stake,
            "potentialProfit": round(stake * (odds - 1)),
            "reason": str(item.get("reason") or "LLM 模拟下注")[:180],
            "placedAt": beijing_time(),
            "status": "待赛",
            "agent": "LLM",
        })
        existing.add(key)
        placed += 1
    for parlay in plan.get("parlays", []) if isinstance(plan.get("parlays"), list) else []:
        raw_legs = parlay.get("legs") if isinstance(parlay.get("legs"), list) else []
        legs = []
        combined_odds = 1.0
        for leg in raw_legs[:3]:
            match = match_by_id.get(str(leg.get("id")))
            pick = leg.get("pick")
            if not match or pick not in ("主胜", "平局", "客胜"):
                continue
            odds = decimal_odds_for_pick(match, pick)
            if not odds:
                continue
            combined_odds *= odds
            legs.append({
                "id": match.get("id"),
                "match": f"{match.get('home')} vs {match.get('away')}",
                "home": match.get("home"),
                "away": match.get("away"),
                "pick": pick,
                "marketCode": match.get("marketCode") or "HAD",
                "marketName": match.get("marketName") or "胜平负",
                "goalLine": match.get("goalLine") or "",
                "odds": round(odds, 2),
            })
        if len(legs) < 2:
            continue
        key = "parlay:" + "|".join(bet_leg_key(leg) for leg in legs)
        if key in existing:
            continue
        try:
            stake = min(round(float(parlay.get("stake") or 0)), int(float(account.get("cash") or 0)))
        except (TypeError, ValueError):
            stake = 0
        if stake <= 0:
            continue
        combined_odds = float(parlay.get("combined_odds") or combined_odds)
        account["cash"] = float(account.get("cash") or 0) - stake
        bets.append({
            "key": key,
            "id": key,
            "type": "parlay",
            "match": " × ".join(f"{leg.get('match')} {leg.get('pick')}" for leg in legs),
            "legs": legs,
            "pick": parlay.get("type") or f"{len(legs)}串1",
            "sportteryPlay": parlay.get("sporttery_play") or "混合过关",
            "passType": parlay.get("pass_type") or parlay.get("type") or f"{len(legs)}串1",
            "odds": round(combined_odds, 2),
            "stake": stake,
            "potentialProfit": round(stake * (combined_odds - 1)),
            "reason": str(parlay.get("reason") or "LLM 串关模拟下注")[:180],
            "placedAt": beijing_time(),
            "status": "待赛",
            "agent": "LLM",
        })
        existing.add(key)
        placed += 1
    history = state.get("history") if isinstance(state.get("history"), list) else []
    history.insert(0, {
        "time": beijing_time(),
        "mode": trigger,
        "summary": plan.get("summary") or "",
        "planMode": plan.get("mode") or "",
        "model": plan.get("model") or MODEL,
        "odds_source": plan.get("odds_source") or "",
        "placed": placed,
        "bets": [
            {
                "id": bet.get("id"),
                "pick": bet.get("pick"),
                "sporttery_play": bet.get("sporttery_play"),
                "pass_type": bet.get("pass_type"),
                "stake": bet.get("stake"),
                "edge_pct": bet.get("edge_pct"),
                "reason": bet.get("reason"),
            }
            for bet in plan.get("bets", [])[:20]
        ],
        "parlays": plan.get("parlays", [])[:10] if isinstance(plan.get("parlays"), list) else [],
        "decisions": plan.get("decisions", [])[:80] if isinstance(plan.get("decisions"), list) else [],
        "error": bool(plan.get("error")),
        "llm_debug": plan.get("llm_debug") if isinstance(plan.get("llm_debug"), dict) else {},
    })
    state["account"] = account
    state["history"] = history[:50]
    if not plan.get("error"):
        state["last_signature"] = signature
    else:
        state["last_signature"] = ""
    state["stats"] = calculate_sim_stats(account)
    state["worker"] = {
        "last_bet_at": beijing_time(),
        "last_bet_placed": placed,
        "last_bet_summary": plan.get("summary") or "",
        "last_signature": signature if not plan.get("error") else "",
    }
    save_sim_state(state)
    return {"placed": placed, "state": state}


def summarize_analysis(text: str) -> str:
    lines = [line.strip(" -•") for line in text.splitlines() if line.strip()]
    useful = [line for line in lines if any(word in line for word in ("结论", "推荐", "概率", "比分", "竞猜", "置信"))]
    return "；".join(useful[:3])[:220] or text[:220]


def extract_predicted_score(text: str) -> str:
    clean_text = re.sub(r"[*_`]+", "", text or "")
    patterns = [
        r"推荐比分\s*[：:]\s*([^\n；;，,]+)",
        r"比分\s*[：:]\s*([^\n；;，,]+)",
        r"推荐比分\s*\|\s*([^|\n]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, clean_text)
        if match:
            return match.group(1).strip().strip("*")
    return ""


def normalize_score_text(value: str) -> str:
    text = str(value or "")
    match = re.search(r"(\d{1,2})\s*[-‐‑‒–—]\s*(\d{1,2})", text)
    if match:
        return f"{int(match.group(1))}-{int(match.group(2))}"
    return ""


def pick_from_score(value: str) -> str:
    score = normalize_score_text(value)
    match = re.fullmatch(r"(\d{1,2})-(\d{1,2})", score)
    if not match:
        return ""
    home_score, away_score = int(match.group(1)), int(match.group(2))
    if home_score > away_score:
        return "主胜"
    if home_score < away_score:
        return "客胜"
    return "平局"


def extract_predicted_pick(text: str) -> str:
    patterns = [
        r"(?:主胜/平局/客胜|竞猜选项|预测方向|方向)[：:]\s*(主胜|平局|客胜)",
        r"(?:最终预测|结论|推荐)[^\n：:]*[：:]\s*(主胜|平局|客胜)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "")
        if match:
            return match.group(1)
    return ""


def strip_tags(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def fetch_json_cached(url: str, ttl_seconds: int = 1800) -> dict:
    now = datetime.now(timezone.utc).timestamp()
    cached = DATA_CACHE.get(url)
    if cached and now - cached["ts"] < ttl_seconds:
        return cached["data"]
    req = request.Request(url, headers={"User-Agent": "worldcup-predictions/1.0"})
    with request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    DATA_CACHE[url] = {"ts": now, "data": data}
    return data


def fetch_json_request_cached(url: str, headers: dict | None = None, ttl_seconds: int = 900, timeout_seconds: int = 25, proxy_url: str = "", force_refresh: bool = False) -> dict:
    cache_id = json.dumps({"url": url, "headers": sorted((headers or {}).items()), "proxy": proxy_url}, sort_keys=True)
    now = datetime.now(timezone.utc).timestamp()
    cached = DATA_CACHE.get(cache_id)
    if not force_refresh and cached and now - cached["ts"] < ttl_seconds:
        return cached["data"]
    req = request.Request(url, headers=headers or {"User-Agent": "worldcup-predictions/1.0"})
    opener = request.build_opener(request.ProxyHandler({"http": proxy_url, "https": proxy_url})) if proxy_url else request.build_opener()
    with opener.open(req, timeout=timeout_seconds) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    DATA_CACHE[cache_id] = {"ts": now, "data": data}
    return data


def unwrap_proxy_json(data: dict) -> dict:
    if isinstance(data, dict) and isinstance(data.get("contents"), str):
        try:
            return json.loads(data["contents"])
        except json.JSONDecodeError:
            return data
    if isinstance(data, dict) and isinstance(data.get("body"), str):
        try:
            return json.loads(data["body"])
        except json.JSONDecodeError:
            return data
    return data


def fetch_text_cached(url: str, ttl_seconds: int = 86400) -> str:
    now = datetime.now(timezone.utc).timestamp()
    cached = DATA_CACHE.get(url)
    if cached and now - cached["ts"] < ttl_seconds:
        return cached["data"]
    req = request.Request(url, headers={"User-Agent": "worldcup-predictions/1.0"})
    with request.urlopen(req, timeout=25) as resp:
        data = resp.read().decode("utf-8", errors="ignore")
    DATA_CACHE[url] = {"ts": now, "data": data}
    return data


def normalize_sporttery_decimal(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "--", "null", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def decimal_to_american(decimal_odds: float | None) -> int | None:
    if not decimal_odds or decimal_odds <= 1:
        return None
    if decimal_odds >= 2:
        return round((decimal_odds - 1) * 100)
    return round(-100 / (decimal_odds - 1))


def probs_from_decimal(decimal_odds: list[float]) -> list[int]:
    implied = [1 / value for value in decimal_odds if value and value > 1]
    if len(implied) != 3:
        return [34, 33, 33]
    total = sum(implied) or 1
    return [round((value / total) * 100) for value in implied]


def draw_assessment_from_probs(probs: list[int], confidence: str = "") -> dict:
    values = [(int(value) if isinstance(value, int) else round(float(value or 0))) for value in (probs or [34, 33, 33])[:3]]
    while len(values) < 3:
        values.append(0)
    side_top = max(values[0], values[2])
    side_gap = abs(values[0] - values[2])
    draw_gap = side_top - values[1]
    draw_leads = values[1] >= side_top
    close_enough = values[1] >= 29 and draw_gap <= 6 and side_top < 43
    balanced_low_score = confidence == "中" and values[1] >= 28.5 and side_top <= 46.5 and draw_gap <= 13.5
    recommend_draw = draw_leads or balanced_low_score or (side_gap <= 8 and close_enough)
    signals = []
    if draw_leads:
        signals.append("平局概率不低于胜负两端")
    if close_enough:
        signals.append("平局概率贴近最高胜率且热门胜率不高")
    if balanced_low_score:
        signals.append("低置信度接近五五开且平局概率达到触发线")
    if not signals:
        signals.append("平局可作为风险项复核，但未触发优先推荐")
    return {
        "type": "draw_assessment",
        "recommend_draw": recommend_draw,
        "probs": values,
        "draw_gap_to_best_side": round(draw_gap, 1),
        "side_gap": round(side_gap, 1),
        "signals": signals,
    }


def should_pick_draw(probs: list[int], confidence: str = "") -> bool:
    return bool(draw_assessment_from_probs(probs, confidence).get("recommend_draw"))


def pick_from_probs(home: str, away: str, probs: list[int]) -> tuple[str, str]:
    if should_pick_draw(probs, conf_from_prob(max(probs))):
        return "平局", "平局"
    idx = probs.index(max(probs))
    if idx == 0:
        return home, "主胜"
    if idx == 1:
        return "平局", "平局"
    return away, "客胜"


def conf_from_prob(prob: int) -> str:
    if prob >= 75:
        return "极高"
    if prob >= 63:
        return "高"
    if prob >= 55:
        return "中高"
    return "中"


def score_from_market(api_pick: str, probs: list[int]) -> str:
    values = probs if isinstance(probs, list) and len(probs) == 3 else [34, 33, 33]
    fav = max(values)
    gap = fav - min(values)
    draw = values[1] or 0
    open_game = draw < 24
    if api_pick == "平局":
        if draw >= 28.5:
            return "1-1"
        return "2-2" if open_game else "0-0"
    home_win = api_pick == "主胜"
    winner = 2
    loser = 1
    opponent_prob = values[2] if home_win else values[0]
    if fav >= 78 or gap >= 65:
        winner = 4 if open_game else 3
        loser = 1 if opponent_prob >= 16 else 0
    elif fav >= 68 or gap >= 48:
        winner = 3
        loser = 1 if opponent_prob >= 18 else 0
    elif opponent_prob <= 18:
        winner = 2
        loser = 0
    elif fav >= 58 or gap >= 32:
        winner = 3 if open_game else 2
        loser = 1
    elif draw >= 30:
        winner = 1
        loser = 0
    return f"{winner}-{loser}" if home_win else f"{loser}-{winner}"


def cn_date(date_text: str) -> str:
    matched = re.search(r"^(\d{4})-(\d{2})-(\d{2})", str(date_text or ""))
    if not matched:
        return str(date_text or "日期待定")
    return f"{int(matched.group(2))}月{int(matched.group(3))}日"


def cn_datetime(date_text: str) -> str:
    matched = re.search(r"^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}:\d{2}))?", str(date_text or ""))
    if not matched:
        return str(date_text or "时间待定")
    return f"{matched.group(1)}-{matched.group(2)}-{matched.group(3)} {matched.group(4) or '时间待定'}"


def sporttery_item_to_match(item: dict, idx: int) -> dict:
    decimal = item.get("decimal") or []
    probs = probs_from_decimal(decimal)
    pick, api_pick = pick_from_probs(item.get("home", ""), item.get("away", ""), probs)
    market_code = str(item.get("market_code") or "HAD").upper()
    goal_line = str(item.get("goal_line") or "").strip()
    market_name = "让球胜平负" if market_code == "HHAD" else "胜平负"
    market_suffix = f"({goal_line})" if goal_line else ""
    odds_source = f"中国体育彩票竞彩网{market_name}固定奖金"
    return {
        "id": f"tc-{idx + 1}",
        "d": cn_date(item.get("match_time")),
        "time": cn_datetime(item.get("match_time")),
        "matchNo": item.get("match_no") or "",
        "matchId": item.get("match_id") or "",
        "home": item.get("home"),
        "away": item.get("away"),
        "venue": f"{item.get('league') or '世界杯'} · 中国体育彩票",
        "odds": item.get("american") or [decimal_to_american(value) for value in decimal],
        "oddsDecimal": decimal,
        "oddsSource": odds_source,
        "marketCode": market_code,
        "marketName": market_name,
        "goalLine": goal_line,
        "supportsSingle": item.get("supports_single", True),
        "supportsAllUp": item.get("supports_all_up", True),
        "poolStatus": item.get("pool_status") or "",
        "businessDate": item.get("business_date") or "",
        "oddsUpdateTime": item.get("update_time") or "",
        "pick": pick,
        "api": api_pick,
        "score": score_from_market(api_pick, probs),
        "conf": conf_from_prob(max(probs)),
        "probs": probs,
        "why": f"以中国体育彩票竞彩网当前{market_name}{market_suffix}固定奖金为主数据源。{api_pick}对应的隐含概率最高；仍需结合阵容、伤病、裁判和临场新闻复核。",
    }


def sporttery_snapshot_to_matches(snapshot: dict) -> list[dict]:
    return [sporttery_item_to_match(item, idx) for idx, item in enumerate(snapshot.get("matches", [])[:40])]


def sporttery_play_label(match: dict) -> str:
    market_name = match.get("marketName") or ("让球胜平负" if str(match.get("marketCode") or "").upper() == "HHAD" else "胜平负")
    goal_line = str(match.get("goalLine") or "").strip()
    return f"{market_name}({goal_line})" if goal_line else str(market_name)


def odds_signature(matches: list[dict]) -> str:
    body = "|".join(f"{match.get('id')}:{match.get('home')}-{match.get('away')}:{'/'.join(str(value) for value in (match.get('odds') or []))}" for match in matches)
    return f"{AUTO_BET_STRATEGY_VERSION}|{body}"


def walk_json(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_json(child)


def sporttery_pick(row: dict, keys: tuple[str, ...]):
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def sporttery_pool_candidates(row: dict) -> list[tuple[dict, str]]:
    candidates: list[tuple[dict, str]] = []
    odds_list = row.get("oddsList")
    if isinstance(odds_list, list):
        candidates.extend((item, str(item.get("poolCode") or item.get("pool_code") or "").upper()) for item in odds_list if isinstance(item, dict))
    for key in ("had", "HAD", "spf", "hhad", "HHAD", "odds"):
        value = row.get(key)
        inferred = "HHAD" if key.lower() == "hhad" else "HAD" if key.lower() in {"had", "spf"} else ""
        if isinstance(value, list):
            candidates.extend((item, inferred or str(item.get("poolCode") or item.get("pool_code") or "").upper()) for item in value if isinstance(item, dict))
        elif isinstance(value, dict):
            candidates.append((value, inferred or str(value.get("poolCode") or value.get("pool_code") or "").upper()))
    if not candidates:
        candidates.append((row, str(row.get("poolCode") or row.get("pool_code") or "").upper()))
    return candidates


def sporttery_pool_choices(row: dict) -> list[tuple[dict, str, str]]:
    choices = []
    seen = set()
    candidates = sporttery_pool_candidates(row)
    for wanted in ("HAD", "HHAD"):
        for item, inferred in candidates:
            code = str(item.get("poolCode") or item.get("pool_code") or inferred or "").upper()
            if code == wanted or (wanted == "HAD" and not code):
                goal_line = str(item.get("goalLine") or item.get("goal_line") or "").strip()
                win = normalize_sporttery_decimal(sporttery_pick(item, ("h", "home", "win", "had_h", "h_sp", "a")))
                draw = normalize_sporttery_decimal(sporttery_pick(item, ("d", "draw", "had_d", "d_sp", "b")))
                lose = normalize_sporttery_decimal(sporttery_pick(item, ("a", "away", "lose", "had_a", "a_sp", "c")))
                key = (wanted, goal_line, win, draw, lose)
                if key not in seen:
                    seen.add(key)
                    choices.append((item, wanted, goal_line))
                break
    return choices


def choose_sporttery_pool(row: dict) -> tuple[dict, str, str]:
    choices = sporttery_pool_choices(row)
    return choices[0] if choices else ({}, "", "")


def sporttery_pool_meta(row: dict, market_code: str) -> dict:
    pool_list = row.get("poolList")
    if not isinstance(pool_list, list):
        return {}
    wanted = str(market_code or "").upper()
    for item in pool_list:
        if not isinstance(item, dict):
            continue
        if str(item.get("poolCode") or "").upper() == wanted:
            return item
    return {}


def sporttery_bool(value, default: bool = True) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "n"}
    return bool(value)


def parse_sporttery_matches(data: dict) -> list[dict]:
    matches = []
    for row in walk_json(data):
        home = sporttery_pick(row, ("homeTeamAllName", "homeTeamName", "homeTeam", "homeTeamAbbName", "hostName", "homeName", "h_cn", "home_team"))
        away = sporttery_pick(row, ("awayTeamAllName", "awayTeamName", "awayTeam", "awayTeamAbbName", "guestName", "awayName", "a_cn", "away_team"))
        if not home or not away:
            continue
        for had, market_code, goal_line in sporttery_pool_choices(row):
            win = normalize_sporttery_decimal(sporttery_pick(had, ("h", "home", "win", "had_h", "h_sp", "a")))
            draw = normalize_sporttery_decimal(sporttery_pick(had, ("d", "draw", "had_d", "d_sp", "b")))
            lose = normalize_sporttery_decimal(sporttery_pick(had, ("a", "away", "lose", "had_a", "a_sp", "c")))
            if not all([win, draw, lose]):
                continue
            pool_meta = sporttery_pool_meta(row, market_code or "HAD")
            matches.append({
                "match_id": sporttery_pick(row, ("matchId", "id")),
                "home": str(home),
                "away": str(away),
                "league": sporttery_pick(row, ("leagueName", "leagueAbbName", "leagueAllName", "league", "l_cn")),
                "match_no": sporttery_pick(row, ("matchNumStr", "matchNum", "matchNo", "num", "issueNum")),
                "match_date": sporttery_pick(row, ("matchDate", "businessDate", "date")),
                "match_clock": sporttery_pick(row, ("matchTime", "time")),
                "business_date": sporttery_pick(row, ("businessDate", "matchDate", "date")),
                "match_status": sporttery_pick(row, ("matchStatus", "status")),
                "match_time": " ".join(str(part) for part in (
                    sporttery_pick(row, ("matchDate", "businessDate", "date")),
                    sporttery_pick(row, ("matchTime", "time")),
                ) if part),
                "market_code": market_code or "HAD",
                "market_name": "让球胜平负" if market_code == "HHAD" else "胜平负",
                "goal_line": goal_line,
                "pool_status": pool_meta.get("poolStatus") or sporttery_pick(had, ("poolStatus", "status")),
                "supports_single": sporttery_bool(pool_meta.get("bettingSingle", pool_meta.get("single", 1))),
                "supports_all_up": sporttery_bool(pool_meta.get("bettingAllup", pool_meta.get("allUp", 1))),
                "update_time": " ".join(str(part) for part in (
                    sporttery_pick(had, ("updateDate",)),
                    sporttery_pick(had, ("updateTime",)),
                ) if part),
                "decimal": [win, draw, lose],
                "american": [decimal_to_american(win), decimal_to_american(draw), decimal_to_american(lose)],
                "source": "中国体育彩票竞彩网固定奖金",
            })
    return matches


def build_sporttery_urls() -> list[str]:
    urls = []
    for raw_url in [SPORTTERY_PROXY_URL, *SPORTTERY_PROXY_URLS, SPORTTERY_OFFICIAL_URL]:
        if not raw_url:
            continue
        if "{url}" in raw_url:
            raw_url = raw_url.replace("{url}", parse.quote(SPORTTERY_OFFICIAL_URL, safe=""))
        if raw_url not in urls:
            urls.append(raw_url)
    return urls


def build_sporttery_calculator_urls() -> list[str]:
    urls = []
    for raw_url in [SPORTTERY_PROXY_URL, *SPORTTERY_PROXY_URLS, SPORTTERY_CALCULATOR_URL]:
        if not raw_url:
            continue
        if "{url}" in raw_url:
            raw_url = raw_url.replace("{url}", parse.quote(SPORTTERY_CALCULATOR_URL, safe=""))
        if raw_url not in urls:
            urls.append(raw_url)
    return urls


def sporttery_fetch_payload() -> tuple[dict | None, str, str]:
    errors = []
    for url in build_sporttery_urls():
        try:
            use_proxy = bool(SPORTTERY_HTTP_PROXY) and url == SPORTTERY_OFFICIAL_URL
            data = fetch_json_request_cached(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "Referer": "https://www.sporttery.cn/",
                "Origin": "https://www.sporttery.cn",
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }, ttl_seconds=60, timeout_seconds=8, proxy_url=SPORTTERY_HTTP_PROXY if use_proxy else "")
            return unwrap_proxy_json(data), url, ""
        except Exception as exc:
            errors.append(f"{url}: {str(exc)[:180]}")
    source_url = build_sporttery_urls()[0] if build_sporttery_urls() else SPORTTERY_OFFICIAL_URL
    return None, source_url, " | ".join(errors)


def sporttery_calculator_fetch_payload(force_refresh: bool = False) -> tuple[dict | None, str, str]:
    errors = []
    for url in build_sporttery_calculator_urls():
        try:
            use_proxy = bool(SPORTTERY_HTTP_PROXY) and url == SPORTTERY_CALCULATOR_URL
            data = fetch_json_request_cached(url, headers={
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
                "Referer": "https://m.sporttery.cn/mjc/jsq/zqspf/",
                "Origin": "https://m.sporttery.cn",
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }, ttl_seconds=15, timeout_seconds=8, proxy_url=SPORTTERY_HTTP_PROXY if use_proxy else "", force_refresh=force_refresh)
            return unwrap_proxy_json(data), url, ""
        except Exception as exc:
            errors.append(f"{url}: {str(exc)[:180]}")
    source_url = build_sporttery_calculator_urls()[0] if build_sporttery_calculator_urls() else SPORTTERY_CALCULATOR_URL
    return None, source_url, " | ".join(errors)


def _build_sporttery_odds_snapshot() -> dict:
    data, url, err = sporttery_fetch_payload()
    if data is None:
        return {
            "source": "中国体育彩票竞彩网",
            "source_url": url,
            "status": "blocked_or_unavailable",
            "note": "已接入免费体彩网关，但当前服务器访问被站点安全策略拦截或网络不可用。可配置 SPORTTERY_PROXY_URL 或 SPORTTERY_PROXY_URLS；支持直接 JSON，也支持 AllOrigins contents 包装格式和 {url} 模板。",
            "error": err[:300],
            "matches": [],
            "generated_at": beijing_time(),
            "official_url": SPORTTERY_OFFICIAL_URL,
        }
    parsed_matches = parse_sporttery_matches(data)
    finished_results = fetch_worldcup_results()
    matches = []
    finished_count = 0
    started_count = 0
    for item in parsed_matches:
        finished = match_result_for_teams(item.get("home", ""), item.get("away", ""), finished_results)
        if finished:
            finished_count += 1
            continue
        if match_has_started(item):
            started_count += 1
            continue
        matches.append(item)
    if matches:
        save_odds_snapshots(matches, "中国体育彩票竞彩网")
    trends = get_odds_trends(matches)
    for item in matches:
        item["trend"] = trends.get(odds_match_key(item), [])
    return {
        "source": "中国体育彩票竞彩网",
        "source_url": url,
        "status": "ok" if matches else "no_matches_parsed",
        "note": "固定奖金为十进制赔率，前端会换算成美式赔率用于原模拟盘。已完赛场次会自动移出待预测列表。",
        "matches": matches[:200],
        "raw_count": len(parsed_matches),
        "filtered_finished_count": finished_count,
        "filtered_started_count": started_count,
        "generated_at": beijing_time(),
        "official_url": SPORTTERY_OFFICIAL_URL,
    }


def _build_sporttery_calculator_snapshot(force_refresh: bool = False) -> dict:
    data, url, err = sporttery_calculator_fetch_payload(force_refresh=force_refresh)
    value = data.get("value") if isinstance(data, dict) else {}
    official_last_update = value.get("lastUpdateTime") if isinstance(value, dict) else ""
    if data is None:
        return {
            "source": "中国体育彩票竞彩网足球胜平负计算器",
            "source_url": url,
            "status": "blocked_or_unavailable",
            "note": "已接入官方足球胜平负计算器接口，但当前服务器访问被站点安全策略拦截或网络不可用。",
            "error": err[:300],
            "matches": [],
            "generated_at": beijing_time(),
            "official_page": "https://m.sporttery.cn/mjc/jsq/zqspf/",
            "official_url": SPORTTERY_CALCULATOR_URL,
            "official_last_update": "",
            "real_time": force_refresh,
        }
    parsed_matches = parse_sporttery_matches(data)
    finished_results = fetch_worldcup_results()
    matches = []
    finished_count = 0
    started_count = 0
    for item in parsed_matches:
        finished = match_result_for_teams(item.get("home", ""), item.get("away", ""), finished_results)
        if finished:
            finished_count += 1
            continue
        if match_has_started(item):
            started_count += 1
            continue
        matches.append(item)
    return {
        "source": "中国体育彩票竞彩网足球胜平负计算器",
        "source_url": url,
        "status": "ok" if matches else "no_matches_parsed",
        "note": "数据采集自官方移动端足球胜平负计算器接口，固定奖金为十进制赔率。",
        "matches": matches[:200],
        "raw_count": len(parsed_matches),
        "filtered_finished_count": finished_count,
        "filtered_started_count": started_count,
        "generated_at": beijing_time(),
        "official_page": "https://m.sporttery.cn/mjc/jsq/zqspf/",
        "official_url": SPORTTERY_CALCULATOR_URL,
        "official_last_update": official_last_update,
        "real_time": force_refresh,
    }


def cached_snapshot(name: str, builder, max_age_seconds: int) -> dict:
    now = time.monotonic()
    with SNAPSHOT_LOCK:
        entry = SNAPSHOT_CACHE[name]
        payload = entry.get("payload")
        if payload is not None:
            result = dict(payload)
            age = now - float(entry.get("ts") or 0)
            result["cache_age_seconds"] = round(age, 1)
            result["cache_stale"] = age > max_age_seconds
            if entry.get("error"):
                result["cache_error"] = entry["error"]
            return result
    try:
        payload = builder()
        with SNAPSHOT_LOCK:
            SNAPSHOT_CACHE[name] = {"payload": payload, "ts": time.monotonic(), "error": ""}
        result = dict(payload)
        result["cache_age_seconds"] = 0
        result["cache_stale"] = False
        return result
    except Exception as exc:
        with SNAPSHOT_LOCK:
            entry = SNAPSHOT_CACHE[name]
            entry["error"] = str(exc)[:240]
            payload = entry.get("payload")
        if payload is not None:
            result = dict(payload)
            result["cache_stale"] = True
            result["cache_error"] = str(exc)[:240]
            return result
        raise


def refresh_snapshot(name: str, builder) -> None:
    try:
        payload = builder()
        with SNAPSHOT_LOCK:
            SNAPSHOT_CACHE[name] = {"payload": payload, "ts": time.monotonic(), "error": ""}
    except Exception as exc:
        with SNAPSHOT_LOCK:
            SNAPSHOT_CACHE[name]["error"] = str(exc)[:240]
        print(f"[snapshot-worker] {beijing_time()} {name} error={exc}", flush=True)


def sporttery_odds_snapshot() -> dict:
    return cached_snapshot("odds", _build_sporttery_odds_snapshot, ODDS_REFRESH_SECONDS * 2)


def sporttery_calculator_snapshot(force_refresh: bool = False) -> dict:
    max_age_seconds = 15
    now = time.monotonic()
    if not force_refresh:
        with SNAPSHOT_LOCK:
            entry = SNAPSHOT_CACHE["calculator"]
            payload = entry.get("payload")
            age = now - float(entry.get("ts") or 0)
            if payload is not None and age < max_age_seconds:
                result = dict(payload)
                result["cache_age_seconds"] = round(age, 1)
                result["cache_stale"] = False
                if entry.get("error"):
                    result["cache_error"] = entry["error"]
                return result
    try:
        payload = _build_sporttery_calculator_snapshot(force_refresh=force_refresh)
        with SNAPSHOT_LOCK:
            SNAPSHOT_CACHE["calculator"] = {"payload": payload, "ts": time.monotonic(), "error": ""}
        result = dict(payload)
        result["cache_age_seconds"] = 0
        result["cache_stale"] = False
        return result
    except Exception as exc:
        with SNAPSHOT_LOCK:
            entry = SNAPSHOT_CACHE["calculator"]
            entry["error"] = str(exc)[:240]
            payload = entry.get("payload")
            age = time.monotonic() - float(entry.get("ts") or 0)
        if payload is not None:
            result = dict(payload)
            result["cache_age_seconds"] = round(age, 1)
            result["cache_stale"] = True
            result["cache_error"] = str(exc)[:240]
            return result
        raise


def sporttery_tool(home_cn: str, away_cn: str) -> list[dict]:
    snapshot = sporttery_odds_snapshot()
    items = []
    for item in snapshot.get("matches", []):
        text = f"{item.get('home', '')} {item.get('away', '')}"
        if home_cn in text or away_cn in text:
            items.append(item)
    return [{
        "type": "sporttery_fixed_bonus_snapshot",
        "source": "中国体育彩票竞彩网",
        "status": snapshot.get("status"),
        "source_url": snapshot.get("source_url"),
        "note": snapshot.get("note"),
        "items": items[:5],
        "error": snapshot.get("error"),
    }]


def sporttery_item_matches_teams(item: dict, home_cn: str, away_cn: str) -> bool:
    home_key = normalize_team_text(home_cn)
    away_key = normalize_team_text(away_cn)
    item_home = normalize_team_text(item.get("home", ""))
    item_away = normalize_team_text(item.get("away", ""))
    if item_home == home_key and item_away == away_key:
        return True
    item_text = normalize_team_text(f"{item.get('home', '')}{item.get('away', '')}")
    return bool(home_key and away_key and home_key in item_text and away_key in item_text)


def compact_sporttery_calculator_item(item: dict) -> dict:
    decimal = item.get("decimal") or []
    return {
        "match_id": item.get("match_id"),
        "match_no": item.get("match_no"),
        "home": item.get("home"),
        "away": item.get("away"),
        "league": item.get("league"),
        "match_time": item.get("match_time"),
        "market_code": item.get("market_code"),
        "market_name": item.get("market_name"),
        "goal_line": item.get("goal_line"),
        "pool_status": item.get("pool_status"),
        "supports_single": item.get("supports_single"),
        "supports_all_up": item.get("supports_all_up"),
        "decimal": decimal,
        "implied_probabilities": probs_from_decimal(decimal),
        "update_time": item.get("update_time"),
        "source": item.get("source"),
    }


def sporttery_calculator_tool(home_cn: str, away_cn: str) -> list[dict]:
    snapshot = sporttery_calculator_snapshot(force_refresh=True)
    items = [
        compact_sporttery_calculator_item(item)
        for item in snapshot.get("matches", [])
        if sporttery_item_matches_teams(item, home_cn, away_cn)
    ]
    return [{
        "type": "sporttery_calculator_snapshot",
        "source": "中国体育彩票竞彩网足球胜平负计算器",
        "status": snapshot.get("status"),
        "source_url": snapshot.get("source_url"),
        "official_page": snapshot.get("official_page"),
        "official_url": snapshot.get("official_url"),
        "official_last_update": snapshot.get("official_last_update"),
        "generated_at": snapshot.get("generated_at"),
        "cache_age_seconds": snapshot.get("cache_age_seconds"),
        "note": "这是官方移动端足球胜平负计算器数据。用于判断市场方向、固定奖金和投注可用性；不是赛果事实。",
        "items": items[:4],
        "matched_count": len(items),
        "raw_count": snapshot.get("raw_count"),
        "filtered_finished_count": snapshot.get("filtered_finished_count"),
        "error": snapshot.get("error") or snapshot.get("cache_error"),
    }]


def free_search(query: str, limit: int = 3) -> list[dict]:
    """Use DuckDuckGo (free, no key needed) to search the web."""
    if not HAS_DDGS:
        return []
    try:
        items = DDGS().text(query, max_results=limit)
    except Exception:
        return []
    results = []
    for item in items:
        results.append({
            "query": query,
            "title": strip_tags(str(item.get("title", ""))),
            "url": str(item.get("href", "")),
            "snippet": strip_tags(str(item.get("body", ""))),
        })
    return results


def search_tool(query: str, limit: int = 3) -> list[dict]:
    if SEARCH_API_URL:
        url = SEARCH_API_URL + ("&" if "?" in SEARCH_API_URL else "?") + parse.urlencode({"q": query, "limit": limit})
        req = request.Request(url, headers={"User-Agent": "worldcup-predictions/1.0"})
        try:
            with request.urlopen(req, timeout=12) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            return [{"query": query, "error": str(exc)}]
        results = []
        raw_items = data.get("results") or data.get("data") or []
        for item in raw_items[:limit]:
            results.append({
                "query": query,
                "title": strip_tags(str(item.get("title", ""))),
                "url": str(item.get("url", "")),
                "snippet": strip_tags(str(item.get("snippet") or item.get("content") or "")),
            })
        return results or [{"query": query, "error": "no results"}]

    results = free_search(query, limit)
    if results:
        return results
    return [{
        "query": query,
        "status": "search_unavailable",
        "note": "免费搜索实例暂时不可用，LLM 只能基于已知数据推断。",
    }]


def worldcup26_tool(home: str, away: str) -> list[dict]:
    results: list[dict] = []
    try:
        teams_data = fetch_json_cached("https://worldcup26.ir/get/teams")
        games_data = fetch_json_cached("https://worldcup26.ir/get/games")
        groups_data = fetch_json_cached("https://worldcup26.ir/get/groups")
    except Exception as exc:
        return [{"type": "worldcup26_api_error", "error": str(exc)}]

    teams = teams_data.get("teams", [])
    by_id = {str(t.get("id")): t for t in teams}
    by_name = {t.get("name_en"): t for t in teams}
    home_team = by_name.get(home)
    away_team = by_name.get(away)

    for game in games_data.get("games", []):
        names = {game.get("home_team_name_en"), game.get("away_team_name_en")}
        if home in names and away in names:
            results.append({
                "type": "2026_fixture_result",
                "source": "worldcup26.ir",
                "group": game.get("group"),
                "matchday": game.get("matchday"),
                "local_date": game.get("local_date"),
                "home": game.get("home_team_name_en"),
                "away": game.get("away_team_name_en"),
                "score": f"{game.get('home_score')}-{game.get('away_score')}",
                "finished": game.get("finished"),
                "status": game.get("time_elapsed"),
            })

    wanted_groups = {t.get("groups") for t in (home_team, away_team) if t}
    for group in groups_data.get("groups", []):
        if group.get("name") not in wanted_groups:
            continue
        standings = []
        for row in group.get("teams", []):
            team = by_id.get(str(row.get("team_id")), {})
            standings.append({
                "team": team.get("name_en", row.get("team_id")),
                "mp": row.get("mp"),
                "w": row.get("w"),
                "d": row.get("d"),
                "l": row.get("l"),
                "pts": row.get("pts"),
                "gf": row.get("gf"),
                "ga": row.get("ga"),
                "gd": row.get("gd"),
            })
        results.append({
            "type": "2026_group_standings",
            "source": "worldcup26.ir",
            "group": group.get("name"),
            "standings": standings,
        })
    return results


def openfootball_schedule_tool(home: str, away: str) -> list[dict]:
    try:
        data = fetch_json_cached("https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.json")
    except Exception as exc:
        return [{"type": "openfootball_schedule_error", "error": str(exc)}]
    results = []
    for match in data.get("matches", []):
        names = {match.get("team1"), match.get("team2")}
        if home in names and away in names:
            results.append({
                "type": "2026_fixture_source",
                "source": "openfootball/worldcup.json",
                "round": match.get("round"),
                "date": match.get("date"),
                "time": match.get("time"),
                "home": match.get("team1"),
                "away": match.get("team2"),
                "group": match.get("group"),
                "ground": match.get("ground"),
            })
    return results


def historical_results_tool(home: str, away: str, limit: int = 8) -> list[dict]:
    url = "https://raw.githubusercontent.com/martj42/international_results/master/results.csv"
    try:
        csv_text = fetch_text_cached(url)
    except Exception as exc:
        return [{"type": "historical_results_error", "error": str(exc)}]
    rows = []
    for line in csv_text.splitlines()[1:]:
        parts = line.split(",")
        if len(parts) < 9:
            continue
        date, home_team, away_team, home_score, away_score, tournament, city, country, neutral = parts[:9]
        if {home_team, away_team} == {home, away}:
            rows.append({
                "type": "head_to_head_result",
                "source": "martj42/international_results",
                "date": date,
                "home": home_team,
                "away": away_team,
                "score": f"{home_score}-{away_score}",
                "tournament": tournament,
                "city": city,
                "country": country,
                "neutral": neutral,
            })
    recent_home = []
    recent_away = []
    for line in reversed(csv_text.splitlines()[1:]):
        parts = line.split(",")
        if len(parts) < 9:
            continue
        date, home_team, away_team, home_score, away_score, tournament, city, country, neutral = parts[:9]
        item = {
            "type": "recent_team_result",
            "source": "martj42/international_results",
            "date": date,
            "home": home_team,
            "away": away_team,
            "score": f"{home_score}-{away_score}",
            "tournament": tournament,
        }
        if len(recent_home) < 5 and home in (home_team, away_team):
            recent_home.append(item)
        if len(recent_away) < 5 and away in (home_team, away_team):
            recent_away.append(item)
        if len(recent_home) >= 5 and len(recent_away) >= 5:
            break
    return rows[-limit:] + recent_home + recent_away


def thesportsdb_tool(home: str, away: str) -> list[dict]:
    key = THESPORTSDB_API_KEY or "3"
    results = []
    endpoints = [
        ("team_home", f"{THESPORTSDB_BASE_URL}/{key}/searchteams.php?{parse.urlencode({'t': home})}"),
        ("team_away", f"{THESPORTSDB_BASE_URL}/{key}/searchteams.php?{parse.urlencode({'t': away})}"),
        ("event_search", f"{THESPORTSDB_BASE_URL}/{key}/searchevents.php?{parse.urlencode({'e': f'{home}_vs_{away}'})}"),
    ]
    for label, url in endpoints:
        try:
            data = fetch_json_request_cached(url, ttl_seconds=1800)
            results.append({
                "type": f"thesportsdb_{label}",
                "source": "TheSportsDB free API",
                "items": (data.get("teams") or data.get("event") or data.get("events") or [])[:3],
            })
        except Exception as exc:
            results.append({"type": f"thesportsdb_{label}_error", "source": "TheSportsDB free API", "error": str(exc)})
    return results


def football_data_tool(home: str, away: str) -> list[dict]:
    if not FOOTBALL_DATA_TOKEN:
        return [{
            "type": "football_data_not_configured",
            "source": "football-data.org free tier",
            "note": "Set FOOTBALL_DATA_TOKEN to fetch free-tier fixtures, scores and standings where competition coverage is available.",
        }]
    headers = {"X-Auth-Token": FOOTBALL_DATA_TOKEN, "User-Agent": "worldcup-predictions/1.0"}
    results = []
    endpoints = [
        ("competitions", f"{FOOTBALL_DATA_BASE_URL}/competitions"),
        ("matches", f"{FOOTBALL_DATA_BASE_URL}/matches"),
    ]
    for label, url in endpoints:
        try:
            data = fetch_json_request_cached(url, headers=headers, ttl_seconds=900)
            results.append({
                "type": f"football_data_{label}",
                "source": "football-data.org free tier",
                "items": data.get("competitions", data.get("matches", []))[:5],
            })
        except Exception as exc:
            results.append({"type": f"football_data_{label}_error", "source": "football-data.org free tier", "error": str(exc)})
    return results


def api_football_tool(home: str, away: str) -> list[dict]:
    if not APIFOOTBALL_API_KEY:
        return [{
            "type": "api_football_free_tier_not_configured",
            "source": "API-Football free tier",
            "note": "Set APIFOOTBALL_API_KEY to use the free tier for fixtures, standings, lineups, injuries, predictions and odds where available.",
        }]
    headers = {"x-apisports-key": APIFOOTBALL_API_KEY}
    results = []
    endpoints = [
        ("fixtures_search", f"{APIFOOTBALL_BASE_URL}/fixtures?season=2026&search={parse.quote(home)}"),
        ("injuries_home", f"{APIFOOTBALL_BASE_URL}/injuries?season=2026&team={parse.quote(home)}"),
        ("injuries_away", f"{APIFOOTBALL_BASE_URL}/injuries?season=2026&team={parse.quote(away)}"),
        ("odds", f"{APIFOOTBALL_BASE_URL}/odds?season=2026&search={parse.quote(home + ' ' + away)}"),
    ]
    for label, url in endpoints:
        try:
            data = fetch_json_request_cached(url, headers=headers, ttl_seconds=900)
            results.append({
                "type": f"api_football_{label}",
                "source": "API-Football free tier",
                "response_count": data.get("results"),
                "items": (data.get("response") or [])[:3],
            })
        except Exception as exc:
            results.append({"type": f"api_football_{label}_error", "source": "API-Football free tier", "error": str(exc)})
    return results


def odds_api_tool(home: str, away: str) -> list[dict]:
    if not ODDS_API_KEY:
        return [{
            "type": "odds_api_not_configured",
            "source": "The Odds API",
            "note": "Set ODDS_API_KEY to fetch bookmaker odds and market movement snapshots.",
        }]
    # Soccer sport keys vary by provider availability; this endpoint is intentionally configurable.
    sport_key = os.getenv("ODDS_API_SPORT", "soccer_fifa_world_cup")
    query = parse.urlencode({
        "apiKey": ODDS_API_KEY,
        "regions": os.getenv("ODDS_API_REGIONS", "us,eu,uk"),
        "markets": os.getenv("ODDS_API_MARKETS", "h2h"),
        "oddsFormat": os.getenv("ODDS_API_FORMAT", "american"),
    })
    url = f"{ODDS_API_BASE_URL}/sports/{sport_key}/odds?{query}"
    try:
        data = fetch_json_request_cached(url, ttl_seconds=600)
    except Exception as exc:
        return [{"type": "odds_api_error", "source": "The Odds API", "error": str(exc)}]
    matches = []
    for item in data if isinstance(data, list) else []:
        teams = {item.get("home_team"), item.get("away_team")}
        if home in teams or away in teams or home in str(item) or away in str(item):
            matches.append(item)
    return [{
        "type": "odds_api_market_snapshot",
        "source": "The Odds API",
        "sport_key": sport_key,
        "home": home,
        "away": away,
        "items": matches[:3],
        "raw_count": len(data) if isinstance(data, list) else None,
    }]


def collect_match_intelligence(match: dict) -> list[dict]:
    home_cn = match.get("home", "")
    away_cn = match.get("away", "")
    home = TEAM_EN.get(match.get("home", ""), match.get("home", ""))
    away = TEAM_EN.get(match.get("away", ""), match.get("away", ""))
    collected = [
        draw_assessment_from_probs(match.get("probs") or [], match.get("conf") or ""),
        {
            "type": "official_source",
            "title": "FIFA official match schedule, fixtures and results",
            "url": "https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/match-schedule-fixtures-results-teams-stadiums",
            "snippet": "Use for official fixture, result, venue and tournament context checks.",
        },
        {
            "type": "official_source",
            "title": "FIFA men's world ranking",
            "url": "https://inside.fifa.com/fifa-world-ranking/men",
            "snippet": "Use for team strength baseline and ranking comparison.",
        },
        {
            "type": "schedule_market_source",
            "title": "ESPN FIFA World Cup schedule",
            "url": "https://www.espn.com/soccer/schedule/_/league/fifa.world",
            "snippet": "Use for schedule display, venue/time checks and market-line snapshot when available.",
        },
    ]
    collected.extend(worldcup26_tool(home, away))
    collected.extend(openfootball_schedule_tool(home, away))
    collected.extend(historical_results_tool(home, away))
    collected.extend(thesportsdb_tool(home, away))
    collected.extend(football_data_tool(home, away))
    collected.extend(api_football_tool(home, away))
    collected.extend(odds_api_tool(home, away))
    collected.extend(sporttery_tool(home_cn, away_cn))
    collected.extend(sporttery_calculator_tool(home_cn, away_cn))
    queries = [
        f"{home} {away} 2026 World Cup fixture group standings result",
        f"{home} national team official squad injuries 2026 World Cup",
        f"{away} national team official squad injuries 2026 World Cup",
        f"{home} {away} recent form FIFA ranking 2026",
        f"{home} {away} locker room negative news injury report",
        f"{home} {away} referee assignment strictness cards penalties odds movement news",
    ]
    seen = {item.get("url") or json.dumps(item, ensure_ascii=False, sort_keys=True) for item in collected}
    for query in queries:
        for item in search_tool(query, limit=2):
            key = item.get("url") or item.get("error") or item.get("title")
            if key in seen:
                continue
            seen.add(key)
            collected.append(item)
    return collected[:36]


def json_response(handler: SimpleHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def parse_beijing_datetime(value: str) -> datetime | None:
    matched = re.search(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", str(value or ""))
    if not matched:
        return None
    year, month, day, hour, minute = map(int, matched.groups())
    return datetime(year, month, day, hour, minute, tzinfo=BEIJING_TZ)


def latest_market_snapshot_before_kickoff(home: str, away: str) -> dict | None:
    fixture_key = strict_fixture_key(home, away)
    if not fixture_key:
        return None
    home_names = list(dict.fromkeys(team_name_candidates(home) or [home]))
    away_names = list(dict.fromkeys(team_name_candidates(away) or [away]))
    home_marks = ",".join("?" for _ in home_names)
    away_marks = ",".join("?" for _ in away_names)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT home, away, match_time, match_no, decimal_json, source, captured_at, captured_ts
            FROM odds_snapshots
            WHERE home IN ({home_marks}) AND away IN ({away_marks})
            ORDER BY captured_ts DESC
            LIMIT 500
            """,
            (*home_names, *away_names),
        ).fetchall()
    for row in rows:
        if strict_fixture_key(row["home"], row["away"]) != fixture_key:
            continue
        kickoff = parse_beijing_datetime(row["match_time"])
        if kickoff and float(row["captured_ts"] or 0) >= kickoff.timestamp():
            continue
        try:
            decimal = json.loads(row["decimal_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(decimal, list) or len(decimal) < 3:
            continue
        normalized_decimal = []
        for value in decimal[:3]:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                normalized_decimal = []
                break
            if numeric <= 1:
                normalized_decimal = []
                break
            normalized_decimal.append(numeric)
        if len(normalized_decimal) != 3:
            continue
        return {
            "decimal": normalized_decimal,
            "captured_at": row["captured_at"],
            "captured_ts": row["captured_ts"],
            "match_no": row["match_no"],
            "match_time": row["match_time"],
            "source": row["source"] or "中国体育彩票竞彩网",
        }
    return None


def market_prediction_backfill(home: str, away: str) -> dict | None:
    snapshot = latest_market_snapshot_before_kickoff(home, away)
    if not snapshot:
        return None
    probs = probs_from_decimal(snapshot["decimal"])
    max_idx = probs.index(max(probs))
    api_pick = ("主胜", "平局", "客胜")[max_idx]
    return {
        "predicted_score": score_from_market(api_pick, probs),
        "predicted_pick": api_pick,
        "generated_at": snapshot.get("captured_at", ""),
        "model": "sporttery-market-snapshot",
        "prompt_version": "odds-backfill-v1",
        "source": snapshot.get("source", ""),
        "source_note": "无严格同场 LLM 赛前记录，使用开赛前最后一条体彩赔率快照补录。",
        "odds_decimal": snapshot.get("decimal", []),
        "odds_probs": probs,
        "match_no": snapshot.get("match_no", ""),
    }


def _build_match_results() -> dict:
    results_raw = fetch_worldcup_results()
    if not results_raw:
        return {"results": [], "summary": "暂无已完赛数据。", "generated_at": beijing_time()}
    all_preds = get_prediction_matches(limit=5000)
    result_en_to_cn = {v: k for k, v in TEAM_EN.items()}
    pred_by_key = {}
    for p in all_preds:
        key = strict_fixture_key(p.get("home", ""), p.get("away", ""))
        if key:
            pred_by_key.setdefault(key, p)
    extra_map = {
        "south korea": "韩国", "czech republic": "捷克", "ecuador": "厄瓜多尔",
        "turkey": "土耳其", "sweden": "瑞典", "jordan": "约旦", "panama": "巴拿马",
        "iraq": "伊拉克", "bosnia and herzegovina": "波黑", "curaçao": "库拉索",
        "democratic republic of the congo": "刚果金", "dr congo": "刚果金",
    }
    for en, cn in extra_map.items():
        result_en_to_cn[en] = cn
    EN_TO_CN_LOWER = {k.lower(): v for k, v in result_en_to_cn.items()}
    items = []
    seen_result_fixtures = set()
    for key, r in results_raw.items():
        home_en, away_en = r["home"], r["away"]
        fixture_key = strict_fixture_key(home_en, away_en)
        if fixture_key in seen_result_fixtures:
            continue
        seen_result_fixtures.add(fixture_key)
        home_cn = EN_TO_CN_LOWER.get(home_en.lower(), home_en)
        away_cn = EN_TO_CN_LOWER.get(away_en.lower(), away_en)
        pred_match = pred_by_key.get(strict_fixture_key(home_cn, away_cn))
        predicted_score = ""
        predicted_pick = ""
        actual_score = f"{r['home_score']}-{r['away_score']}"
        pick_hit = None
        score_hit = None
        if pred_match:
            predicted_score = normalize_score_text(pred_match.get("predicted_score", ""))
            predicted_pick = pick_from_score(predicted_score) or pred_match.get("predicted_pick") or ""
            if not predicted_pick:
                predicted_pick = extract_predicted_pick(pred_match.get("summary", "").strip())
            if predicted_pick:
                pick_hit = (predicted_pick == r["pick"])
            if predicted_score:
                score_hit = (predicted_score == actual_score)
        else:
            pred_match = market_prediction_backfill(home_cn, away_cn)
            if pred_match:
                predicted_score = pred_match.get("predicted_score", "")
                predicted_pick = pred_match.get("predicted_pick", "")
                if predicted_pick:
                    pick_hit = (predicted_pick == r["pick"])
                if predicted_score:
                    score_hit = (predicted_score == actual_score)
        items.append({
            "home": home_cn,
            "away": away_cn,
            "home_en": home_en,
            "away_en": away_en,
            "actual_score": actual_score,
            "actual_pick": r["pick"],
            "predicted_score": predicted_score,
            "predicted_pick": predicted_pick,
            "prediction_generated_at": pred_match.get("generated_at", "") if pred_match else "",
            "prediction_model": pred_match.get("model", "") if pred_match else "",
            "prediction_prompt_version": pred_match.get("prompt_version", "") if pred_match else "",
            "prediction_source_note": pred_match.get("source_note", "") if pred_match else "",
            "prediction_odds_decimal": pred_match.get("odds_decimal", []) if pred_match else [],
            "prediction_odds_probs": pred_match.get("odds_probs", []) if pred_match else [],
            "pick_hit": pick_hit,
            "score_hit": score_hit,
            "source": r.get("source", ""),
        })
    total_pred = sum(1 for i in items if i["predicted_score"])
    hits = sum(1 for i in items if i["score_hit"] is True)
    misses = sum(1 for i in items if i["score_hit"] is False)
    direction_hits = sum(1 for i in items if i["pick_hit"] is True)
    direction_misses = sum(1 for i in items if i["pick_hit"] is False)
    return {
        "results": items,
        "total_finished": len(items),
        "total_predicted": total_pred,
        "hits": hits,
        "misses": misses,
        "direction_hits": direction_hits,
        "direction_misses": direction_misses,
        "direction_total": direction_hits + direction_misses,
        "direction_accuracy": round(direction_hits / (direction_hits + direction_misses) * 100, 1) if (direction_hits + direction_misses) else None,
        "accuracy": round(hits / total_pred * 100, 1) if total_pred else None,
        "generated_at": beijing_time(),
    }


def get_match_results() -> dict:
    return cached_snapshot("results", _build_match_results, RESULTS_REFRESH_SECONDS * 2)


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path: str) -> str:
        raw_path = path.split("?", 1)[0].split("#", 1)[0]
        rel = parse.unquote(raw_path).lstrip("/") or "index.html"
        target = (ROOT / rel).resolve()
        try:
            target.relative_to(ROOT)
        except ValueError:
            return str(ROOT / "__not_found__")
        return str(target)

    def do_GET(self) -> None:
        parsed = parse.urlparse(self.path)
        if parsed.path == "/api/odds":
            json_response(self, 200, sporttery_odds_snapshot())
            return
        if parsed.path == "/api/sporttery-calculator":
            params = parse.parse_qs(parsed.query)
            force_refresh = params.get("refresh", ["0"])[0].lower() in {"1", "true", "yes"}
            json_response(self, 200, sporttery_calculator_snapshot(force_refresh=force_refresh))
            return
        if parsed.path == "/api/history":
            params = parse.parse_qs(parsed.query)
            match_id = params.get("match_id", [""])[0] or None
            try:
                limit = int(params.get("limit", ["50"])[0])
            except ValueError:
                limit = 50
            limit = max(1, min(limit, 100))
            json_response(self, 200, {"history": get_history(match_id=match_id, limit=limit)})
            return
        if parsed.path == "/api/sim/account":
            params = parse.parse_qs(parsed.query)
            auto_settle = params.get("settle", ["0"])[0] == "1"
            json_response(self, 200, get_sim_snapshot(params.get("account_id", ["global"])[0], auto_settle=auto_settle))
            return
        if parsed.path == "/api/sim/settle":
            json_response(self, 405, {"error": "请使用 POST 触发结算。"})
            return
        if parsed.path == "/api/sim/worker/run":
            if not is_local_request(self):
                json_response(self, 403, {"error": "后台 worker 只能由服务器本机触发。"})
                return
            json_response(self, 200, run_auto_worker_once())
            return
        if parsed.path == "/api/results":
            json_response(self, 200, get_match_results())
            return
        if parsed.path == "/api/prediction-snapshot":
            json_response(self, 200, build_prediction_snapshot())
            return
        super().do_GET()

    def do_POST(self) -> None:
        if self.path == "/api/sim/save":
            try:
                ip = client_ip(self)
                if rate_limited(f"sim-save:{ip}", SIM_SAVE_RATE_LIMIT_COUNT, SIM_SAVE_RATE_LIMIT_SECONDS):
                    json_response(self, 429, {"error": "模拟账户保存过于频繁，请稍后再试。"})
                    return
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                json_response(self, 200, save_client_sim_state(payload))
            except Exception as exc:
                json_response(self, 500, {"error": str(exc)})
            return

        if self.path == "/api/sim/settle":
            try:
                ip = client_ip(self)
                if rate_limited(f"sim-settle:{ip}", 1, SIM_SETTLE_RATE_LIMIT_SECONDS):
                    json_response(self, 429, {"error": "结算请求过于频繁，请稍后再试。"})
                    return
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}") if length else {}
                account_id = payload.get("account_id") or "global"
                json_response(self, 200, settle_sim_account(account_id))
            except Exception as exc:
                json_response(self, 500, {"error": str(exc)})
            return

        if self.path not in ("/api/predict", "/api/predictions", "/api/auto-bet"):
            json_response(self, 404, {"error": "接口不存在"})
            return

        key = os.getenv("OPENAI_API_KEY")
        if not key:
            json_response(self, 503, {"error": "服务器未配置 OPENAI_API_KEY，无法调用 LLM。"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if self.path == "/api/auto-bet":
                if not payload.get("manual"):
                    json_response(self, 429, {"error": "前端自动下注已关闭；请使用手动按钮或等待后台定时 worker。"})
                    return
                ip = client_ip(self)
                if rate_limited(f"auto-bet:{ip}", 1, AUTO_BET_RATE_LIMIT_SECONDS):
                    json_response(self, 429, {"error": f"LLM 自动下注请求过于频繁，请稍后再试。"})
                    return
                result = get_auto_bet_plan(key, payload)
                json_response(self, 200, result)
                return
            if self.path == "/api/predict":
                if not payload.get("manual"):
                    json_response(self, 429, {"error": "前端自动 AI 预测已关闭；请手动点击单场分析。"})
                    return
                ip = client_ip(self)
                if rate_limited(f"predict:{ip}", PREDICT_RATE_LIMIT_COUNT, PREDICT_RATE_LIMIT_SECONDS):
                    json_response(self, 429, {"error": "AI 预测请求过于频繁，请稍后再试。"})
                    return
                result = get_prediction(key, payload.get("match") or {})
                json_response(self, 200, result)
                return

            json_response(self, 410, {"error": "批量 AI 预测接口已停用，请逐场手动触发。"})
        except Exception as exc:
            json_response(self, 500, {"error": str(exc)})


def extract_json_object(text: str) -> dict:
    text = text.strip()
    tagged = re.search(r"<json>\s*(.*?)\s*</json>", text, re.S | re.I)
    if tagged:
        text = tagged.group(1).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start:end + 1])
        raise


def auto_plan_debug(plan: object, text: str = "") -> dict:
    keys = sorted(plan.keys()) if isinstance(plan, dict) else []
    return {
        "plan_type": type(plan).__name__,
        "plan_keys": keys[:30],
        "raw_preview": strip_tags(str(text or ""))[:1200],
    }


def normalize_auto_pick(value: object) -> str:
    raw = str(value or "").strip()
    lowered = raw.lower()
    mapping = {
        "主胜": "主胜", "胜": "主胜", "home": "主胜", "home_win": "主胜", "home win": "主胜",
        "平局": "平局", "平": "平局", "draw": "平局", "tie": "平局",
        "客胜": "客胜", "负": "客胜", "away": "客胜", "away_win": "客胜", "away win": "客胜",
        "跳过": "跳过", "skip": "跳过", "pass": "跳过", "no bet": "跳过", "不下注": "跳过",
    }
    return mapping.get(raw) or mapping.get(lowered) or "跳过"


def first_present(data: dict, keys: list[str], default=None):
    for key in keys:
        if key in data and data.get(key) not in (None, ""):
            return data.get(key)
    return default


def normalize_auto_plan_items(plan: object) -> dict:
    if not isinstance(plan, dict):
        return {"summary": "", "mode": "", "decisions": [], "bets": [], "parlays": []}
    decisions = first_present(plan, ["decisions", "decision", "matches", "recommendations", "picks", "选择"], [])
    bets = first_present(plan, ["bets", "bet", "single_bets", "singles", "wagers", "下注"], [])
    parlays = first_present(plan, ["parlays", "parlay_bets", "combo_bets", "accumulators", "串关"], [])
    if isinstance(decisions, dict):
        decisions = list(decisions.values())
    if isinstance(bets, dict):
        bets = list(bets.values())
    if isinstance(parlays, dict):
        parlays = list(parlays.values())
    return {
        "summary": str(first_present(plan, ["summary", "策略", "总结"], "") or ""),
        "mode": str(first_present(plan, ["mode", "planMode", "模式"], "") or ""),
        "decisions": decisions if isinstance(decisions, list) else [],
        "bets": bets if isinstance(bets, list) else [],
        "parlays": parlays if isinstance(parlays, list) else [],
    }


def call_llm_json(api_key: str, prompt: str, max_tokens: int = 2200) -> tuple[dict, str]:
    body = {
        "model": MODEL,
        "messages": [
            {
                "role": "system",
                "content": "你是结构化输出代理。必须把唯一 JSON 放在 <json> 和 </json> 标签之间，不要输出标签外文本。",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    req = request.Request(
        f"{BASE_URL}/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    text = data["choices"][0]["message"]["content"].strip()
    try:
        return extract_json_object(text), text
    except Exception:
        repair_prompt = f"""下面这段文本本应包含一个 JSON 对象。请只输出修复后的 JSON，并放在 <json></json> 中。

要求：
- 顶层对象只能包含 summary 和 bets。
- bets 必须是数组。
- 不要添加解释。

原始文本：
{text[:6000]}
"""
        repair_body = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": "只输出 <json>...</json>，其中必须是合法 JSON。"},
                {"role": "user", "content": repair_prompt},
            ],
            "temperature": 0,
            "max_tokens": max_tokens,
        }
        repair_req = request.Request(
            f"{BASE_URL}/v1/chat/completions",
            data=json.dumps(repair_body).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(repair_req, timeout=45) as resp:
            repair_data = json.loads(resp.read().decode("utf-8"))
        repaired = repair_data["choices"][0]["message"]["content"].strip()
        return extract_json_object(repaired), repaired


def get_auto_bet_plan(api_key: str, payload: dict) -> dict:
    incoming_matches = payload.get("matches") or []
    odds_source = "页面待赛赔率"
    if payload.get("prefer_calculator", True):
        calculator_snapshot = sporttery_calculator_snapshot(force_refresh=True)
        if calculator_snapshot.get("status") == "ok":
            incoming_matches = sporttery_snapshot_to_matches(calculator_snapshot)
            odds_source = calculator_snapshot.get("source") or "体彩计算器"
    finished_results = fetch_worldcup_results()
    matches = [
        match for match in incoming_matches
        if not match_result_for_teams(match.get("home", ""), match.get("away", ""), finished_results)
    ]
    settings = payload.get("settings") or {}
    bankroll = float(settings.get("cash") or settings.get("bankroll") or 0)
    max_stake_pct = float(settings.get("maxStakePct") or 5)
    min_edge_pct = float(settings.get("minEdgePct") or 2)
    compact = []
    for match in matches[:40]:
        kickoff_status = kickoff_gate(match)
        compact.append({
            "id": match.get("id"),
            "date": match.get("d"),
            "kickoff": match.get("time") or match.get("d"),
            "betting_allowed": kickoff_status["allowed"],
            "time_note": kickoff_status["reason"],
            "match": f"{match.get('home')} vs {match.get('away')}",
            "home": match.get("home"),
            "away": match.get("away"),
            "odds_decimal": match.get("oddsDecimal") or [],
            "odds_american": match.get("odds") or [],
            "page_probabilities": match.get("probs") or [],
            "page_pick": match.get("api"),
            "confidence": match.get("conf"),
            "source": match.get("oddsSource"),
            "market_code": match.get("marketCode"),
            "market_name": match.get("marketName"),
            "goal_line": match.get("goalLine"),
            "sporttery_play": sporttery_play_label(match),
            "supports_single": match.get("supportsSingle"),
            "supports_all_up": match.get("supportsAllUp"),
            "pool_status": match.get("poolStatus"),
            "odds_update_time": match.get("oddsUpdateTime"),
        })
    prompt = f"""你是 2026 世界杯模拟盘自动下注代理。

目标：基于 worldcup-2026-predictor skill 的思路，结合体彩计算器固定奖金、页面概率、球队实力推断、新闻/裁判/阵容缺失风险，给出模拟下注计划。这里是研究型模拟盘，不是真实资金，不要因为信息不完整就全部跳过。

硬性限制：
- 这是模拟盘，不是真实下注。
- 不得编造伤病、裁判、新闻、大名单；缺失信息视为风险，降低下注金额。
- 只允许选择 主胜、平局、客胜 或 跳过。
- 你自己选择下注模式：单关、2串1、3串1、混合、全部跳过。
- 只能下注 betting_allowed=true 的比赛；已开赛或开赛前30分钟内必须跳过。
- 串关最多3场，不能包含 betting_allowed=false 的比赛。
- 同一个串关里不能放同一场比赛的不同玩法；同一场最多出现一次。
- 单场下注不能超过可用资金的 {max_stake_pct}%。
- 体彩固定奖金天然含水位，页面概率多为盘口隐含概率归一化；不要机械要求每场都有严格数学正期望。
- 如果比赛列表包含 market_code、goal_line、supports_single、supports_all_up、odds_update_time，必须用于判断投注模式和盘口方向。
- 优先选择 3-6 场强信号小仓试单，除非整批比赛确实没有任何清晰倾向。
- 强信号定义：热门方胜率/实力层级明显、赔率没有严重过热、或弱势方向有清晰背离价值。
- 信息缺失时降低仓位，不要直接跳过所有比赛。
- 单场常规下注建议为可用资金的 0.5%-2.0%；极强信号最多到 {max_stake_pct}%。
- 总下注金额建议控制在可用资金的 8%-18%，最高不超过 35%。
- 输出必须把唯一 JSON 放在 <json> 和 </json> 标签之间。
- 标签外不要写任何文字。
- JSON 里不要出现注释、尾随逗号、NaN、Infinity。

可用资金：{bankroll}
设置：{json.dumps(settings, ensure_ascii=False)}
赔率数据源：{odds_source}
比赛列表：{json.dumps(compact, ensure_ascii=False)}

输出格式：
<json>
{{
  "summary": "一句话说明整体策略",
  "mode": "单关|2串1|3串1|混合|全部跳过",
  "decisions": [
    {{
      "id": "比赛id",
      "pick": "主胜|平局|客胜|跳过",
      "sporttery_play": "胜平负|让球胜平负(-1)|混合过关|跳过",
      "pass_type": "单关|2串1|3串1|无",
      "probability": 0.62,
      "edge_pct": 4.2,
      "stake": 120,
      "risk": "低|中|高",
      "reason": "20-80字中文理由"
    }}
  ],
  "bets": [
    {{
      "id": "比赛id",
      "pick": "主胜|平局|客胜|跳过",
      "sporttery_play": "胜平负|让球胜平负(-1)",
      "pass_type": "单关",
      "probability": 0.62,
      "edge_pct": 4.2,
      "stake": 120,
      "reason": "20-50字中文理由"
    }}
  ],
  "parlays": [
    {{
      "type": "2串1|3串1",
      "sporttery_play": "混合过关|胜平负过关|让球胜平负过关",
      "pass_type": "2串1|3串1",
      "legs": [
        {{"id": "比赛id", "pick": "主胜|平局|客胜"}}
      ],
      "stake": 80,
      "reason": "20-80字中文理由"
    }}
  ]
}}
</json>
"""
    text = ""
    try:
        plan, text = call_llm_json(api_key, prompt, max_tokens=2200)
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        summary = f"LLM API 错误 {exc.code}: {detail[:180]}"
        return {
            "summary": summary,
            "mode": "全部跳过",
            "decisions": error_decisions(matches, summary),
            "bets": [],
            "parlays": [],
            "model": MODEL,
            "generated_at": beijing_time(),
            "error": True,
            "llm_debug": {"raw_preview": detail[:1200]},
        }
    except Exception as exc:
        summary = f"LLM 自动下注计划解析失败：{exc}"
        return {
            "summary": summary,
            "mode": "全部跳过",
            "decisions": error_decisions(matches, summary),
            "bets": [],
            "parlays": [],
            "model": MODEL,
            "generated_at": beijing_time(),
            "error": True,
            "llm_debug": {"raw_preview": str(exc)[:1200]},
        }

    normalized_plan = normalize_auto_plan_items(plan)
    if not normalized_plan["decisions"] and not normalized_plan["bets"] and not normalized_plan["parlays"]:
        retry_prompt = prompt + f"""

上一次你返回的是空计划，无法用于模拟盘：
{json.dumps(plan, ensure_ascii=False)[:1200]}

请重新输出。必须满足：
- decisions 必须覆盖所有比赛 id，不能省略。
- 至少给出 3 场非“跳过”的小仓单关，除非所有 betting_allowed 都是 false。
- bets 必须包含这些非跳过单关，stake 为 50 到 {round(bankroll * max_stake_pct / 100)} 之间的整数。
- 仍然只输出 <json>...</json>。
"""
        try:
            plan, text = call_llm_json(api_key, retry_prompt, max_tokens=2600)
        except Exception:
            pass

    debug = auto_plan_debug(plan, text)
    normalized_plan = normalize_auto_plan_items(plan)
    match_ids = {str(match.get("id")) for match in matches}
    match_by_id = {str(match.get("id")): match for match in matches}
    decisions_by_id = {}
    raw_valid_decisions = 0
    raw_decisions = normalized_plan["decisions"]
    for item in raw_decisions:
        if not isinstance(item, dict):
            continue
        mid = str(first_present(item, ["id", "match_id", "matchId", "比赛id", "场次"], ""))
        if mid not in match_ids:
            continue
        pick = normalize_auto_pick(first_present(item, ["pick", "selection", "choice", "result", "预测", "选择"], "跳过"))
        raw_valid_decisions += 1
        decisions_by_id[mid] = {
            "id": mid,
            "pick": pick,
            "sporttery_play": str(first_present(item, ["sporttery_play", "play", "market", "market_name", "玩法"], sporttery_play_label(match_by_id.get(mid, {}))) or ""),
            "pass_type": str(first_present(item, ["pass_type", "passType", "bet_type", "过关方式"], "单关" if pick != "跳过" else "无") or ""),
            "probability": first_present(item, ["probability", "prob", "confidence", "胜率"], 0),
            "edge_pct": first_present(item, ["edge_pct", "edge", "value", "优势"], 0),
            "stake": first_present(item, ["stake", "amount", "bet", "下注金额"], 0) if pick != "跳过" else 0,
            "risk": first_present(item, ["risk", "风险"], "中"),
            "reason": str(first_present(item, ["reason", "rationale", "analysis", "理由"], "LLM未给出详细理由"))[:160],
        }
    for match in matches:
        mid = str(match.get("id"))
        if mid not in decisions_by_id:
            decisions_by_id[mid] = {
                "id": match.get("id"),
                "pick": "跳过",
                "sporttery_play": sporttery_play_label(match),
                "pass_type": "无",
                "probability": 0,
                "edge_pct": 0,
                "stake": 0,
                "risk": "中",
                "reason": "LLM未返回该场决策，按未下注处理。",
            }

    safe_bets = []
    remaining = bankroll
    max_single = bankroll * max_stake_pct / 100
    raw_bets = normalized_plan["bets"]
    if not raw_bets:
        raw_bets = [item for item in decisions_by_id.values() if item.get("pick") != "跳过" and item.get("stake")]
    for item in raw_bets:
        if not isinstance(item, dict):
            continue
        mid = str(first_present(item, ["id", "match_id", "matchId", "比赛id", "场次"], ""))
        if mid not in match_ids:
            continue
        if not kickoff_gate(match_by_id[mid])["allowed"]:
            continue
        pick = normalize_auto_pick(first_present(item, ["pick", "selection", "choice", "result", "预测", "选择"], "跳过"))
        if pick == "跳过":
            continue
        try:
            stake = max(0, min(float(first_present(item, ["stake", "amount", "bet", "下注金额"], 0) or 0), max_single, remaining))
        except (TypeError, ValueError):
            stake = 0
        if stake <= 0:
            continue
        remaining -= stake
        item["stake"] = round(stake)
        item["id"] = mid
        item["pick"] = pick
        item["sporttery_play"] = str(first_present(item, ["sporttery_play", "play", "market", "market_name", "玩法"], sporttery_play_label(match_by_id[mid])) or "")
        item["pass_type"] = str(first_present(item, ["pass_type", "passType", "bet_type", "过关方式"], "单关") or "单关")
        safe_bets.append(item)
        if mid in decisions_by_id:
            decisions_by_id[mid]["pick"] = pick
            decisions_by_id[mid]["stake"] = round(stake)
            decisions_by_id[mid]["sporttery_play"] = item["sporttery_play"]
            decisions_by_id[mid]["pass_type"] = item["pass_type"]
    safe_parlays = []
    for parlay in normalized_plan["parlays"]:
        if not isinstance(parlay, dict):
            continue
        legs = []
        combined_odds = 1.0
        raw_legs = first_present(parlay, ["legs", "matches", "legs_list", "串关场次"], [])
        if not isinstance(raw_legs, list):
            raw_legs = []
        for leg in raw_legs[:3]:
            if not isinstance(leg, dict):
                continue
            mid = str(first_present(leg, ["id", "match_id", "matchId", "比赛id", "场次"], ""))
            pick = normalize_auto_pick(first_present(leg, ["pick", "selection", "choice", "result", "预测", "选择"], ""))
            if mid not in match_by_id or pick not in ("主胜", "平局", "客胜"):
                continue
            if not kickoff_gate(match_by_id[mid])["allowed"]:
                continue
            odds = decimal_odds_for_pick(match_by_id[mid], pick)
            if not odds:
                continue
            fixture_key = f"{normalize_team_text(match_by_id[mid].get('home', ''))}-{normalize_team_text(match_by_id[mid].get('away', ''))}"
            if any(item.get("fixture_key") == fixture_key for item in legs):
                continue
            combined_odds *= odds
            legs.append({"id": match_by_id[mid].get("id"), "pick": pick, "odds": round(odds, 2), "fixture_key": fixture_key})
        if len(legs) not in (2, 3):
            continue
        try:
            stake = max(0, min(float(parlay.get("stake") or 0), max_single, remaining))
        except (TypeError, ValueError):
            stake = 0
        if stake <= 0:
            continue
        remaining -= stake
        play_names = [sporttery_play_label(match_by_id[str(leg["id"])]) for leg in legs if str(leg.get("id")) in match_by_id]
        unique_plays = {name for name in play_names if name}
        sporttery_play = str(parlay.get("sporttery_play") or ("混合过关" if len(unique_plays) > 1 else f"{next(iter(unique_plays), '胜平负')}过关"))
        safe_parlays.append({
            "type": f"{len(legs)}串1",
            "sporttery_play": sporttery_play,
            "pass_type": f"{len(legs)}串1",
            "legs": [{key: value for key, value in leg.items() if key != "fixture_key"} for leg in legs],
            "combined_odds": round(combined_odds, 2),
            "stake": round(stake),
            "reason": str(parlay.get("reason") or "LLM 串关模拟下注")[:160],
        })
    valid_non_skip_decisions = sum(1 for item in decisions_by_id.values() if item.get("pick") != "跳过")
    if not raw_valid_decisions and not safe_bets and not safe_parlays:
        summary = "LLM 返回了空计划或非预期结构，未识别到有效比赛决策；保留赔率签名以便下次重试。"
        return {
            "summary": summary,
            "mode": normalized_plan["mode"] or "解析失败",
            "decisions": error_decisions(matches, summary),
            "bets": [],
            "parlays": [],
            "model": MODEL,
            "generated_at": beijing_time(),
            "error": True,
            "llm_debug": debug,
        }
    return {
        "summary": normalized_plan["summary"] or "LLM 已生成模拟下注计划。",
        "mode": normalized_plan["mode"] or "单关",
        "odds_source": odds_source,
        "source_matches": matches[:80],
        "decisions": list(decisions_by_id.values()),
        "bets": safe_bets,
        "parlays": safe_parlays,
        "model": MODEL,
        "generated_at": beijing_time(),
        "llm_debug": {
            **debug,
            "raw_valid_decisions": raw_valid_decisions,
            "valid_non_skip_decisions": valid_non_skip_decisions,
            "valid_bets": len(safe_bets),
            "valid_parlays": len(safe_parlays),
        },
    }


def parse_beijing_kickoff(match: dict) -> datetime | None:
    value = str(match.get("time") or match.get("match_time") or "")
    parsed = re.search(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", value)
    if parsed:
        year, month, day, hour, minute = map(int, parsed.groups())
        return datetime(year, month, day, hour, minute, tzinfo=BEIJING_TZ)
    date_value = str(match.get("d") or match.get("date") or "")
    date_match = re.search(r"(\d{1,2})月(\d{1,2})日", date_value)
    time_match = re.search(r"(\d{1,2}):(\d{2})", value)
    if not date_match or not time_match:
        return None
    year = datetime.now(BEIJING_TZ).year
    month, day = map(int, date_match.groups())
    hour, minute = map(int, time_match.groups())
    return datetime(year, month, day, hour, minute, tzinfo=BEIJING_TZ)


def kickoff_gate(match: dict) -> dict:
    kickoff = parse_beijing_kickoff(match)
    if not kickoff:
        return {"allowed": True, "reason": "未能解析开赛时间，按可下注处理但需复核。"}
    now = datetime.now(BEIJING_TZ)
    cutoff = kickoff - timedelta(minutes=30)
    if now >= kickoff:
        return {"allowed": False, "reason": "比赛已开赛，禁止下注。"}
    if now >= cutoff:
        return {"allowed": False, "reason": "距离开赛不足30分钟，禁止下注。"}
    return {"allowed": True, "reason": f"距离开赛约{int((kickoff - now).total_seconds() // 60)}分钟。"}


def match_has_started(match: dict) -> bool:
    kickoff = parse_beijing_kickoff(match)
    return bool(kickoff and datetime.now(BEIJING_TZ) >= kickoff)


def decimal_odds_for_pick(match: dict, pick: str) -> float | None:
    idx = {"主胜": 0, "平局": 1, "客胜": 2}.get(pick)
    if idx is None:
        return None
    decimal = match.get("oddsDecimal") or []
    if len(decimal) > idx:
        try:
            return float(decimal[idx])
        except (TypeError, ValueError):
            return None
    american = match.get("odds") or []
    if len(american) <= idx:
        return None
    try:
        odds = float(american[idx])
    except (TypeError, ValueError):
        return None
    return 1 + (100 / abs(odds)) if odds < 0 else 1 + (odds / 100)


def error_decisions(matches: list[dict], reason: str) -> list[dict]:
    return [
        {
            "id": match.get("id"),
            "pick": "跳过",
            "probability": 0,
            "edge_pct": 0,
            "stake": 0,
            "risk": "高",
            "reason": f"{reason[:120]}；未生成LLM下注。",
        }
        for match in matches
    ]


def call_openai(api_key: str, match: dict, intelligence: list[dict]) -> str:
    user_prompt = f"""直接给这场 2026 世界杯比赛的预测分析。不要写开场白，不要说“我将按照规则分析”，第一行就进入结论。

比赛快照：
{json.dumps(match, ensure_ascii=False, indent=2)}

后端在线情报工具返回的搜索摘要和来源：
{json.dumps(intelligence, ensure_ascii=False, indent=2)}

{RULES}

请按以下结构输出，语气像给懂球但不想看废话的人读：

1. 结论
- 主胜/平局/客胜：
- 推荐比分：
- 胜平负概率：
- 竞猜选项：
- 置信度：
- 平局触发判断：是否触发；若触发但最终不选平局，说明原因。

2. 关键依据
- 小组赛赛程/已结束赛果/小组排名：优先使用在线情报工具里的可靠来源；没有可靠结果就标注需补充。
- 双方官方大名单与球员状态：说明大名单、年龄身高、伤病、更衣室/负面消息、当家球星状态是否已知；在线情报没有给出就不要编造。
- 双方综合实力与整体状态：基于快照和通用实力层级做推断，并标明这是推断。
- 对手信息如何影响本场：写出 2-3 个具体对位因素。
- 淘汰赛首轮路径与名次动机：如果当前排名/出线形势未知，明确写无法判断；如果可推断动机，也必须说明依据。
- 综合结论：说明是否调整静态卡片预测。

3. 双轨背离
- 基本面轨道50%：球队实力、球员状态、赛程场地、晋级动机。
- 市场/舆情轨道35%：赔率/盘口方向、新闻导向、公众热度或异常信号。
- 裁判与比赛控制15%：裁判执法严谨度、牌点倾向、犯规尺度、VAR/点球风险；若当前快照未提供裁判信息，必须标注需赛前联网补充。
- 双轨背离判断：基本面轨道与市场/舆情轨道是同向、轻微背离还是强背离？背离时最终预测如何修正？

4. 临场复核清单
- 列出 3-6 个赛前必须联网确认的信息，必须包含裁判信息、赔率变化、新闻导向。

限制：不要编造当前伤病、最终名单、已结束赛果、小组排名、球员年龄身高或新闻；缺失就明确写“当前快照未提供，需赛前联网补充”。禁止输出“好的，我将按照...”这类套话。"""
    body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 1800,
    }
    req = request.Request(
        f"{BASE_URL}/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM API 错误 {exc.code}: {detail[:500]}") from exc

    try:
        text = data["choices"][0]["message"]["content"].strip()
        if not text:
            raise RuntimeError("LLM API 返回了空文本。")
        return text
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("LLM API 没有返回 chat 文本。") from exc


def run_auto_worker_once() -> dict:
    account_id = AUTO_WORKER_ACCOUNT_ID
    state = get_sim_state(account_id)
    snapshot = sporttery_calculator_snapshot(force_refresh=True)
    if snapshot.get("status") != "ok":
        snapshot = sporttery_odds_snapshot()
    matches = sporttery_snapshot_to_matches(snapshot) if snapshot.get("status") == "ok" else []
    signature = odds_signature(matches) if matches else ""
    settle_result = settle_sim_account(account_id)
    placed = 0
    bet_summary = ""
    bet_error = ""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if matches and signature and signature != state.get("last_signature") and api_key:
        worker = state.get("worker") if isinstance(state.get("worker"), dict) else {}
        last_error_ts = float(worker.get("last_error_ts") or 0)
        if signature == worker.get("last_error_signature") and time.time() - last_error_ts < AUTO_BET_RETRY_SECONDS:
            bet_error = "同一赔率签名的 LLM 自动下注刚失败过，等待冷却后再试。"
        else:
            refreshed_state = get_sim_state(account_id)
            account = refreshed_state.get("account") or {"initial": 10000, "cash": 10000, "bets": []}
            plan = get_auto_bet_plan(api_key, {
                "matches": matches,
                "settings": {
                    "bankroll": account.get("initial", 10000),
                    "cash": account.get("cash", 10000),
                    "riskMode": "half",
                    "maxStakePct": 5,
                    "minEdgePct": 2,
                },
            })
            with SIM_STATE_LOCK:
                latest_state = get_sim_state(account_id)
                if signature == latest_state.get("last_signature"):
                    bet_summary = "赔率签名已由其它任务处理，跳过重复下注。"
                else:
                    applied = apply_auto_bet_plan_to_state(latest_state, matches, plan, signature, "后台自动触发")
                    placed = applied["placed"]
                    bet_summary = plan.get("summary") or ""
                    if plan.get("error"):
                        bet_error = bet_summary
    elif matches and signature == state.get("last_signature"):
        bet_summary = "赔率签名未变化，跳过 LLM 自动下注。"
    elif not api_key:
        bet_error = "未配置 OPENAI_API_KEY，跳过 LLM 自动下注。"
    else:
        bet_error = f"体彩赔率不可用，状态 {snapshot.get('status')}。"
    result = {
        "time": beijing_time(),
        "odds_status": snapshot.get("status"),
        "odds_source": snapshot.get("source"),
        "odds_count": len(matches),
        "settled": settle_result.get("settled", 0),
        "reviews_added": settle_result.get("reviews_added", 0),
        "placed": placed,
        "summary": bet_summary,
        "error": bet_error,
    }
    with SIM_STATE_LOCK:
        state_after = get_sim_state(account_id)
        worker_log = state_after.get("worker_log") if isinstance(state_after.get("worker_log"), list) else []
        worker_log.insert(0, result)
        state_after["worker_log"] = worker_log[:50]
        state_after["worker"] = {
            **(state_after.get("worker") if isinstance(state_after.get("worker"), dict) else {}),
            "last_run_at": result["time"],
            "last_odds_status": result["odds_status"],
            "last_odds_source": result["odds_source"],
            "last_odds_count": result["odds_count"],
            "last_settled": result["settled"],
            "last_reviews_added": result["reviews_added"],
            "last_placed": result["placed"],
            "last_error": result["error"],
            "last_error_signature": signature if result["error"] else "",
            "last_error_ts": time.time() if result["error"] else 0,
        }
        save_sim_state(state_after)
    print(
        f"[auto-worker] {result['time']} odds={result['odds_status']} count={result['odds_count']} "
        f"placed={result['placed']} settled={result['settled']} reviews={result['reviews_added']} "
        f"error={result['error'] or '-'}",
        flush=True,
    )
    return result


def auto_worker_loop() -> None:
    while True:
        try:
            run_auto_worker_once()
        except Exception as exc:
            print(f"[auto-worker] {beijing_time()} error={exc}", flush=True)
        time.sleep(AUTO_WORKER_INTERVAL_SECONDS)


def snapshot_worker_loop() -> None:
    next_odds = 0.0
    next_results = 0.0
    while True:
        now = time.monotonic()
        if now >= next_results:
            refresh_snapshot("results", _build_match_results)
            next_results = now + RESULTS_REFRESH_SECONDS
        if now >= next_odds:
            refresh_snapshot("odds", _build_sporttery_odds_snapshot)
            next_odds = now + ODDS_REFRESH_SECONDS
        time.sleep(5)


def start_snapshot_worker() -> None:
    global SNAPSHOT_WORKER_STARTED
    if SNAPSHOT_WORKER_STARTED:
        return
    SNAPSHOT_WORKER_STARTED = True
    thread = threading.Thread(target=snapshot_worker_loop, name="snapshot-worker", daemon=True)
    thread.start()


def start_auto_worker() -> None:
    global AUTO_WORKER_STARTED
    if AUTO_WORKER_STARTED or not AUTO_WORKER_ENABLED:
        return
    AUTO_WORKER_STARTED = True
    thread = threading.Thread(target=auto_worker_loop, name="auto-worker", daemon=True)
    thread.start()


def main() -> None:
    init_db()
    start_snapshot_worker()
    start_auto_worker()
    port = int(os.getenv("PORT", "8765"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Serving on http://0.0.0.0:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
