"""
audit_logger.py — Slack 에이전트 앱 감사 로그 + 보안 모니터링
GS Retail AI Bot 대상, 다른 앱에도 재사용 가능

사용법 (app.py에서):
    from audit_logger import audit_middleware, log_llm_call, log_api_call, get_audit_dashboard_blocks
    app.use(audit_middleware)
"""

import os
import re
import json
import time
import sqlite3
import logging
import threading
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger("audit_logger")

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

DB_PATH = os.environ.get("AUDIT_DB_PATH", "audit.db")
ALERT_CHANNEL = os.environ.get("AUDIT_ALERT_CHANNEL", "")  # 보안 알림 채널 ID
ADMIN_USER_ID = os.environ.get("AUDIT_ADMIN_USER_ID", "")  # 관리자 Slack ID

# 보안 임계치
RATE_LIMIT_WINDOW = 300       # 5분
RATE_LIMIT_MAX_REQUESTS = 30  # 5분 내 최대 요청 수
OFF_HOURS_START = 22          # 심야 시작 (22시)
OFF_HOURS_END = 6             # 심야 끝 (06시)
DAILY_TOKEN_LIMIT = 500000    # 일일 토큰 임계치


# ─────────────────────────────────────────────
# DB 초기화
# ─────────────────────────────────────────────

_db_lock = threading.Lock()


def _get_conn():
    """스레드별 SQLite 커넥션"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """감사 로그 테이블 생성"""
    with _db_lock:
        conn = _get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                user_id TEXT,
                channel_id TEXT,
                intent TEXT,
                details TEXT,
                severity TEXT DEFAULT 'info',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS llm_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                user_id TEXT,
                model TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                latency_ms INTEGER DEFAULT 0,
                intent TEXT,
                error TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS api_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                user_id TEXT,
                service TEXT,
                endpoint TEXT,
                status_code INTEGER,
                latency_ms INTEGER DEFAULT 0,
                error TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS security_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                user_id TEXT,
                channel_id TEXT,
                description TEXT,
                raw_data TEXT,
                acknowledged INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_events(user_id);
            CREATE INDEX IF NOT EXISTS idx_audit_type ON audit_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_llm_timestamp ON llm_calls(timestamp);
            CREATE INDEX IF NOT EXISTS idx_security_severity ON security_alerts(severity);
        """)
        conn.commit()
        conn.close()


# 모듈 로드 시 DB 자동 초기화
init_db()


# ─────────────────────────────────────────────
# 감사 이벤트 기록
# ─────────────────────────────────────────────

def _log_event(event_type, user_id=None, channel_id=None, intent=None, details=None, severity="info"):
    """감사 이벤트 DB 저장"""
    try:
        with _db_lock:
            conn = _get_conn()
            conn.execute(
                "INSERT INTO audit_events (timestamp, event_type, user_id, channel_id, intent, details, severity) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), event_type, user_id, channel_id, intent, json.dumps(details or {}, ensure_ascii=False), severity),
            )
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"감사 로그 저장 실패: {e}")


def log_llm_call(user_id=None, model=None, input_tokens=0, output_tokens=0, latency_ms=0, intent=None, error=None):
    """LLM 호출 기록"""
    total = input_tokens + output_tokens
    try:
        with _db_lock:
            conn = _get_conn()
            conn.execute(
                "INSERT INTO llm_calls (timestamp, user_id, model, input_tokens, output_tokens, total_tokens, latency_ms, intent, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), user_id, model, input_tokens, output_tokens, total, latency_ms, intent, error),
            )
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"LLM 로그 저장 실패: {e}")

    # 일일 토큰 임계치 초과 체크
    if total > 0:
        _check_daily_token_limit(user_id)


def log_api_call(user_id=None, service=None, endpoint=None, status_code=None, latency_ms=0, error=None):
    """외부 API 호출 기록 (Jira, GitHub, Google Maps 등)"""
    try:
        with _db_lock:
            conn = _get_conn()
            conn.execute(
                "INSERT INTO api_calls (timestamp, user_id, service, endpoint, status_code, latency_ms, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), user_id, service, endpoint, status_code, latency_ms, error),
            )
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"API 로그 저장 실패: {e}")


# ─────────────────────────────────────────────
# Slack Bolt 글로벌 미들웨어
# ─────────────────────────────────────────────

def audit_middleware(body, payload, context, next):
    """
    모든 Slack 이벤트/커맨드/액션에 자동 적용되는 감사 미들웨어.
    app.use(audit_middleware) 한 줄로 등록.
    """
    start_time = time.time()

    # 이벤트 타입 결정
    event_type = "unknown"
    user_id = None
    channel_id = None
    user_text = None

    # message 이벤트
    event = body.get("event", {})
    if event.get("type") == "message" or event.get("type") == "assistant_thread_context_changed":
        event_type = "message"
        user_id = event.get("user")
        channel_id = event.get("channel")
        user_text = event.get("text", "")
    # app_mention
    elif event.get("type") == "app_mention":
        event_type = "app_mention"
        user_id = event.get("user")
        channel_id = event.get("channel")
        user_text = event.get("text", "")
    # slash command
    elif body.get("command"):
        event_type = "slash_command"
        user_id = body.get("user_id")
        channel_id = body.get("channel_id")
        user_text = body.get("text", "")
    # block action (버튼 클릭 등)
    elif body.get("type") == "block_actions":
        event_type = "block_action"
        user_id = body.get("user", {}).get("id")
        channel_id = body.get("channel", {}).get("id")
    # assistant_thread_started
    elif event.get("type") == "assistant_thread_started":
        event_type = "assistant_thread_started"
        user_id = event.get("assistant_thread", {}).get("user")
        channel_id = event.get("assistant_thread", {}).get("channel_id")

    # 봇 메시지는 건너뛰기
    if event.get("bot_id") or event.get("subtype"):
        next()
        return

    # ── 보안 스캔 (메시지가 있을 때만) ──
    if user_text and user_id:
        _run_security_scan(user_id, channel_id, user_text)

    # ── 요청 속도 제한 체크 ──
    if user_id:
        _check_rate_limit(user_id)

    # ── 심야 사용 체크 ──
    if user_id:
        _check_off_hours(user_id, event_type)

    # context에 user_id 주입 (scenarios.py에서 사용)
    context["audit_user_id"] = user_id

    # 감사 이벤트 기록
    _log_event(
        event_type=event_type,
        user_id=user_id,
        channel_id=channel_id,
        details={"text_length": len(user_text) if user_text else 0, "text_preview": (user_text[:100] if user_text else "")},
    )

    # 다음 미들웨어/핸들러로 전달
    next()

    # 처리 시간 기록
    elapsed = int((time.time() - start_time) * 1000)
    if elapsed > 5000:
        logger.warning(f"느린 요청: {event_type} user={user_id} {elapsed}ms")


# ─────────────────────────────────────────────
# 보안 스캔
# ─────────────────────────────────────────────

# DLP 패턴 — 민감 정보 탐지
DLP_PATTERNS = {
    "주민등록번호": re.compile(r"\d{6}\s*-\s*[1-4]\d{6}"),
    "카드번호": re.compile(r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}"),
    "API_KEY_SLACK": re.compile(r"xox[bpras]-[a-zA-Z0-9\-]+"),
    "API_KEY_GENERIC": re.compile(r"(?:sk-|AKIA|ghp_|gho_|github_pat_)[a-zA-Z0-9\-_]{20,}"),
    "내부_IP": re.compile(r"(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})"),
    "이메일_대량": re.compile(r"(?:[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}\s*[,;\n]\s*){3,}"),
}

# 프롬프트 인젝션 패턴
PROMPT_INJECTION_PATTERNS = [
    re.compile(r"(?i)ignore\s+(previous|all|above|prior)\s+(instructions?|prompts?|rules?)"),
    re.compile(r"(?i)you\s+are\s+now\s+(?:a|an|the)"),
    re.compile(r"(?i)(?:system|developer)\s*(?:prompt|message|instruction)"),
    re.compile(r"(?i)(?:pretend|act)\s+(?:to\s+be|as\s+if|like)"),
    re.compile(r"(?i)(?:reveal|show|print|output)\s+(?:your|the)\s+(?:system|initial|original)\s+(?:prompt|instruction)"),
    re.compile(r"(?i)DAN\s+mode|jailbreak|bypass\s+(?:filter|restriction|safety)"),
]


def _run_security_scan(user_id, channel_id, text):
    """메시지 보안 스캔 — DLP + 프롬프트 인젝션"""
    # DLP 패턴 검사
    for pattern_name, pattern in DLP_PATTERNS.items():
        if pattern.search(text):
            _create_security_alert(
                alert_type="dlp_violation",
                severity="warning",
                user_id=user_id,
                channel_id=channel_id,
                description=f"민감 정보 패턴 탐지: {pattern_name}",
                raw_data={"pattern": pattern_name, "text_preview": text[:100]},
            )

    # 프롬프트 인젝션 검사
    for pattern in PROMPT_INJECTION_PATTERNS:
        if pattern.search(text):
            _create_security_alert(
                alert_type="prompt_injection",
                severity="warning",
                user_id=user_id,
                channel_id=channel_id,
                description=f"프롬프트 인젝션 시도 탐지",
                raw_data={"matched_pattern": pattern.pattern, "text_preview": text[:100]},
            )
            break  # 하나만 잡으면 충분


# ─────────────────────────────────────────────
# 비정상 사용 패턴 탐지
# ─────────────────────────────────────────────

# 인메모리 요청 카운터 (5분 윈도우)
_request_counter = defaultdict(list)
_counter_lock = threading.Lock()


def _check_rate_limit(user_id):
    """5분 내 요청 수 임계치 초과 체크"""
    now = time.time()
    with _counter_lock:
        # 5분 넘은 기록 제거
        _request_counter[user_id] = [t for t in _request_counter[user_id] if now - t < RATE_LIMIT_WINDOW]
        _request_counter[user_id].append(now)
        count = len(_request_counter[user_id])

    if count > RATE_LIMIT_MAX_REQUESTS:
        _create_security_alert(
            alert_type="rate_limit_exceeded",
            severity="warning",
            user_id=user_id,
            description=f"{RATE_LIMIT_WINDOW}초 내 {count}회 요청 — 임계치({RATE_LIMIT_MAX_REQUESTS}) 초과",
            raw_data={"count": count, "window_seconds": RATE_LIMIT_WINDOW},
        )


def _check_off_hours(user_id, event_type):
    """심야 시간대 사용 감지"""
    hour = datetime.utcnow().hour + 9  # KST 변환 (단순)
    if hour >= 24:
        hour -= 24

    if hour >= OFF_HOURS_START or hour < OFF_HOURS_END:
        _log_event(
            event_type="off_hours_usage",
            user_id=user_id,
            details={"hour_kst": hour, "original_event": event_type},
            severity="info",
        )


def _check_daily_token_limit(user_id):
    """일일 토큰 사용량 임계치 초과 체크"""
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        with _db_lock:
            conn = _get_conn()
            row = conn.execute(
                "SELECT SUM(total_tokens) as total FROM llm_calls WHERE timestamp >= ? AND user_id = ?",
                (today, user_id),
            ).fetchone()
            conn.close()

        total = row["total"] or 0
        if total > DAILY_TOKEN_LIMIT:
            _create_security_alert(
                alert_type="token_limit_exceeded",
                severity="critical",
                user_id=user_id,
                description=f"일일 토큰 사용량 {total:,} — 임계치({DAILY_TOKEN_LIMIT:,}) 초과",
                raw_data={"total_tokens": total, "limit": DAILY_TOKEN_LIMIT},
            )
    except Exception as e:
        logger.error(f"토큰 임계치 체크 실패: {e}")


# ─────────────────────────────────────────────
# 보안 알림 생성 + Slack 알림 발송
# ─────────────────────────────────────────────

def _create_security_alert(alert_type, severity, user_id=None, channel_id=None, description="", raw_data=None):
    """보안 알림 DB 저장 + Slack 알림"""
    try:
        with _db_lock:
            conn = _get_conn()
            conn.execute(
                "INSERT INTO security_alerts (timestamp, alert_type, severity, user_id, channel_id, description, raw_data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), alert_type, severity, user_id, channel_id, description, json.dumps(raw_data or {}, ensure_ascii=False)),
            )
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"보안 알림 저장 실패: {e}")

    logger.warning(f"[SECURITY][{severity}] {alert_type}: {description} (user={user_id})")


def send_security_alerts(client):
    """
    미발송 보안 알림을 Slack으로 전송.
    별도 스케줄러나 앱 시작 시 호출.
    """
    if not ALERT_CHANNEL:
        return

    try:
        with _db_lock:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT * FROM security_alerts WHERE acknowledged = 0 AND severity IN ('warning', 'critical') ORDER BY timestamp DESC LIMIT 20"
            ).fetchall()
            conn.close()

        if not rows:
            return

        severity_emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}

        for row in rows:
            emoji = severity_emoji.get(row["severity"], "ℹ️")
            text = (
                f"{emoji} *보안 알림 — {row['severity'].upper()}*\n"
                f"→ 유형: {row['alert_type']}\n"
                f"→ 설명: {row['description']}\n"
                f"→ 사용자: <@{row['user_id']}>\n" if row["user_id"] else ""
                f"→ 시각: {row['timestamp'][:19]}"
            )

            try:
                client.chat_postMessage(channel=ALERT_CHANNEL, text=text)
            except Exception as e:
                logger.error(f"Slack 알림 전송 실패: {e}")

            # critical이면 관리자 DM
            if row["severity"] == "critical" and ADMIN_USER_ID:
                try:
                    dm = client.conversations_open(users=[ADMIN_USER_ID])
                    client.chat_postMessage(channel=dm["channel"]["id"], text=f"🚨 *긴급 보안 알림*\n{text}")
                except Exception:
                    pass

            # acknowledged 처리
            with _db_lock:
                conn = _get_conn()
                conn.execute("UPDATE security_alerts SET acknowledged = 1 WHERE id = ?", (row["id"],))
                conn.commit()
                conn.close()

    except Exception as e:
        logger.error(f"보안 알림 전송 루프 실패: {e}")


# ─────────────────────────────────────────────
# Audit Logs API 폴러 (Enterprise+)
# ─────────────────────────────────────────────

def poll_audit_logs_api(org_token, app_ids=None):
    """
    Slack Audit Logs API에서 앱 관련 이벤트를 폴링.
    org_token: Enterprise Org Owner의 xoxp- 토큰 (auditlogs:read 스코프)
    app_ids: 모니터링할 앱 ID 리스트 (None이면 전체)

    별도 스케줄러에서 5분 주기로 호출 권장.
    """
    try:
        from slack_sdk.audit_logs import AuditLogsClient

        client = AuditLogsClient(token=org_token)

        # 최근 10분 이벤트
        oldest = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())

        # 앱 관련 위험 액션 필터
        dangerous_actions = [
            "app_scopes_expanded",
            "app_token_preserved",
            "app_manifest_updated",
            "app_installed",
            "app_deleted",
        ]

        for action in dangerous_actions:
            try:
                response = client.logs(action=action, oldest=oldest, limit=100)
                entries = response.body.get("entries", [])

                for entry in entries:
                    entity_app_id = entry.get("entity", {}).get("app", {}).get("id")

                    # 특정 앱만 모니터링
                    if app_ids and entity_app_id not in app_ids:
                        continue

                    severity = "critical" if action == "app_scopes_expanded" else "warning"

                    _create_security_alert(
                        alert_type=f"audit_api_{action}",
                        severity=severity,
                        user_id=entry.get("actor", {}).get("user", {}).get("id"),
                        description=f"Audit Logs API: {action} (app={entity_app_id})",
                        raw_data={"action": action, "entity": entry.get("entity"), "context": entry.get("context")},
                    )
            except Exception as e:
                logger.error(f"Audit Logs API 폴링 실패 ({action}): {e}")

        # Anomaly 이벤트 폴링
        try:
            response = client.logs(action="anomaly", oldest=oldest, limit=100)
            entries = response.body.get("entries", [])

            for entry in entries:
                details = entry.get("details", {})
                reasons = details.get("reason", [])

                # 복수 anomaly = severity 상승
                severity = "critical" if len(reasons) > 1 else "warning"

                _create_security_alert(
                    alert_type="audit_api_anomaly",
                    severity=severity,
                    user_id=entry.get("actor", {}).get("user", {}).get("id"),
                    description=f"Anomaly: {', '.join(reasons) if isinstance(reasons, list) else reasons}",
                    raw_data=entry,
                )
        except Exception as e:
            logger.error(f"Anomaly 이벤트 폴링 실패: {e}")

    except ImportError:
        logger.warning("slack_sdk.audit_logs 미설치 — pip install slack_sdk")
    except Exception as e:
        logger.error(f"Audit Logs API 폴러 오류: {e}")


# ─────────────────────────────────────────────
# 통계 조회 함수 (대시보드용)
# ─────────────────────────────────────────────

def get_usage_stats(days=7):
    """최근 N일 사용 통계"""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    stats = {}

    try:
        with _db_lock:
            conn = _get_conn()

            # 총 요청 수
            row = conn.execute("SELECT COUNT(*) as cnt FROM audit_events WHERE timestamp >= ?", (since,)).fetchone()
            stats["total_requests"] = row["cnt"]

            # 사용자별 요청 수 Top 5
            rows = conn.execute(
                "SELECT user_id, COUNT(*) as cnt FROM audit_events WHERE timestamp >= ? AND user_id IS NOT NULL GROUP BY user_id ORDER BY cnt DESC LIMIT 5",
                (since,),
            ).fetchall()
            stats["top_users"] = [{"user_id": r["user_id"], "count": r["cnt"]} for r in rows]

            # 인텐트별 분포
            rows = conn.execute(
                "SELECT intent, COUNT(*) as cnt FROM audit_events WHERE timestamp >= ? AND intent IS NOT NULL GROUP BY intent ORDER BY cnt DESC",
                (since,),
            ).fetchall()
            stats["intent_distribution"] = [{"intent": r["intent"], "count": r["cnt"]} for r in rows]

            # LLM 토큰 합계
            row = conn.execute("SELECT SUM(total_tokens) as total, AVG(latency_ms) as avg_latency FROM llm_calls WHERE timestamp >= ?", (since,)).fetchone()
            stats["total_tokens"] = row["total"] or 0
            stats["avg_llm_latency_ms"] = int(row["avg_latency"] or 0)

            # 보안 알림 수
            row = conn.execute("SELECT COUNT(*) as cnt FROM security_alerts WHERE timestamp >= ?", (since,)).fetchone()
            stats["security_alerts"] = row["cnt"]

            # severity별 보안 알림
            rows = conn.execute(
                "SELECT severity, COUNT(*) as cnt FROM security_alerts WHERE timestamp >= ? GROUP BY severity",
                (since,),
            ).fetchall()
            stats["alerts_by_severity"] = {r["severity"]: r["cnt"] for r in rows}

            conn.close()
    except Exception as e:
        logger.error(f"통계 조회 실패: {e}")

    return stats


def get_audit_dashboard_blocks(days=7):
    """
    Slack App Home 탭에 표시할 감사 대시보드 Block Kit 블록.
    app.py의 app_home_opened 핸들러에서 사용.
    """
    stats = get_usage_stats(days)

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "📊 감사 로그 대시보드"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"최근 {days}일 기준"}},
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*총 요청 수*\n{stats.get('total_requests', 0):,}건"},
                {"type": "mrkdwn", "text": f"*LLM 토큰 사용량*\n{stats.get('total_tokens', 0):,}"},
                {"type": "mrkdwn", "text": f"*평균 응답 시간*\n{stats.get('avg_llm_latency_ms', 0):,}ms"},
                {"type": "mrkdwn", "text": f"*보안 알림*\n{stats.get('security_alerts', 0)}건"},
            ],
        },
    ]

    # Top 사용자
    top_users = stats.get("top_users", [])
    if top_users:
        user_lines = "\n".join([f"→ <@{u['user_id']}> — {u['count']}건" for u in top_users[:5]])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*👤 활발한 사용자 Top 5*\n{user_lines}"}})

    # 보안 상태
    alerts = stats.get("alerts_by_severity", {})
    if alerts:
        alert_text = "  ".join([f"{sev}: {cnt}건" for sev, cnt in alerts.items()])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*🔒 보안 알림 현황*\n{alert_text}"}})
    else:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*🔒 보안 알림 현황*\n이상 없음 ✅"}})

    return blocks


# ─────────────────────────────────────────────
# Canvas 리포트 생성용 마크다운
# ─────────────────────────────────────────────

def generate_audit_report_markdown(days=7, slack_client=None):
    """감사 리포트를 Canvas 마크다운으로 생성 (상세 버전)"""
    stats = get_usage_stats(days)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()

    # 사용자 이름 변환 헬퍼
    _user_cache = {}
    def _resolve_user(uid):
        if not uid:
            return "알 수 없음"
        if uid in _user_cache:
            return _user_cache[uid]
        if slack_client:
            try:
                info = slack_client.users_info(user=uid)
                name = info["user"]["real_name"] or info["user"]["name"]
                _user_cache[uid] = name
                return name
            except Exception:
                pass
        _user_cache[uid] = uid
        return uid

    lines = [
        f"# 📊 GS Retail AI 감사 리포트",
        f"생성일: {today} | 분석 기간: 최근 {days}일",
        "",
        "---",
        "",
        "## 📈 사용 현황 요약",
        f"- 총 요청 수: {stats.get('total_requests', 0):,}건",
        f"- LLM 총 토큰: {stats.get('total_tokens', 0):,}",
        f"- 평균 응답 시간: {stats.get('avg_llm_latency_ms', 0):,}ms",
        f"- 보안 알림: {stats.get('security_alerts', 0)}건",
    ]

    # ── 사용자별 상세 ──
    lines += ["", "---", "", "## 👤 사용자별 활동 분석"]
    top_users = stats.get("top_users", [])
    if top_users:
        for u in top_users:
            name = _resolve_user(u["user_id"])
            lines.append(f"- {name}: {u['count']}건")
    else:
        lines.append("- 활동 기록 없음")

    # ── 사용자별 LLM 토큰 소비 ──
    try:
        with _db_lock:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT user_id, SUM(total_tokens) as total, COUNT(*) as calls, AVG(latency_ms) as avg_lat "
                "FROM llm_calls WHERE timestamp >= ? AND user_id IS NOT NULL "
                "GROUP BY user_id ORDER BY total DESC LIMIT 10",
                (since,),
            ).fetchall()
            conn.close()
        if rows:
            lines += ["", "## 🤖 LLM 호출 분석"]
            for r in rows:
                name = _resolve_user(r["user_id"])
                lines.append(f"- {name}: {r['calls']}회 호출, 토큰 {r['total']:,}, 평균 {int(r['avg_lat']):,}ms")
    except Exception:
        pass

    # ── 인텐트 분포 ──
    lines += ["", "---", "", "## 🎯 인텐트 분포"]
    intent_dist = stats.get("intent_distribution", [])
    if intent_dist:
        total_intents = sum(i["count"] for i in intent_dist)
        for i in intent_dist:
            pct = (i["count"] / total_intents * 100) if total_intents > 0 else 0
            lines.append(f"- {i['intent']}: {i['count']}건 ({pct:.1f}%)")
    else:
        lines.append("- 인텐트 기록 없음")

    # ── 최근 사용자 질문 내역 ──
    try:
        with _db_lock:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT timestamp, user_id, details FROM audit_events "
                "WHERE timestamp >= ? AND event_type = 'message' AND details IS NOT NULL "
                "ORDER BY timestamp DESC LIMIT 30",
                (since,),
            ).fetchall()
            conn.close()
        if rows:
            lines += ["", "---", "", "## 💬 최근 사용자 질문 내역"]
            for r in rows:
                try:
                    d = json.loads(r["details"])
                    text_preview = d.get("text_preview", "")
                    if not text_preview or len(text_preview) < 2:
                        continue
                    ts = r["timestamp"][:19].replace("T", " ")
                    try:
                        h = int(ts[11:13]) + 9
                        if h >= 24:
                            h -= 24
                        ts_kst = f"{ts[:11]}{h:02d}{ts[13:]}"
                    except Exception:
                        ts_kst = ts
                    name = _resolve_user(r["user_id"])
                    lines.append(f"- [{ts_kst}] {name}: {text_preview}")
                except Exception:
                    continue
    except Exception:
        pass

    # ── 시간대별 사용 패턴 ──
    try:
        with _db_lock:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT timestamp FROM audit_events WHERE timestamp >= ? AND event_type = 'message'",
                (since,),
            ).fetchall()
            conn.close()
        if rows:
            hour_counts = defaultdict(int)
            for r in rows:
                try:
                    h = int(r["timestamp"][11:13]) + 9  # UTC → KST
                    if h >= 24:
                        h -= 24
                    hour_counts[h] += 1
                except Exception:
                    pass
            if hour_counts:
                lines += ["", "## ⏰ 시간대별 사용 패턴 (KST)"]
                peak_hour = max(hour_counts, key=hour_counts.get)
                lines.append(f"- 피크 시간: {peak_hour}시 ({hour_counts[peak_hour]}건)")
                off_hours = sum(v for h, v in hour_counts.items() if h >= 22 or h < 6)
                if off_hours > 0:
                    lines.append(f"- 심야 사용 (22시~06시): {off_hours}건")
                business = sum(v for h, v in hour_counts.items() if 9 <= h < 18)
                lines.append(f"- 업무 시간 (09시~18시): {business}건")
    except Exception:
        pass

    # ── LLM 성능 분석 ──
    try:
        with _db_lock:
            conn = _get_conn()
            row = conn.execute(
                "SELECT MIN(latency_ms) as min_lat, MAX(latency_ms) as max_lat, "
                "AVG(latency_ms) as avg_lat, COUNT(*) as total_calls, "
                "SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as error_count "
                "FROM llm_calls WHERE timestamp >= ?",
                (since,),
            ).fetchone()
            conn.close()
        if row and row["total_calls"] > 0:
            lines += ["", "---", "", "## ⚡ LLM 성능 분석"]
            lines.append(f"- 총 호출: {row['total_calls']}회")
            lines.append(f"- 응답 시간: 최소 {row['min_lat']:,}ms / 평균 {int(row['avg_lat']):,}ms / 최대 {row['max_lat']:,}ms")
            error_rate = (row["error_count"] / row["total_calls"] * 100) if row["total_calls"] > 0 else 0
            lines.append(f"- 에러: {row['error_count']}건 ({error_rate:.1f}%)")
            if error_rate > 5:
                lines.append(f"- ⚠️ 에러율이 5%를 초과합니다. 확인이 필요합니다.")
    except Exception:
        pass

    # ── 보안 알림 상세 ──
    lines += ["", "---", "", "## 🔒 보안 알림 상세"]
    try:
        with _db_lock:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT timestamp, alert_type, severity, user_id, description, raw_data "
                "FROM security_alerts WHERE timestamp >= ? ORDER BY timestamp DESC LIMIT 20",
                (since,),
            ).fetchall()
            conn.close()

        if rows:
            # severity 요약
            sev_counts = defaultdict(int)
            type_counts = defaultdict(int)
            for r in rows:
                sev_counts[r["severity"]] += 1
                type_counts[r["alert_type"]] += 1

            lines.append(f"총 {len(rows)}건의 보안 알림 발생\n")
            lines.append("**등급별 현황**")
            for sev in ["critical", "warning", "info"]:
                if sev_counts.get(sev, 0) > 0:
                    emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(sev, "")
                    lines.append(f"- {emoji} {sev}: {sev_counts[sev]}건")

            lines.append("\n**유형별 현황**")
            for atype, cnt in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
                label = {"dlp_violation": "민감정보 노출", "prompt_injection": "프롬프트 인젝션",
                         "rate_limit_exceeded": "요청 속도 초과", "token_limit_exceeded": "토큰 한도 초과"}.get(atype, atype)
                lines.append(f"- {label}: {cnt}건")

            lines.append("\n**개별 알림 내역**")
            for r in rows:
                ts = r["timestamp"][:19].replace("T", " ")
                # UTC → KST 간단 변환
                try:
                    h = int(ts[11:13]) + 9
                    if h >= 24:
                        h -= 24
                    ts_kst = f"{ts[:11]}{h:02d}{ts[13:]}"
                except Exception:
                    ts_kst = ts
                name = _resolve_user(r["user_id"])
                emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(r["severity"], "")
                lines.append(f"- {emoji} [{ts_kst}] {r['description']} (사용자: {name})")
        else:
            lines.append("✅ 이상 없음 — 보안 알림이 발생하지 않았습니다.")
    except Exception:
        lines.append("- 보안 알림 조회 실패")

    # ── 종합 평가 ──
    lines += ["", "---", "", "## 📋 종합 평가"]
    total_req = stats.get("total_requests", 0)
    total_alerts = stats.get("security_alerts", 0)
    alert_rate = (total_alerts / total_req * 100) if total_req > 0 else 0

    if total_alerts == 0:
        lines.append("✅ 보안 이상 없음. 정상 운영 중입니다.")
    elif alert_rate < 5:
        lines.append(f"🟡 주의 — 보안 알림 {total_alerts}건 발생 (요청 대비 {alert_rate:.1f}%). 모니터링을 지속하세요.")
    else:
        lines.append(f"🔴 경고 — 보안 알림 {total_alerts}건 발생 (요청 대비 {alert_rate:.1f}%). 즉각 점검이 필요합니다.")

    if stats.get("avg_llm_latency_ms", 0) > 3000:
        lines.append("⚠️ LLM 평균 응답 시간이 3초를 초과합니다. 모델 또는 프롬프트 최적화를 검토하세요.")

    lines += ["", "---", f"생성: GS Retail AI Audit Logger | {today}"]

    return "\n".join(lines)
