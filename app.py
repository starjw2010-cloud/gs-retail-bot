"""
app.py — GS Retail AI Agent 메인 앱
SlackBolt + Socket Mode 기반
프로모션 매니저 봇 + OFC 스마트 어시스턴트 통합 운영
"""

import os
import threading
import time
import logging
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from scenarios import route_and_execute, detect_intent
from audit_logger import audit_middleware, log_llm_call, get_audit_dashboard_blocks, send_security_alerts, generate_audit_report_markdown, get_usage_stats  # ← 추가

# ─────────────────────────────────────────────
# 환경 설정
# ─────────────────────────────────────────────

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
app.use(audit_middleware)  # ← 추가: 모든 이벤트 자동 감사 로그
_BOT_USER_ID = None


# ─────────────────────────────────────────────
# 상태 메시지 매핑
# ─────────────────────────────────────────────

STATUS_MAP = {
    "promo_search": "📦 프로모션 조회 중...",
    "promo_product_check": "🔍 상품 행사 확인 중...",
    "promo_upcoming": "📅 예정 행사 조회 중...",
    "promo_ending": "⏰ 종료 임박 행사 확인 중...",
    "promo_performance": "📊 행사 성과 분석 중...",
    "ofc_manual": "📖 매뉴얼 검색 중...",
    "ofc_store_status": "🏪 점포 현황 조회 중...",
    "ofc_issue_register": "✏️ 이슈 등록 중...",
    "ofc_notice": "📢 공지사항 확인 중...",
    "orchestration_briefing": "🤖 통합 브리핑 생성 중...",
    "orchestration_weekly": "📋 주간 리포트 생성 중...",
    "canvas_create": "📋 Canvas 문서 생성 중...",
    "general_chat": "💬 답변 준비 중...",
}


def _get_bot_user_id(client):
    global _BOT_USER_ID
    if _BOT_USER_ID:
        return _BOT_USER_ID
    try:
        auth = client.auth_test()
        _BOT_USER_ID = auth.get("user_id")
    except Exception:
        _BOT_USER_ID = None
    return _BOT_USER_ID


def _get_previous_bot_reply(client, channel, thread_ts, current_ts=None):
    """
    스레드 내 직전 봇 답변 텍스트를 가져온다.
    """
    try:
        resp = client.conversations_replies(channel=channel, ts=thread_ts, limit=100)
        messages = resp.get("messages", [])
        bot_user_id = _get_bot_user_id(client)
        for m in reversed(messages):
            if current_ts and m.get("ts") == current_ts:
                continue
            text = (m.get("text") or "").strip()
            if not text:
                continue
            is_bot_message = (
                bool(m.get("bot_id"))
                or m.get("subtype") == "bot_message"
                or (bot_user_id and m.get("user") == bot_user_id)
            )
            if is_bot_message:
                return text
    except Exception as e:
        logger.warning(f"직전 봇 답변 조회 실패: {e}")
    return None


STREAMING_INTENTS = {
    "orchestration_briefing", "orchestration_weekly",
    "ai_analysis", "general_chat", "ofc_manual",
    "store_promo_analysis",
    "multi_agent_analysis",
}


def _stream_to_slack(client, channel, thread_ts, text):
    """응답을 줄 단위로 스트리밍 (타이핑 효과)"""
    try:
        resp = client.chat_postMessage(channel=channel, thread_ts=thread_ts, text="💭 분석 중...")
        msg_ts = resp["ts"]

        lines = text.split("\n")
        buffer = ""
        update_count = 0
        for line in lines:
            buffer += line + "\n"
            update_count += 1
            # 3줄마다 업데이트 (Slack rate limit 방지)
            if update_count % 3 == 0:
                try:
                    client.chat_update(channel=channel, ts=msg_ts, text=buffer.strip())
                    time.sleep(0.4)
                except Exception:
                    time.sleep(1.0)

        # 최종 업데이트
        client.chat_update(channel=channel, ts=msg_ts, text=text)
        return msg_ts
    except Exception as e:
        logger.error(f"스트리밍 오류: {e}")
        client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=text)
        return None


def _get_thread_history(client, channel, thread_ts, current_ts=None, limit=10):
    """스레드 내 최근 대화 히스토리 (사용자+봇) 수집"""
    try:
        resp = client.conversations_replies(channel=channel, ts=thread_ts, limit=100)
        messages = resp.get("messages", [])
        bot_user_id = _get_bot_user_id(client)
        history = []
        for m in messages:
            if current_ts and m.get("ts") == current_ts:
                continue
            text = (m.get("text") or "").strip()
            if not text:
                continue
            is_bot = (
                bool(m.get("bot_id"))
                or m.get("subtype") == "bot_message"
                or (bot_user_id and m.get("user") == bot_user_id)
            )
            role = "assistant" if is_bot else "user"
            history.append({"role": role, "text": text[:500]})
        return history[-limit:]
    except Exception as e:
        logger.warning(f"스레드 히스토리 조회 실패: {e}")
    return []


def _get_all_bot_replies(client, channel, thread_ts, current_ts=None):
    """스레드 내 모든 봇 답변을 시간순으로 수집 (리스트 반환)"""
    try:
        resp = client.conversations_replies(channel=channel, ts=thread_ts, limit=100)
        messages = resp.get("messages", [])
        bot_user_id = _get_bot_user_id(client)
        replies = []
        for m in messages:
            if current_ts and m.get("ts") == current_ts:
                continue
            text = (m.get("text") or "").strip()
            if not text or len(text) < 30:
                continue
            is_bot = (
                bool(m.get("bot_id"))
                or m.get("subtype") == "bot_message"
                or (bot_user_id and m.get("user") == bot_user_id)
            )
            if is_bot:
                replies.append(text)
        return replies
    except Exception as e:
        logger.warning(f"전체 봇 답변 조회 실패: {e}")
    return []


# ─────────────────────────────────────────────
# Assistant Thread API — 웰컴 메시지 + 추천 프롬프트
# ─────────────────────────────────────────────

@app.event("assistant_thread_started")
def handle_assistant_thread_started(event, say, set_suggested_prompts):
    """어시스턴트 스레드 시작 시 웰컴 메시지 + 추천 프롬프트"""
    try:
        say(
            text=(
                "안녕하세요! *GS Retail AI 어시스턴트*입니다 🏪\n\n"
                "저는 GS25 프로모션 매니저와 OFC 스마트 어시스턴트 역할을 합니다.\n\n"
                "📦 *프로모션* — 행사 조회, 카테고리별 검색, 종료 임박 알림\n"
                "🏪 *점포 현황* — 지역별/도 단위/권역별 점포 분석\n"
                "📖 *매뉴얼* — 운영 절차, 규정 검색\n"
                "🧠 *AI 분석* — 점포 전략, 비교 분석\n"
                "📋 *캔버스* — 분석 결과를 Canvas 리포트로 정리\n\n"
                "아래 추천 질문을 눌러보시거나, 편하게 물어보세요!"
            )
        )
        set_suggested_prompts(
            prompts=[
                {"title": "📦 진행 중인 행사", "message": "지금 진행 중인 행사 보여줘"},
                {"title": "🏪 서울 점포 현황", "message": "서울 점포 현황 알려줘"},
                {"title": "📊 오늘 방문 브리핑", "message": "오늘 방문 브리핑 해줘"},
                {"title": "📋 전체 캔버스 리포트", "message": "전체 대화 캔버스로 정리해줘"},
            ]
        )
    except Exception as e:
        logger.error(f"assistant_thread_started 오류: {e}")


# ─────────────────────────────────────────────
# 메시지 핸들러 — DM + 멘션
# ─────────────────────────────────────────────

@app.event("message")
def handle_message(event, say, set_status, client):
    """DM 메시지 처리 (Assistant Thread)"""
    # 봇 자신의 메시지 무시
    if event.get("bot_id") or event.get("subtype"):
        return

    user_message = event.get("text", "").strip()
    if not user_message:
        return

    channel = event.get("channel", "")
    thread_ts = event.get("thread_ts") or event.get("ts")
    current_ts = event.get("ts")

    def process():
        try:
            # 감사 리포트 요청 감지 (관리자 전용) ← 추가
            audit_keywords = ["감사 리포트", "감사 로그", "audit report", "감사리포트", "감사로그"]
            user_id = event.get("user", "")
            admin_id = os.environ.get("AUDIT_ADMIN_USER_ID", "")
            if user_id == admin_id and any(k in user_message for k in audit_keywords):
                try:
                    set_status("📊 감사 리포트 생성 중...")
                except Exception:
                    pass
                from scenarios import create_canvas
                from datetime import datetime as _dt
                has_canvas = any(k in user_message for k in ["캔버스", "canvas", "리포트로", "문서로"])
                if has_canvas:
                    md = generate_audit_report_markdown(days=7)
                    title = f"감사 리포트 - {_dt.now().strftime('%m/%d %H:%M')}"
                    canvas_result = create_canvas(client, channel, md, title)
                    # create_canvas가 이미 메시지를 보냄 — 에러일 때만 say
                    if not canvas_result.startswith("http"):
                        say(text=canvas_result, thread_ts=thread_ts)
                else:
                    stats = get_usage_stats(days=7)
                    top_users = "\n".join([f"  → <@{u['user_id']}> — {u['count']}건" for u in stats.get("top_users", [])[:5]])
                    alerts = stats.get("alerts_by_severity", {})
                    alert_text = "이상 없음 ✅" if not alerts else "  ".join([f"{sev}: {cnt}건" for sev, cnt in alerts.items()])
                    say(text=(
                        f"📊 *감사 로그 요약* (최근 7일)\n\n"
                        f"→ 총 요청: *{stats.get('total_requests', 0):,}건*\n"
                        f"→ LLM 토큰: *{stats.get('total_tokens', 0):,}*\n"
                        f"→ 평균 응답: *{stats.get('avg_llm_latency_ms', 0):,}ms*\n\n"
                        f"*👤 활발한 사용자*\n{top_users}\n\n"
                        f"*🔒 보안 알림*\n→ {alert_text}\n\n"
                        f"💡 \"감사 리포트 캔버스로\" 라고 하면 Canvas로 생성해요!"
                    ), thread_ts=thread_ts)
                try:
                    set_status("")
                except Exception:
                    pass
                return

            # 인텐트 감지 → 상태 표시
            intent = detect_intent(user_message)
            status_text = STATUS_MAP.get(intent, "💬 처리 중...")

            try:
                set_status(status_text)
            except Exception:
                pass  # set_status 미지원 환경 대비

            logger.info(f"[{intent}] {user_message[:50]}...")

            previous_bot_reply = None
            all_bot_replies = None
            thread_history = _get_thread_history(
                client=client, channel=channel,
                thread_ts=thread_ts, current_ts=current_ts,
            )
            if intent == "canvas_create":
                previous_bot_reply = _get_previous_bot_reply(
                    client=client,
                    channel=channel,
                    thread_ts=thread_ts,
                    current_ts=current_ts,
                )
                all_bot_replies = _get_all_bot_replies(
                    client=client,
                    channel=channel,
                    thread_ts=thread_ts,
                    current_ts=current_ts,
                )

            # 시나리오 실행
            result, _ = route_and_execute(
                user_message,
                slack_client=client,
                channel=channel,
                previous_bot_reply=previous_bot_reply,
                all_bot_replies=all_bot_replies,
                thread_history=thread_history,
            )

            # 응답 전송 (LLM 인텐트는 스트리밍)
            if intent in STREAMING_INTENTS:
                _stream_to_slack(client, channel, thread_ts, result)
            else:
                say(text=result, thread_ts=thread_ts)

            # 상태 초기화
            try:
                set_status("")
            except Exception:
                pass

        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}")
            say(text=f"❌ 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.", thread_ts=thread_ts)
            try:
                set_status("")
            except Exception:
                pass

    # 비동기 처리 (Slack 3초 제한 회피)
    threading.Thread(target=process, daemon=True).start()


@app.event("app_mention")
def handle_app_mention(event, say, client):
    """채널에서 @멘션 처리"""
    user_message = event.get("text", "").strip()
    # @봇이름 제거
    import re
    user_message = re.sub(r'<@[A-Z0-9]+>\s*', '', user_message).strip()

    if not user_message:
        say(
            text="무엇을 도와드릴까요? 프로모션 조회, 매뉴얼 검색, 점포 현황 등을 물어보세요!",
            thread_ts=event.get("ts"),
        )
        return

    thread_ts = event.get("ts")
    current_ts = event.get("ts")

    def process():
        try:
            intent = detect_intent(user_message)
            previous_bot_reply = None
            if intent == "canvas_create":
                previous_bot_reply = _get_previous_bot_reply(
                    client=client,
                    channel=event.get("channel"),
                    thread_ts=thread_ts,
                    current_ts=current_ts,
                )

            result, intent = route_and_execute(
                user_message,
                slack_client=client,
                channel=event.get("channel"),
                previous_bot_reply=previous_bot_reply,
            )
            logger.info(f"[mention][{intent}] {user_message[:50]}...")
            say(text=result, thread_ts=thread_ts)
        except Exception as e:
            logger.error(f"앱 멘션 처리 오류: {e}")
            say(text="❌ 처리 중 오류가 발생했습니다.", thread_ts=thread_ts)

    threading.Thread(target=process, daemon=True).start()


# ─────────────────────────────────────────────
# 슬래시 커맨드
# ─────────────────────────────────────────────

@app.command("/promo")
def handle_promo_command(ack, command, say):
    """프로모션 조회 슬래시 커맨드"""
    ack()
    user_text = command.get("text", "").strip()
    if not user_text:
        user_text = "이번 주 진행 중인 행사 전체 보여줘"

    def process():
        result, _ = route_and_execute(user_text, slack_client=app.client, channel=command.get("channel_id"))
        say(text=result)

    threading.Thread(target=process, daemon=True).start()


@app.command("/manual")
def handle_manual_command(ack, command, say):
    """매뉴얼 검색 슬래시 커맨드"""
    ack()
    user_text = command.get("text", "").strip()
    if not user_text:
        say(text="검색할 내용을 입력해주세요. 예: /manual 유통기한 처리 절차")
        return

    def process():
        result, _ = route_and_execute(user_text, slack_client=app.client, channel=command.get("channel_id"))
        say(text=result)

    threading.Thread(target=process, daemon=True).start()


@app.command("/store")
def handle_store_command(ack, command, say):
    """점포 현황 슬래시 커맨드"""
    ack()
    user_text = command.get("text", "").strip()
    if not user_text:
        user_text = "담당 점포 현황 요약해줘"

    def process():
        result, _ = route_and_execute(user_text, slack_client=app.client, channel=command.get("channel_id"))
        say(text=result)

    threading.Thread(target=process, daemon=True).start()


@app.command("/briefing")
def handle_briefing_command(ack, command, say):
    """통합 브리핑 슬래시 커맨드"""
    ack()

    def process():
        result, _ = route_and_execute("오늘 방문 브리핑 해줘", slack_client=app.client, channel=command.get("channel_id"))
        say(text=result)

    threading.Thread(target=process, daemon=True).start()


@app.command("/audit")
def handle_audit_command(ack, command, say, client):
    """감사 로그 조회 슬래시 커맨드 (관리자 전용)"""
    ack()
    user_id = command.get("user_id", "")
    admin_id = os.environ.get("AUDIT_ADMIN_USER_ID", "")

    # 관리자만 사용 가능
    if user_id != admin_id:
        say(text="🔒 감사 로그는 관리자만 조회할 수 있습니다.")
        return

    sub_command = command.get("text", "").strip().lower()

    def process():
        try:
            # /audit canvas → Canvas 리포트 생성
            if sub_command in ["canvas", "캔버스", "리포트", "report"]:
                from scenarios import create_canvas
                md = generate_audit_report_markdown(days=7)
                from datetime import datetime as _dt
                title = f"감사 리포트 - {_dt.now().strftime('%m/%d %H:%M')}"
                canvas_result = create_canvas(client, command.get("channel_id"), md, title)
                # create_canvas가 이미 채널에 메시지를 보냄 — say() 안 함
                if not canvas_result.startswith("http"):
                    say(text=canvas_result)  # 에러일 때만 say
                return

            # /audit (기본) → 통계 요약
            stats = get_usage_stats(days=7)
            top_users_lines = ""
            for u in stats.get("top_users", [])[:5]:
                top_users_lines += f"\n  → <@{u['user_id']}> — {u['count']}건"

            alerts = stats.get("alerts_by_severity", {})
            alert_text = "이상 없음 ✅" if not alerts else "  ".join([f"{sev}: {cnt}건" for sev, cnt in alerts.items()])

            intent_lines = ""
            for i in stats.get("intent_distribution", [])[:7]:
                intent_lines += f"\n  → {i['intent']}: {i['count']}건"

            text = (
                f"📊 *감사 로그 요약* (최근 7일)\n\n"
                f"*📈 사용 현황*\n"
                f"→ 총 요청: *{stats.get('total_requests', 0):,}건*\n"
                f"→ LLM 토큰: *{stats.get('total_tokens', 0):,}*\n"
                f"→ 평균 응답: *{stats.get('avg_llm_latency_ms', 0):,}ms*\n\n"
                f"*👤 활발한 사용자*{top_users_lines}\n\n"
                f"*🎯 인텐트 분포*{intent_lines}\n\n"
                f"*🔒 보안 알림*\n→ {alert_text}\n\n"
                f"💡 Canvas 리포트: `/audit canvas`"
            )
            say(text=text)

        except Exception as e:
            logger.error(f"/audit 처리 오류: {e}")
            say(text="❌ 감사 로그 조회 중 오류가 발생했습니다.")

    threading.Thread(target=process, daemon=True).start()


# ─────────────────────────────────────────────
# App Home 탭 — 기존 대시보드 + 감사 로그 대시보드
# ─────────────────────────────────────────────

@app.event("app_home_opened")
def handle_app_home(event, client):
    """App Home 탭 — 대시보드 + 감사 로그"""
    user_id = event["user"]
    try:
        # 기존 메인 블록
        main_blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🏪 GS Retail AI 어시스턴트"}
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "프로모션 조회 · 매뉴얼 검색 · 점포 현황 확인\n"
                        "DM으로 대화하거나, 채널에서 @멘션 해주세요!"
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*📦 프로모션 매니저*\n"
                        "→ 진행/예정/종료 행사 조회, 카테고리별 검색, 상품 검색\n"
                        "→ 예: \"음료 행사\", \"콜라 행사 중이야?\", \"곧 끝나는 행사\""
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*🏪 점포 현황 (1,500개 점포)*\n"
                        "→ 서울/강남/역삼 (시→구→동네 계층 검색)\n"
                        "→ 전라도/경상도 (도 단위), 수도권/영남권 (권역)\n"
                        "→ 매출 상위/하위, 폐기율, 퀵커머스, 이슈 점포"
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*📖 매뉴얼 검색*\n"
                        "→ 유통기한, 폐기, 위생, 계약, 퀵커머스 등 20개 매뉴얼\n"
                        "→ 예: \"유통기한 지난 상품 어떻게 처리해?\""
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*🧠 AI 분석 & 브리핑*\n"
                        "→ 점포 전략, 지역 비교, 프로모션 연관 분석\n"
                        "→ 오늘 방문 브리핑, 주간 리포트"
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*📋 Canvas 리포트*\n"
                        "→ \"캔버스로 만들어줘\" — 바로 위 답변을 Canvas로\n"
                        "→ \"전체 대화 캔버스로\" — 모든 답변을 Canvas로\n"
                        "→ \"요약해서 캔버스로\" — AI 요약 Canvas\n"
                        "→ \"강남과 부산 캔버스로\" — 여러 지역 통합 Canvas"
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*슬래시 커맨드*\n"
                        "• `/promo` — 프로모션 조회\n"
                        "• `/manual 검색어` — 매뉴얼 검색\n"
                        "• `/store` — 점포 현황\n"
                        "• `/briefing` — 통합 브리핑"
                    ),
                },
            },
        ]

        # ── 감사 로그 대시보드 (관리자만 표시) ── ← 추가
        admin_user_id = os.environ.get("AUDIT_ADMIN_USER_ID", "")
        if user_id == admin_user_id:
            try:
                audit_blocks = get_audit_dashboard_blocks(days=7)
                main_blocks.append({"type": "divider"})
                main_blocks.extend(audit_blocks)
            except Exception as e:
                logger.warning(f"감사 대시보드 로드 실패 (무시): {e}")

        # 푸터
        main_blocks.append({"type": "divider"})
        main_blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "GS Retail AI Agent v0.2 | 1,500 점포 · 300 프로모션 · 20 매뉴얼 | Powered by Groq (Llama 3.3 70B)",
                }
            ],
        })

        client.views_publish(
            user_id=user_id,
            view={"type": "home", "blocks": main_blocks}
        )
    except Exception as e:
        logger.error(f"App Home 오류: {e}")


# ─────────────────────────────────────────────
# 앱 실행
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # 보안 알림 주기적 발송 (5분 간격) ← 추가
    def _alert_scheduler():
        while True:
            try:
                send_security_alerts(app.client)
            except Exception as e:
                logger.error(f"알림 스케줄러 오류: {e}")
            time.sleep(300)

    alert_thread = threading.Thread(target=_alert_scheduler, daemon=True)
    alert_thread.start()
    logger.info("🔒 보안 알림 스케줄러 시작")

    logger.info("🚀 GS Retail AI Agent 시작!")
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    handler.start()
