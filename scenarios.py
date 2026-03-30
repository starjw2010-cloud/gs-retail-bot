"""
scenarios.py — GS Retail AI Agent 시나리오 실행 모듈
프로모션 매니저 봇 + OFC 스마트 어시스턴트 + 통합 오케스트레이션
1500건 데이터 기반 다양한 질문 대응
"""

import os
import re
import json
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from mock_data import (
    get_active_promotions, get_upcoming_promotions, get_ending_soon,
    get_starting_soon, search_promotion, get_promo_by_category,
    get_ended_promotions, get_promo_stats,
    get_stores_by_ofc, get_stores_by_region, get_store_by_name,
    get_struggling_stores, get_top_stores, get_bottom_stores,
    get_high_waste_stores, get_store_issues, get_quick_commerce_stores,
    get_region_summary, search_manual,
    PROMOTIONS, STORES, MANUALS, PRODUCT_CATALOG, REGIONS, OFC_NAMES,
)

# ─────────────────────────────────────────────
# LLM 호출 함수 (Groq API — OpenAI 호환)
# ─────────────────────────────────────────────

load_dotenv()

_llm_client = OpenAI(
    api_key=os.environ.get("GROQ_API_KEY", "").strip(),
    base_url="https://api.groq.com/openai/v1",
)

def _call_llm(system_prompt, user_message, temperature=0.3):
    try:
        response = _llm_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=temperature,
            max_tokens=2000,
        )
        return response.choices[0].message.content
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "rate" in error_msg.lower():
            return "⏳ 잠시 후 다시 시도해주세요. (API 호출 한도 초과)"
        return f"❌ AI 분석 중 오류가 발생했습니다: {error_msg[:100]}"


def _to_slack_mrkdwn(text):
    if not text:
        return text
    text = re.sub(r'\*\*(.+?)\*\*', r'*\1*', text)
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'^[\-]\s+', '• ', text, flags=re.MULTILINE)
    text = re.sub(r'^\*\s+(?![*])', '• ', text, flags=re.MULTILINE)
    text = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text

CATEGORY_LIST = list(PRODUCT_CATALOG.keys())

def _extract_category(msg):
    for cat in CATEGORY_LIST:
        if cat in msg:
            return cat
    return None

def _extract_region(msg):
    region_keywords = []
    for region, locations in REGIONS.items():
        region_keywords.append(region)
        region_keywords.extend(locations)
    for kw in sorted(region_keywords, key=len, reverse=True):
        if kw in msg:
            return kw
    return None

def _extract_ofc(msg):
    for name in OFC_NAMES:
        if name in msg:
            return name
    return None

def _extract_number(msg, default=5):
    nums = re.findall(r'(\d+)', msg)
    if nums:
        n = int(nums[0])
        return min(n, 20)
    return default


def _find_store_in_message(user_message):
    for s in STORES:
        if s["name"] in user_message:
            return s
    candidates = re.findall(r"([가-힣A-Za-z0-9]+점)", user_message)
    for token in sorted(set(candidates), key=len, reverse=True):
        for s in STORES:
            if token in s["name"] or s["name"] in token:
                return s
    return None


# ─────────────────────────────────────────────
# 도 단위 / 권역 매핑
# ─────────────────────────────────────────────

def _find_all_stores_in_message(user_message):
    """메시지에서 매칭되는 점포를 모두 반환"""
    matches = []
    seen_ids = set()
    # 1) 정확 일치
    for s in STORES:
        if s["name"] in user_message and s["id"] not in seen_ids:
            matches.append(s)
            seen_ids.add(s["id"])
    if matches:
        return matches
    # 2) "OO점" 패턴
    candidates = re.findall(r"([가-힣A-Za-z0-9]+점)", user_message)
    for token in sorted(set(candidates), key=len, reverse=True):
        for s in STORES:
            if s["id"] not in seen_ids and (token in s["name"] or s["name"] in token):
                matches.append(s)
                seen_ids.add(s["id"])
    if matches:
        return matches
    # 3) 점포명 키워드 부분 일치 (예: "상동3호" → "상동3호점")
    for s in STORES:
        name_clean = s["name"].replace("점", "")
        for word in user_message.split():
            word_clean = word.replace("점", "").replace("은", "").replace("는", "").replace("이", "").replace("가", "")
            if len(word_clean) >= 3 and word_clean in name_clean and s["id"] not in seen_ids:
                matches.append(s)
                seen_ids.add(s["id"])
    return matches


_PROVINCE_MAP = {
    "전라도": ["전북", "전남", "광주"],
    "경상도": ["경북", "경남", "부산", "대구", "울산"],
    "충청도": ["충북", "충남", "대전", "세종"],
    "강원도": ["강원"],
    "제주도": ["제주"],
    "전라": ["전북", "전남", "광주"],
    "경상": ["경북", "경남", "부산", "대구", "울산"],
    "충청": ["충북", "충남", "대전", "세종"],
    "호남": ["전북", "전남", "광주"],
    "영남": ["경북", "경남", "부산", "대구", "울산"],
}

_MACRO_AREAS = {
    "수도권": {"emoji": "🏙️", "order": 0, "prefixes": ("서울", "경기", "인천")},
    "충청권": {"emoji": "🌾", "order": 1, "prefixes": ("대전", "세종", "충북", "충남")},
    "영남권": {"emoji": "🌊", "order": 2, "prefixes": ("부산", "대구", "울산", "경북", "경남")},
    "호남권": {"emoji": "🌿", "order": 3, "prefixes": ("광주", "전북", "전남")},
    "강원권": {"emoji": "⛰️", "order": 4, "prefixes": ("강원",)},
    "제주권": {"emoji": "🏝️", "order": 5, "prefixes": ("제주",)},
}


def _get_macro_area(region_name):
    if not region_name:
        return "기타"
    for macro, conf in _MACRO_AREAS.items():
        if region_name.startswith(conf["prefixes"]):
            return macro
    return "기타"


def _get_stores_by_province(province_key):
    """도 단위 키워드로 여러 지역 점포를 합쳐서 반환"""
    prefixes = _PROVINCE_MAP.get(province_key, [])
    if not prefixes:
        return []
    result = []
    for s in STORES:
        region = s.get("region", "")
        if any(region.startswith(p) for p in prefixes):
            result.append(s)
    return result


def _region_mentioned(user_message):
    msg = user_message
    # 도 단위 키워드 확인
    for prov in _PROVINCE_MAP:
        if prov in msg:
            return True
    # 권역 키워드 확인
    for macro in _MACRO_AREAS:
        if macro in msg:
            return True
    # REGIONS 키워드 확인
    if _extract_region(msg):
        return True
    for region_name, locations in REGIONS.items():
        if region_name in msg:
            return True
        for loc in locations:
            if loc in msg:
                return True
        for part in region_name.split():
            if len(part) >= 2 and part in msg:
                return True
    return False


def _is_nationwide_region_query(user_message):
    for k in ["전국", "전체", "지역별", "권역별"]:
        if k in user_message:
            return True
    return False


def _extract_region_filter_keyword(user_message):
    """지역 필터용 키워드 (도 단위 → 권역 → REGIONS → 동네 순서)"""
    # 1) 도 단위 매핑 확인 (전라도, 경상도, 충청도 등)
    for prov in sorted(_PROVINCE_MAP.keys(), key=len, reverse=True):
        if prov in user_message:
            return prov

    # 2) 권역 확인 (수도권, 영남권 등)
    for macro in _MACRO_AREAS:
        if macro in user_message:
            return macro

    # 3) REGIONS 키워드 + 동네
    tokens = []
    for region_name, locations in REGIONS.items():
        tokens.append(region_name)
        tokens.extend(locations)
        for part in region_name.split():
            if len(part) >= 2:
                tokens.append(part)
    for kw in sorted(set(tokens), key=len, reverse=True):
        if kw in user_message:
            return kw
    return None


def _get_region_suggestions(keyword, limit=5):
    kw = (keyword or "").replace("시", "").replace("구", "").replace("군", "").strip()
    candidates = []
    for r, locs in REGIONS.items():
        candidates.append(r)
        candidates.extend(locs)
        for part in r.split():
            if len(part) >= 2:
                candidates.append(part)
    uniq = list(dict.fromkeys(candidates))
    matched = [c for c in uniq if kw and (kw in c or c in kw)]
    if not matched:
        matched = uniq[:]
    return matched[:limit]


def _format_nationwide_region_summary():
    summary = get_region_summary()
    grouped = {}
    for r, data in summary.items():
        macro = _get_macro_area(r)
        grouped.setdefault(macro, []).append((r, data))

    lines = ["📍 *지역별 점포 현황 (권역별)*\n"]
    for macro, _items in sorted(grouped.items(), key=lambda x: _MACRO_AREAS.get(x[0], {"order": 99})["order"]):
        emoji = _MACRO_AREAS.get(macro, {"emoji": "📌"})["emoji"]
        lines.append(f"{emoji} *{macro}*")
        for r, data in sorted(_items, key=lambda x: x[1]["count"], reverse=True):
            lines.append(
                f"• {r}: {data['count']}개 점포, 평균 매출 {data['avg_sales']:,.0f}원"
                + (f", 이슈 {data['issues']}건" if data["issues"] > 0 else "")
            )
        lines.append("")
    lines.append(f"📎 총 {len(STORES)}개 점포, {len(summary)}개 지역")
    return "\n".join(lines).rstrip()


def _get_store_status_label(store):
    sales_change = store.get("sales_change", 0)
    waste_rate = store.get("waste_rate", 0)
    has_issues = bool(store.get("issues"))
    if sales_change >= 10 and waste_rate < 3 and not has_issues:
        return "우수"
    if sales_change < -5 or waste_rate > 6 or has_issues:
        return "주의"
    return "양호"


def _get_status_emoji(store):
    label = _get_store_status_label(store)
    return {"우수": "🟢", "양호": "🟡", "주의": "🔴"}.get(label, "⚪")


# ─────────────────────────────────────────────
# 인텐트 감지
# ─────────────────────────────────────────────

def detect_intent(user_message):
    msg = user_message.lower()
    promo_keywords = ["행사", "프로모션", "1+1", "2+1", "할인", "이벤트"]
    analysis_keywords = ["포지셔닝", "전략", "비교", "분석", "어떻게 하면", "왜", "원인"]
    canvas_keywords = ["캔버스", "canvas", "정리해줘", "문서로", "리포트로"]

    if any(k in msg for k in ["방문 브리핑", "통합 브리핑", "종합 현황", "오늘 브리핑"]):
        return "orchestration_briefing"
    if any(k in msg for k in ["주간 리포트", "주간 보고", "위클리 리포트", "이번 주 리포트"]):
        return "orchestration_weekly"
    if any(k in msg for k in ["멀티 분석", "종합 분석", "통합 분석", "에이전트 분석", "전체 분석"]):
        return "multi_agent_analysis"
    if any(k in msg for k in canvas_keywords):
        return "canvas_create"

    store = _find_store_in_message(user_message)
    if store:
        if any(k in msg for k in promo_keywords):
            return "store_promo_analysis"
        if any(k in msg for k in analysis_keywords):
            return "ai_analysis"
        return "ofc_store_status"

    if any(k in msg for k in promo_keywords) and re.search(r"[가-힣A-Za-z0-9]+점", user_message):
        return "store_promo_analysis"

    # 분석 키워드가 있으면 AI 분석 우선 (지역+분석 = AI 분석)
    region_with_analysis = ["어떻게 하면", "어떻게", "심각", "개선", "해결", "방안", "대책", "왜 이렇게", "원인"]
    if _region_mentioned(user_message) and any(k in msg for k in region_with_analysis):
        return "ai_analysis"

    region_store_triggers = ["점포", "매장", "현황", "지역", "권역", "몇 개", "어때"]
    if _region_mentioned(user_message) and any(k in msg for k in region_store_triggers):
        return "ofc_region_stores"

    if any(k in msg for k in ["행사 통계", "프로모션 현황", "행사 몇 개", "행사 수"]):
        return "promo_stats"
    if any(k in msg for k in ["끝난 행사", "종료된", "지난 행사", "이전 행사"]):
        return "promo_ended"
    if any(k in msg for k in ["이후", "다음에", "그 뒤에", "후속", "예정"]):
        if any(k in msg for k in ["행사", "프로모션"]):
            return "promo_upcoming"
    if any(k in msg for k in promo_keywords):
        if any(k in msg for k in ["등록", "추가", "만들"]):
            return "promo_register"
        if any(k in msg for k in ["종료", "끝나", "마감", "임박"]):
            return "promo_ending"
        if any(k in msg for k in ["시작", "새로", "신규"]):
            return "promo_starting"
        if any(k in msg for k in ["성과", "매출", "top", "순위", "많이 팔", "인기"]):
            return "promo_performance"
        return "promo_search"
    if any(k in msg for k in ["행사 중", "행사중", "지금 행사", "할인 중"]):
        return "promo_product_check"

    if any(k in msg for k in analysis_keywords):
        return "ai_analysis"

    if any(k in msg for k in ["상위", "top", "우수", "잘 되는", "매출 높"]):
        return "ofc_top_stores"
    if any(k in msg for k in ["하위", "bottom", "부진", "안 되는", "매출 낮", "매출 떨"]):
        return "ofc_bottom_stores"
    if any(k in msg for k in ["폐기율", "폐기 높", "폐기 많"]):
        return "ofc_waste"
    if any(k in msg for k in ["퀵커머스", "배달", "딜리버리"]):
        return "ofc_quick_commerce"
    if any(k in msg for k in ["이슈 목록", "이슈 현황", "문제 점포", "이슈 있는"]):
        return "ofc_issues_list"
    if any(k in msg for k in ["절차", "방법", "어떻게", "매뉴얼", "규정", "기준",
                               "체크리스트", "처리", "안내", "가이드", "알려줘"]):
        return "ofc_manual"
    if any(k in msg for k in ["이슈 등록", "문제 등록", "신고", "접수"]):
        return "ofc_issue_register"
    if any(k in msg for k in ["공지", "알림", "본사", "새 소식"]):
        return "ofc_notice"
    if any(k in msg for k in ["점포", "매장", "매출", "현황", "상태"]):
        return "ofc_store_status"

    return "general_chat"


# ─────────────────────────────────────────────
# 프로모션 매니저 봇
# ─────────────────────────────────────────────

def handle_promo_search(user_message):
    msg = user_message.lower()
    category = _extract_category(msg)
    for cat_products in PRODUCT_CATALOG.values():
        for product in cat_products:
            if product[:3].lower() in msg:
                results = search_promotion(product[:3])
                if results:
                    return _format_promo_results(results, f"'{product[:3]}' 검색 결과")
    promos = get_active_promotions(category)
    if not promos:
        if category:
            return f"현재 {category} 카테고리에 진행 중인 행사가 없습니다."
        return "현재 진행 중인 행사가 없습니다."
    if len(promos) > 30 and not category:
        return _format_promo_summary(promos, category)
    return _format_promo_list(promos, category)


def _format_promo_list(promos, category=None):
    by_type = {}
    for p in promos:
        by_type.setdefault(p["type"], []).append(p)
    today = datetime.now().strftime("%m/%d")
    cat_label = f" ({category})" if category else ""
    lines = [f"📢 *현재 진행 중인 행사{cat_label}* — {today} 기준\n"]
    for ptype, items in by_type.items():
        lines.append(f"*[ {ptype} ]* ({len(items)}건)")
        for p in items[:15]:
            end_date = datetime.strptime(p["end"], "%Y-%m-%d").strftime("%m/%d")
            extra = f" ({p.get('discount', '')} 할인)" if p.get("discount") else ""
            lines.append(f"  • {p['name']}{extra}  ~{end_date}까지")
        if len(items) > 15:
            lines.append(f"  ... 외 {len(items) - 15}건")
        lines.append("")
    ending = get_ending_soon(3)
    if ending:
        lines.append(f"⚠️ *3일 내 종료 예정* ({len(ending)}건)")
        for p in ending[:5]:
            end_date = datetime.strptime(p["end"], "%Y-%m-%d").strftime("%m/%d")
            lines.append(f"  • {p['name']} ({p['type']}) — {end_date} 종료")
    lines.append(f"\n📎 총 {len(promos)}개 행사 진행 중")
    return "\n".join(lines)


def _format_promo_summary(promos, category=None):
    cat_label = f" ({category})" if category else ""
    by_type = {}
    by_cat = {}
    for p in promos:
        by_type[p["type"]] = by_type.get(p["type"], 0) + 1
        by_cat[p["category"]] = by_cat.get(p["category"], 0) + 1
    lines = [f"📢 *현재 진행 중인 행사{cat_label}* — 총 {len(promos)}건\n"]
    lines.append("*유형별*")
    for t, cnt in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"  • {t}: {cnt}건")
    if not category:
        lines.append("\n*카테고리별*")
        for c, cnt in sorted(by_cat.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {c}: {cnt}건")
    lines.append("\n💡 특정 카테고리를 보려면 '음료 행사', '과자 행사' 처럼 물어보세요!")
    return "\n".join(lines)


def _format_promo_results(results, title):
    lines = [f"🔍 *{title}*\n"]
    for p in results[:10]:
        end_date = datetime.strptime(p["end"], "%Y-%m-%d").strftime("%m/%d")
        status_emoji = "🟢" if p["status"] == "진행중" else "🟡"
        extra = f" ({p.get('discount', '')} 할인)" if p.get("discount") else ""
        lines.append(f"{status_emoji} *{p['name']}* — {p['type']}{extra}")
        lines.append(f"  기간: {p['start']} ~ {p['end']} ({p['status']})")
    if len(results) > 10:
        lines.append(f"\n... 외 {len(results) - 10}건")
    return "\n".join(lines)


def handle_promo_product_check(user_message):
    msg = user_message.replace("행사 중이야?", "").replace("행사중이야?", "").replace("지금 행사", "").replace("할인 중", "").strip()
    keywords = [w for w in msg.split() if len(w) >= 2]
    results = []
    for kw in keywords:
        results.extend(search_promotion(kw))
    seen = set()
    unique = [p for p in results if p["id"] not in seen and not seen.add(p["id"])]
    if not unique:
        keyword_str = ", ".join(keywords) if keywords else user_message
        return f"🔍 *'{keyword_str}'* 관련 현재 진행 중인 행사가 없습니다.\n\n💡 다른 키워드로 검색해보세요."
    return _format_promo_results(unique, "검색 결과")


def handle_promo_upcoming(user_message):
    promos = get_upcoming_promotions()
    if not promos:
        return "📅 현재 예정된 신규 행사가 없습니다."
    category = _extract_category(user_message)
    if category:
        promos = [p for p in promos if p["category"] == category]
    lines = [f"📅 *예정된 신규 행사* ({len(promos)}건)\n"]
    for p in promos[:20]:
        start_date = datetime.strptime(p["start"], "%Y-%m-%d").strftime("%m/%d")
        end_date = datetime.strptime(p["end"], "%Y-%m-%d").strftime("%m/%d")
        extra = f" ({p.get('discount', '')} 할인)" if p.get("discount") else ""
        lines.append(f"🟡 *{p['name']}* — {p['type']}{extra}")
        lines.append(f"  {start_date} 시작 → {end_date} 종료")
    if len(promos) > 20:
        lines.append(f"\n... 외 {len(promos) - 20}건")
    return "\n".join(lines)


def handle_promo_starting(user_message):
    promos = get_starting_soon(3)
    if not promos:
        return "✅ 3일 내 새로 시작하는 행사가 없습니다."
    lines = [f"🆕 *3일 내 시작 예정* ({len(promos)}건)\n"]
    for p in promos:
        start_date = datetime.strptime(p["start"], "%Y-%m-%d").strftime("%m/%d")
        extra = f" ({p.get('discount', '')} 할인)" if p.get("discount") else ""
        lines.append(f"• {p['name']} ({p['type']}{extra}) — *{start_date} 시작*")
    return "\n".join(lines)


def handle_promo_ending(user_message):
    ending = get_ending_soon(3)
    if not ending:
        return "✅ 3일 내 종료 예정인 행사가 없습니다."
    lines = [f"⚠️ *3일 내 종료 예정* ({len(ending)}건)\n"]
    for p in ending[:15]:
        end_date = datetime.strptime(p["end"], "%Y-%m-%d").strftime("%m/%d")
        lines.append(f"• {p['name']} ({p['type']}) — *{end_date} 종료*")
    return "\n".join(lines)


def handle_promo_ended(user_message):
    ended = get_ended_promotions(14)
    if not ended:
        return "최근 2주 내 종료된 행사가 없습니다."
    lines = [f"📋 *최근 종료된 행사* ({len(ended)}건)\n"]
    for p in ended[:15]:
        end_date = datetime.strptime(p["end"], "%Y-%m-%d").strftime("%m/%d")
        lines.append(f"• {p['name']} ({p['type']}) — {end_date} 종료")
    return "\n".join(lines)


def handle_promo_stats(user_message):
    stats = get_promo_stats()
    lines = [
        "📊 *프로모션 현황 통계*\n",
        f"• 진행 중: *{stats['active_count']}건*",
        f"• 예정: *{stats['upcoming_count']}건*",
        f"• 최근 종료: *{stats['ended_count']}건*\n",
        "*유형별 (진행 중)*",
    ]
    for t, cnt in sorted(stats["by_type"].items(), key=lambda x: x[1], reverse=True):
        lines.append(f"  • {t}: {cnt}건")
    lines.append("\n*카테고리별 (진행 중)*")
    for c, cnt in sorted(stats["by_category"].items(), key=lambda x: x[1], reverse=True):
        lines.append(f"  • {c}: {cnt}건")
    return "\n".join(lines)


def handle_promo_performance(user_message):
    active = get_active_promotions()
    category = _extract_category(user_message)
    if category:
        active = [p for p in active if p["category"] == category]
    import random as _rand
    _rand.seed(len(user_message))
    ranked = sorted([(p, _rand.randint(10, 80)) for p in active], key=lambda x: x[1], reverse=True)
    n = _extract_number(user_message, 10)
    cat_label = f" ({category})" if category else ""
    lines = [f"📊 *행사 상품 매출 Top {n}{cat_label}*\n"]
    for i, (p, avg) in enumerate(ranked[:n], 1):
        lines.append(f"{i}. {p['name']} ({p['type']}) — 일평균 {avg}개")
    return "\n".join(lines)


# ─────────────────────────────────────────────
# OFC 스마트 어시스턴트
# ─────────────────────────────────────────────

def handle_ofc_manual(user_message):
    keywords = ["유통기한", "폐기", "발주", "취소", "위생", "점검", "체크리스트",
                "계약", "갱신", "담배", "성인", "인증", "안심", "운영", "지원금",
                "퀵커머스", "배송", "프레시", "냉장", "냉동", "온도", "교통카드",
                "충전", "환불", "CCTV", "보안", "알바", "채용", "근무", "인력",
                "현금", "정산", "시재", "진열", "상품", "클레임", "고객", "불만",
                "택배", "재고", "실사", "영업지역", "보호", "화재", "재난",
                "소방", "멤버십", "포인트", "적립"]
    found = [kw for kw in keywords if kw in user_message]
    if not found:
        found = [user_message]
    results = []
    for kw in found:
        results.extend(search_manual(kw))
    seen = set()
    unique = [m for m in results if m["id"] not in seen and not seen.add(m["id"])]

    if not unique:
        ai_answer = _call_llm(
            "당신은 GS25 편의점 운영 전문가입니다. 간결하게 답변하세요. 같은 내용 반복 금지.",
            user_message)
        return _to_slack_mrkdwn(f"📖 *AI 답변*\n\n{ai_answer}\n\n⚠️ 매뉴얼에서 정확한 내용을 찾지 못해 AI가 답변했습니다.")

    manual_text = "\n\n".join([f"[{m['title']}]\n{m['content']}" for m in unique[:3]])
    system_prompt = "당신은 GS25 매뉴얼 전문가입니다. 매뉴얼 내용을 바탕으로 핵심만 간결하게 답변하세요. 같은 내용 반복 금지."
    ai_answer = _call_llm(system_prompt, f"매뉴얼:\n{manual_text}\n\n질문: {user_message}")
    source_titles = ", ".join([m["title"] for m in unique[:3]])
    return _to_slack_mrkdwn(f"📖 *매뉴얼 답변*\n\n{ai_answer}") + f"\n\n📎 출처: {source_titles}"


def handle_ofc_store_status(user_message):
    # 여러 점포 매칭 확인
    all_matches = _find_all_stores_in_message(user_message)
    if len(all_matches) == 1:
        return _format_store_detail(all_matches[0])
    if len(all_matches) >= 2:
        # 같은 이름이 여러 지역에 있으면 선택지 표시
        lines = [f"🔍 *'{all_matches[0]['name']}'* 이름의 점포가 {len(all_matches)}개 있습니다. 어떤 점포를 확인할까요?\n"]
        for i, s in enumerate(all_matches[:5], 1):
            emoji = _get_status_emoji(s)
            lines.append(
                f"{i}. {emoji} *{s['name']}* ({s['region']})"
                f" — 매출 {s['daily_sales']:,.0f}원 | 폐기율 {s['waste_rate']}%"
            )
        lines.append("\n💡 지역을 포함해서 다시 물어보세요! 예: \"목포 상동3호점\"")
        return "\n".join(lines)
    # 단일 점포 검색
    store = _find_store_in_message(user_message)
    if store:
        return _format_store_detail(store)
    ofc = _extract_ofc(user_message)
    stores = get_stores_by_ofc(ofc) if ofc else get_stores_by_ofc("김지원")
    return _format_store_summary(stores, ofc or "김지원")


def _format_store_detail(s):
    status_emoji = _get_status_emoji(s)
    status_label = _get_store_status_label(s)
    region = s.get("region", "").replace(" ", "+")
    location = s.get("location", "")
    map_query = f"GS25+{region}+{location}".replace(" ", "+")
    map_url = f"https://www.google.com/maps/search/{map_query}"
    lines = [
        f"🏪 *{s['name']}* ({s['region']}) {status_emoji} {status_label}\n",
        f"• 일 매출: {s['daily_sales']:,.0f}원 (전주 대비 {s['sales_change']:+.1f}%)",
        f"• 폐기율: {s['waste_rate']}%",
        f"• 행사 발주 이행률: {s['promo_order_rate']}%",
        f"• 퀵커머스 매출: {s['quick_commerce_sales']:,.0f}원" if s['quick_commerce_sales'] > 0 else "• 퀵커머스: 미운영",
        f"• OFC: {s['ofc']}  • 직원: {s['staff_count']}명  • 개점: {s['open_year']}년",
        f"• 📍 <{map_url}|지도에서 보기>",
    ]
    if s["issues"]:
        lines.append(f"\n⚠️ *이슈*")
        for issue in s["issues"]:
            lines.append(f"  • {issue}")
    else:
        lines.append("• 이슈: 없음")
    return "\n".join(lines)


def _format_store_summary(stores, ofc_name):
    if not stores:
        return f"OFC {ofc_name} 담당 점포가 없습니다."
    avg_sales = sum(s["daily_sales"] for s in stores) / len(stores)
    avg_waste = sum(s["waste_rate"] for s in stores) / len(stores)
    best = max(stores, key=lambda s: s["sales_change"])
    worst = min(stores, key=lambda s: s["sales_change"])
    lines = [
        f"📊 *담당 점포 현황* — OFC {ofc_name} ({len(stores)}개 점포)\n",
        f"• 평균 일매출: {avg_sales:,.0f}원",
        f"• 평균 폐기율: {avg_waste:.1f}%\n",
        f"{_get_status_emoji(best)} *상위*: {best['name']} ({best['region']}, 매출 {best['sales_change']:+.1f}%)",
        f"{_get_status_emoji(worst)} *하위*: {worst['name']} ({worst['region']}, 매출 {worst['sales_change']:+.1f}%)",
    ]
    issue_stores = [s for s in stores if s["issues"]]
    if issue_stores:
        lines.append(f"\n⚠️ *미처리 이슈* ({len(issue_stores)}개 점포)")
        for s in issue_stores[:5]:
            lines.append(f"• {_get_status_emoji(s)} {s['name']}: {s['issues'][0]}")
    return "\n".join(lines)


def handle_ofc_region_stores(user_message):
    """지역별 점포 현황 — 도 단위/권역/시/구/동네 모두 지원"""

    kw = _extract_region_filter_keyword(user_message)
    if kw is None and _is_nationwide_region_query(user_message):
        return _format_nationwide_region_summary()
    if kw is None:
        return _format_nationwide_region_summary()

    # 도 단위 (전라도, 경상도, 충청도, 호남, 영남 등)
    if kw in _PROVINCE_MAP:
        stores = _get_stores_by_province(kw)
        title = f"{kw} 점포 현황"
    # 권역 (수도권, 영남권 등)
    elif kw in _MACRO_AREAS:
        stores = [s for s in STORES if _get_macro_area(s.get("region")) == kw]
        title = f"{kw} 점포 현황"
    # 일반 지역 검색
    else:
        stores = get_stores_by_region(kw)
        title = f"{kw} 점포 현황"

    if not stores:
        suggestions = _get_region_suggestions(kw, limit=5)
        hint = f"\n유사 지역: {', '.join(suggestions)}" if suggestions else ""
        return f"'{kw}' 지역에 해당하는 점포가 없습니다.{hint}"

    # 매출 내림차순
    stores = sorted(stores, key=lambda s: s["daily_sales"], reverse=True)

    # 상태 집계
    good = sum(1 for s in stores if _get_store_status_label(s) == "우수")
    normal = sum(1 for s in stores if _get_store_status_label(s) == "양호")
    warn = sum(1 for s in stores if _get_store_status_label(s) == "주의")

    lines = [
        f"📍 *{title}* ({len(stores)}개)",
        f"📊 🟢 {good}개 | 🟡 {normal}개 | 🔴 {warn}개\n",
    ]

    # 20개 초과: 세부 지역별 요약
    if len(stores) > 50:
        grouped = {}
        for s in stores:
            grouped.setdefault(s["region"], []).append(s)

        for region, items in sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True):
            g = sum(1 for s in items if _get_store_status_label(s) == "우수")
            w = sum(1 for s in items if _get_store_status_label(s) == "주의")
            y = len(items) - g - w
            avg_sales = int(sum(s["daily_sales"] for s in items) / len(items))
            issue_cnt = sum(1 for s in items if s.get("issues"))
            lines.append(f"📍 *{region}* ({len(items)}개) — 🟢{g} 🟡{y} 🔴{w}")
            lines.append(f"  평균 매출 {avg_sales:,.0f}원 | 이슈 {issue_cnt}건")

            warn_items = sorted(
                [s for s in items if _get_store_status_label(s) == "주의"],
                key=lambda s: s.get("waste_rate", 0), reverse=True
            )[:2]
            if warn_items:
                parts = []
                for s in warn_items:
                    reasons = []
                    if s.get("waste_rate", 0) > 6:
                        reasons.append(f"폐기율 {s['waste_rate']}%")
                    if s.get("issues"):
                        reasons.append(s["issues"][0])
                    if s.get("sales_change", 0) < -5:
                        reasons.append(f"전주 {s['sales_change']:+.1f}%")
                    parts.append(f"{s['name']}({', '.join(reasons) if reasons else '주의'})")
                lines.append(f"  ⚠️ {', '.join(parts)}")
            lines.append("")
        return "\n".join(lines).rstrip()

    # 20개 이하: 상세 리스트 (상태별 그룹핑)
    by_status = {"우수": [], "양호": [], "주의": []}
    for s in stores:
        by_status[_get_store_status_label(s)].append(s)

    for label, header in [("우수", "🟢 우수"), ("양호", "🟡 양호"), ("주의", "🔴 주의")]:
        items = sorted(by_status[label], key=lambda x: x["daily_sales"], reverse=True)
        if not items:
            continue
        lines.append(header)
        for s in items:
            extra = ""
            if label == "주의":
                if s.get("waste_rate", 0) > 6:
                    extra = " ⚠️ 폐기율 초과"
                elif s.get("issues"):
                    extra = f" ⚠️ 이슈: {s['issues'][0]}"
                elif s.get("sales_change", 0) < -5:
                    extra = " ⚠️ 매출 하락"
            issues_text = ", ".join(s.get("issues", [])[:2]) if s.get("issues") else "없음"
            lines.append(
                f"• {_get_status_emoji(s)} {s['name']} ({s['region']})\n"
                f"  매출 {s['daily_sales']:,.0f}원 (전주 {s['sales_change']:+.1f}%) | 폐기율 {s['waste_rate']}% | 이슈: {issues_text}{extra}"
            )
        lines.append("")
    return "\n".join(lines).rstrip()


def handle_ofc_top_stores(user_message):
    n = _extract_number(user_message, 10)
    ofc = _extract_ofc(user_message)
    stores = get_top_stores(n, ofc)
    label = f"OFC {ofc} " if ofc else ""
    lines = [f"🏆 *{label}매출 상위 Top {n}*\n"]
    for i, s in enumerate(stores, 1):
        lines.append(f"{i}. {_get_status_emoji(s)} {s['name']} ({s['region']}) — {s['daily_sales']:,.0f}원 ({s['sales_change']:+.1f}%) | 폐기율 {s['waste_rate']}%")
    return "\n".join(lines)


def handle_ofc_bottom_stores(user_message):
    n = _extract_number(user_message, 10)
    ofc = _extract_ofc(user_message)
    stores = get_bottom_stores(n, ofc)
    label = f"OFC {ofc} " if ofc else ""
    lines = [f"📉 *{label}매출 하위 {n}개 점포*\n"]
    for i, s in enumerate(stores, 1):
        lines.append(f"{i}. {_get_status_emoji(s)} {s['name']} ({s['region']}) — {s['daily_sales']:,.0f}원 ({s['sales_change']:+.1f}%) | 폐기율 {s['waste_rate']}%")
    return "\n".join(lines)


def handle_ofc_waste(user_message):
    threshold = 5.0
    ofc = _extract_ofc(user_message)
    stores = sorted(get_high_waste_stores(threshold, ofc), key=lambda s: s["waste_rate"], reverse=True)
    label = f"OFC {ofc} " if ofc else ""
    lines = [f"🗑️ *{label}폐기율 {threshold}% 초과 점포* ({len(stores)}개)\n"]
    for s in stores[:15]:
        lines.append(f"• {_get_status_emoji(s)} {s['name']} ({s['region']}) — 폐기율 *{s['waste_rate']}%* | 매출 {s['daily_sales']:,.0f}원")
    if len(stores) > 15:
        lines.append(f"... 외 {len(stores) - 15}개 점포")
    return "\n".join(lines)


def handle_ofc_quick_commerce(user_message):
    ofc = _extract_ofc(user_message)
    stores = get_quick_commerce_stores(ofc)
    total_quick = sum(s["quick_commerce_sales"] for s in stores)
    avg_quick = total_quick / len(stores) if stores else 0
    label = f"OFC {ofc} " if ofc else ""
    lines = [
        f"🛵 *{label}퀵커머스 현황* ({len(stores)}개 점포)\n",
        f"• 총 매출: {total_quick:,.0f}원/일 | 평균: {avg_quick:,.0f}원/일\n",
        "*매출 상위*",
    ]
    for s in sorted(stores, key=lambda s: s["quick_commerce_sales"], reverse=True)[:5]:
        lines.append(f"  • {s['name']} — {s['quick_commerce_sales']:,.0f}원 ({s['quick_commerce_change']:+.1f}%)")
    return "\n".join(lines)


def handle_ofc_issues_list(user_message):
    ofc = _extract_ofc(user_message)
    stores = get_store_issues(ofc)
    label = f"OFC {ofc} " if ofc else ""
    lines = [f"⚠️ *{label}미처리 이슈 현황* ({len(stores)}개 점포)\n"]
    for s in stores[:20]:
        for issue in s["issues"]:
            lines.append(f"• {_get_status_emoji(s)} *{s['name']}* ({s['region']}): {issue}")
    if len(stores) > 20:
        lines.append(f"... 외 {len(stores) - 20}개 점포")
    return "\n".join(lines)


def handle_ofc_issue_register(user_message):
    return f"✅ *이슈가 등록되었습니다*\n• 내용: {user_message}\n• 등록일: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n• 상태: 접수됨"


def handle_ofc_notice(user_message):
    return (
        "📢 *이번 주 주요 공지* (3월 4주차)\n\n"
        "*1. 재고처리한도 상향* → 점포당 연 108만원\n"
        "*2. 하절기 위생 강화* → 프레시푸드 온도 점검 매일 3회\n"
        "*3. 퀵커머스 처리 시간 단축* → 4/1부터 10분→7분\n"
        "*4. GS&POINT 더블 적립* → 4/1~4/7 포인트 2배\n"
        "*5. 신규 프레시푸드 출시* → 시그니처 도시락 5종 4/3"
    )


# ─────────────────────────────────────────────
# 오케스트레이션
# ─────────────────────────────────────────────

def handle_orchestration_briefing(user_message):
    ofc = _extract_ofc(user_message) or "김지원"
    struggling = get_struggling_stores(ofc)
    ending = get_ending_soon(3)
    starting = get_starting_soon(3)
    active = get_active_promotions()
    all_stores = get_stores_by_ofc(ofc)
    data = {
        "OFC": ofc, "담당점포수": len(all_stores),
        "주의점포": [{"name": s["name"], "매출변동": s["sales_change"], "폐기율": s["waste_rate"], "이슈": s["issues"]} for s in struggling[:5]],
        "종료임박행사": len(ending), "신규시작행사": len(starting), "진행중행사수": len(active),
    }
    system_prompt = "당신은 GS25 OFC 브리핑 AI입니다. 데이터 기반으로 오늘 브리핑을 작성하세요. 액션 아이템 3~5개, 같은 내용 반복 금지, Slack 스타일."
    ai_result = _call_llm(system_prompt, json.dumps(data, ensure_ascii=False, indent=2))
    result = _to_slack_mrkdwn(ai_result)
    result += f"\n\n📎 프로모션 {len(active)}건 | 점포 {len(all_stores)}개"
    return result


def handle_orchestration_weekly(user_message):
    ofc = _extract_ofc(user_message) or "김지원"
    all_stores = get_stores_by_ofc(ofc)
    active = get_active_promotions()
    data = {
        "OFC": ofc, "점포수": len(all_stores),
        "평균매출": f"{sum(s['daily_sales'] for s in all_stores) / len(all_stores):,.0f}원" if all_stores else "0원",
        "주의점포수": len(get_struggling_stores(ofc)),
        "진행중행사": len(active),
    }
    ai_result = _call_llm("GS25 OFC 주간 리포트 AI. 핵심 수치→분석→액션 순서. Slack 스타일.", json.dumps(data, ensure_ascii=False))
    return _to_slack_mrkdwn(ai_result)


# ─────────────────────────────────────────────
# 일반 대화 / 캔버스 / 분석
# ─────────────────────────────────────────────

def handle_general_chat(user_message, thread_history=None):
    system = "당신은 GS25 편의점 AI 어시스턴트입니다. 프로모션/매뉴얼/점포 현황 도움. 짧고 친절하게."
    if thread_history:
        context = "\n".join([f"{'봇' if h['role']=='assistant' else '사용자'}: {h['text'][:300]}" for h in thread_history[-5:]])
        user_prompt = f"이전 대화:\n{context}\n\n현재 질문: {user_message}"
    else:
        user_prompt = user_message
    result = _call_llm(system, user_prompt)
    return _to_slack_mrkdwn(result)


def _normalize_canvas_markdown(content):
    if not content:
        return ""
    lines = []
    for line in content.splitlines():
        stripped = line.lstrip()
        if re.match(r"^\d+\.\s+", stripped):
            stripped = re.sub(r"^\d+\.\s+", "- ", stripped)
        elif re.match(r"^[-*•]\s+", stripped):
            stripped = re.sub(r"^[-*•]\s+", "- ", stripped)
        lines.append(stripped if stripped.startswith("- ") else line.strip())
    return "\n".join(lines)


def create_canvas(client, channel, content, title):
    try:
        result = client.api_call("canvases.create", json={"title": title, "document_content": {"type": "markdown", "markdown": content}})
        canvas_id = result["canvas_id"]
        # DM 채널(D로 시작)은 access.set 불가 — 채널(C로 시작)만 권한 설정
        if channel and channel.startswith("C"):
            try:
                client.api_call("canvases.access.set", json={"canvas_id": canvas_id, "access_level": "read", "channel_ids": [channel]})
            except Exception:
                pass
        team_id = client.auth_test()["team_id"]
        canvas_url = f"https://app.slack.com/docs/{team_id}/{canvas_id}"
        client.chat_postMessage(channel=channel, text=f"📋 Canvas 리포트가 생성되었습니다!\n{canvas_url}", unfurl_links=True, unfurl_media=True)
        return canvas_url
    except Exception as e:
        return f"캔버스 생성 실패: {str(e)}"


def handle_canvas_create(user_message, slack_client=None, channel=None, previous_bot_reply=None, all_bot_replies=None):
    if slack_client is None or channel is None:
        return "캔버스 생성에 필요한 Slack 클라이언트 정보가 없습니다."

    msg = user_message.strip()
    cleaned = msg
    for kw in ["캔버스로", "캔버스", "canvas", "정리해줘", "문서로", "리포트로", "만들어줘", "작성해줘", "만들어", "정리해"]:
        cleaned = cleaned.replace(kw, " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # 패턴 감지
    has_all = any(k in msg for k in ["전체", "모든", "다", "전부", "지금까지", "대화 내용", "대화내용", "정리"])
    has_summary = any(k in msg for k in ["요약", "핵심만", "간단히"])
    has_number = re.search(r"(\d+)개", msg) or re.search(r"마지막\s*(\d+)", msg)

    # 패턴 1: 전체 대화 캔버스
    if has_all and all_bot_replies:
        content = "\n\n---\n\n".join(all_bot_replies)
        title = f"GS Retail 전체 분석 리포트 - {datetime.now().strftime('%m/%d %H:%M')}"

    # 패턴 2: 요약 캔버스 (LLM으로 요약)
    elif has_summary and all_bot_replies:
        all_text = "\n\n".join(all_bot_replies)
        summary = _call_llm(
            "아래 분석 내용들을 핵심만 요약해서 Slack Canvas용 리포트로 작성하세요. 번호목록 금지, 불릿(-)만 사용. 같은 내용 반복 금지.",
            all_text[:3000]
        )
        content = _to_slack_mrkdwn(summary)
        title = f"GS Retail 요약 리포트 - {datetime.now().strftime('%m/%d %H:%M')}"

    # 패턴 3: 최근 N개 캔버스
    elif has_number and all_bot_replies:
        match = re.search(r"(\d+)", msg)
        n = min(int(match.group(1)), len(all_bot_replies)) if match else 1
        content = "\n\n---\n\n".join(all_bot_replies[-n:])
        title = f"GS Retail 최근 {n}건 리포트 - {datetime.now().strftime('%m/%d %H:%M')}"

    # 패턴 4: 짧은 요청 → 바로 위 봇 답변 1개
    elif len(cleaned) < 10 and previous_bot_reply:
        content = previous_bot_reply
        title = f"GS Retail Canvas - {datetime.now().strftime('%m/%d %H:%M')}"

    # 패턴 5: 구체적 요청 → 여러 주제면 분리 처리
    else:
        if not cleaned:
            cleaned = "지역별 점포 현황 리포트 작성"
        # "과", "랑", "하고", "그리고"로 여러 주제 분리
        parts = re.split(r"(?:와|과|랑|하고|그리고|,|&)\s*", cleaned)
        parts = [p.strip() for p in parts if len(p.strip()) >= 2]
        if len(parts) > 1:
            results = []
            for part in parts:
                # 각 파트에 "점포현황" 등 트리거 보충
                if not any(k in part for k in ["점포", "현황", "행사", "프로모션", "매출"]):
                    part = part + " 점포 현황"
                r, _ = route_and_execute(part, allow_canvas=False)
                results.append(r)
            content = "\n\n---\n\n".join(results)
        else:
            content, _ = route_and_execute(cleaned, allow_canvas=False)
        title = f"GS Retail Canvas - {datetime.now().strftime('%m/%d %H:%M')}"

    content = _normalize_canvas_markdown(content)
    canvas_result = create_canvas(slack_client, channel, content, title)
    if canvas_result.startswith("http"):
        return f"📄 Canvas로 정리했습니다.\n{canvas_result}"
    return canvas_result


def handle_store_promo_analysis(user_message):
    store = _find_store_in_message(user_message)
    if not store:
        return "점포명을 찾지 못했습니다. 예: '역삼역점 프로모션 분석해줘'"
    active_promos = get_active_promotions()[:20]
    payload = {"질문": user_message, "점포": {"name": store["name"], "region": store["region"], "daily_sales": store["daily_sales"], "sales_change": store["sales_change"], "waste_rate": store["waste_rate"], "promo_order_rate": store["promo_order_rate"]}, "진행중프로모션_샘플": active_promos}
    ai_result = _call_llm("GS25 점포 프로모션 분석가. 매출/발주율/폐기율 근거로 행사 유형 분석, 추천 상품 3~5개, 액션 3개. 반복 금지.", json.dumps(payload, ensure_ascii=False, indent=2))
    return _to_slack_mrkdwn(f"🏪 *점포 프로모션 연관 분석*\n\n{ai_result}")


def handle_ai_analysis(user_message, thread_history=None):
    store = _find_store_in_message(user_message)
    region = _extract_region(user_message)
    if store:
        related = [store]
    elif region:
        related = get_stores_by_region(region)[:15]
    else:
        related = STORES[:15]
    thread_context = ""
    if thread_history:
        thread_context = "\n".join([f"{'봇' if h['role']=='assistant' else '사용자'}: {h['text'][:300]}" for h in thread_history[-5:]])
    context = {"질문": user_message, "이전대화": thread_context, "점포현황": [{"name": s["name"], "region": s["region"], "daily_sales": s["daily_sales"], "sales_change": s["sales_change"], "waste_rate": s["waste_rate"]} for s in related]}
    system_prompt = (
        "당신은 GS25 운영 전략 AI입니다.\n"
        "규칙:\n"
        "- 이전 대화가 있으면 반드시 그 맥락을 이어서 답변\n"
        "- 사용자가 '전파', '적용', '다른 지역'을 언급하면 이전 분석의 전략을 구체적 지역에 매핑\n"
        "- '어디', '어떤 지역'을 물으면 데이터 기반으로 유사한 조건의 지역을 추천\n"
        "- 구체적 지역명 + 이유를 함께 제시\n"
        "- 같은 내용 반복 금지, Slack 스타일(*볼드*, • 글머리)"
    )
    ai_result = _call_llm(system_prompt, json.dumps(context, ensure_ascii=False, indent=2))
    return _to_slack_mrkdwn(f"🧠 *AI 심층 분석*\n\n{ai_result}")


def handle_multi_agent_analysis(user_message, slack_client=None, channel=None, thread_ts=None):
    """멀티 에이전트 오케스트레이션 — 3개 페르소나 결과를 하나로 합쳐 반환"""

    region = _extract_region(user_message) or _extract_region_filter_keyword(user_message)
    if not region:
        region = "서울"

    lines = []

    # --- 에이전트 1: 프로모션 매니저 ---
    active = get_active_promotions()
    ending = get_ending_soon(3)
    starting = get_starting_soon(3)
    promo_by_cat = {}
    for p in active:
        promo_by_cat[p["category"]] = promo_by_cat.get(p["category"], 0) + 1
    top_cats = sorted(promo_by_cat.items(), key=lambda x: x[1], reverse=True)[:3]

    lines.append("📦 *[프로모션 매니저]*")
    lines.append(f"현재 진행 중 행사 *{len(active)}건*")
    lines.append(f"• 종료 임박: {len(ending)}건 | 신규 시작: {len(starting)}건")
    lines.append(f"• 인기 카테고리: {', '.join([c + '(' + str(n) + '건)' for c,n in top_cats])}")
    if ending:
        lines.append(f"⚠️ 종료 임박: {', '.join([p['name'] for p in ending[:3]])}")
    lines.append("")

    # --- 에이전트 2: OFC 어시스턴트 ---
    stores = get_stores_by_region(region)
    if not stores:
        stores = STORES[:50]
    good = sum(1 for s in stores if _get_store_status_label(s) == "우수")
    warn = sum(1 for s in stores if _get_store_status_label(s) == "주의")
    normal = len(stores) - good - warn
    avg_sales = sum(s["daily_sales"] for s in stores) / len(stores) if stores else 0
    avg_waste = sum(s["waste_rate"] for s in stores) / len(stores) if stores else 0
    issue_stores = [s for s in stores if s.get("issues")]

    lines.append(f"🏪 *[OFC 어시스턴트]* — {region} 분석")
    lines.append(f"총 *{len(stores)}개* 점포 | 🟢{good} 🟡{normal} 🔴{warn}")
    lines.append(f"• 평균 매출: {avg_sales:,.0f}원 | 평균 폐기율: {avg_waste:.1f}%")
    lines.append(f"• 이슈 점포: {len(issue_stores)}개")
    if warn > 0:
        worst = sorted([s for s in stores if _get_store_status_label(s) == "주의"],
                       key=lambda s: s["waste_rate"], reverse=True)[:3]
        worst_names = [s["name"] + "(폐기율 " + str(s["waste_rate"]) + "%)" for s in worst]
        lines.append(f"⚠️ 주의 점포: {', '.join(worst_names)}")
    lines.append("")

    # --- 에이전트 3: 전략 어드바이저 (LLM) ---
    lines.append("🧠 *[전략 어드바이저]* — 종합 전략 수립")
    lines.append("")

    context = {
        "지역": region,
        "프로모션": {"진행중": len(active), "종료임박": len(ending), "인기카테고리": [c for c,n in top_cats]},
        "점포": {"총수": len(stores), "우수": good, "양호": normal, "주의": warn,
                "평균매출": f"{avg_sales:,.0f}원", "평균폐기율": f"{avg_waste:.1f}%"},
    }

    strategy = _call_llm(
        "당신은 GS25 수석 전략 어드바이저입니다. 프로모션 데이터와 점포 실적을 종합해 심층 전략 리포트를 작성하세요.\n\n"
        "반드시 아래 5개 섹션을 포함하세요:\n"
        "1. 📊 현황 진단 — 프로모션과 점포 데이터의 핵심 수치를 연결해서 진단 (예: 행사 이행률이 낮은 점포의 매출 하락 상관관계)\n"
        "2. ⚠️ 리스크 분석 — 폐기율 초과, 매출 하락, 미처리 이슈 점포의 구체적 위험 요인\n"
        "3. 💡 핵심 인사이트 — 데이터에서 발견한 3~4개의 의미 있는 패턴 (숫자 근거 포함)\n"
        "4. 🎯 실행 전략 — 단기(이번 주)/중기(이번 달)/장기(분기) 3단계로 구분한 액션 플랜\n"
        "5. 📈 기대 효과 — 각 전략 실행 시 예상되는 정량적 개선 효과\n\n"
        "규칙:\n"
        "- 모든 주장에 데이터 수치를 근거로 제시\n"
        "- 점포명을 구체적으로 언급\n"
        "- 같은 내용 반복 금지\n"
        "- Slack 스타일(*볼드*, • 글머리)",
        json.dumps(context, ensure_ascii=False, indent=2)
    )
    strategy = _to_slack_mrkdwn(strategy)
    lines.append(strategy)

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 메인 라우터
# ─────────────────────────────────────────────

def route_and_execute(user_message, slack_client=None, channel=None, allow_canvas=True, previous_bot_reply=None, all_bot_replies=None, thread_history=None, thread_ts=None):
    intent = detect_intent(user_message)
    if not allow_canvas and intent == "canvas_create":
        intent = "general_chat"

    handler_map = {
        "orchestration_briefing": handle_orchestration_briefing,
        "orchestration_weekly": handle_orchestration_weekly,
        "multi_agent_analysis": lambda msg: handle_multi_agent_analysis(msg, slack_client=slack_client, channel=channel, thread_ts=thread_ts),
        "canvas_create": lambda msg: handle_canvas_create(msg, slack_client=slack_client, channel=channel, previous_bot_reply=previous_bot_reply, all_bot_replies=all_bot_replies),
        "store_promo_analysis": handle_store_promo_analysis,
        "ai_analysis": lambda msg: handle_ai_analysis(msg, thread_history=thread_history),
        "promo_search": handle_promo_search,
        "promo_product_check": handle_promo_product_check,
        "promo_upcoming": handle_promo_upcoming,
        "promo_starting": handle_promo_starting,
        "promo_ending": handle_promo_ending,
        "promo_ended": handle_promo_ended,
        "promo_stats": handle_promo_stats,
        "promo_performance": handle_promo_performance,
        "ofc_manual": handle_ofc_manual,
        "ofc_store_status": handle_ofc_store_status,
        "ofc_region_stores": handle_ofc_region_stores,
        "ofc_top_stores": handle_ofc_top_stores,
        "ofc_bottom_stores": handle_ofc_bottom_stores,
        "ofc_waste": handle_ofc_waste,
        "ofc_quick_commerce": handle_ofc_quick_commerce,
        "ofc_issues_list": handle_ofc_issues_list,
        "ofc_issue_register": handle_ofc_issue_register,
        "ofc_notice": handle_ofc_notice,
        "general_chat": lambda msg: handle_general_chat(msg, thread_history=thread_history),
    }

    handler = handler_map.get(intent, handle_general_chat)
    return handler(user_message), intent