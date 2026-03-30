"""
mock_data.py — GS Retail 데모용 확장 더미 데이터
프로모션 300건 / 점포 500개 (지역별 매칭) / 매뉴얼 20건
실서비스 전환 시 Jira API / Confluence API / DB 조회로 교체
"""

import random
from datetime import datetime, timedelta

random.seed(42)

# ─────────────────────────────────────────────
# 기초 데이터 사전
# ─────────────────────────────────────────────

PRODUCT_CATALOG = {
    "음료": [
        "코카콜라 500ml", "코카콜라 제로 500ml", "펩시 500ml", "제로펩시 500ml",
        "스프라이트 500ml", "환타 오렌지 500ml", "포카리스웨트 340ml", "게토레이 600ml",
        "바나나우유 240ml", "초코우유 300ml", "딸기우유 240ml", "커피우유 300ml",
        "TOP 아메리카노 275ml", "카페라떼 300ml", "레쓰비 캔커피", "조지아 크래프트",
        "콘트라베이스 블랙", "핫식스 250ml", "레드불 250ml", "몬스터에너지 355ml",
        "비타500 100ml", "박카스 120ml", "삼다수 500ml", "삼다수 2L",
        "오렌지주스 300ml", "사과주스 300ml", "포도주스 300ml", "자몽주스 350ml",
        "밀키스 250ml", "트레비 500ml", "웰치스 포도 355ml", "데미소다 250ml",
        "토레타 240ml", "실론티 500ml", "아이시스 500ml", "쿨피스 500ml",
        "요구르트 65ml", "야쿠르트 라이트", "매일 두유 190ml", "서울우유 초코 200ml",
    ],
    "과자": [
        "허니버터칩", "꼬북칩 초코맛", "꼬북칩 콘스프맛", "새우깡", "감자깡",
        "양파링", "포카칩 오리지널", "포카칩 사워크림", "프링글스 오리지널",
        "프링글스 사워크림", "꿀꽈배기", "초코파이", "빈츠", "칙촉",
        "오레오", "에이스", "마가렛트", "카스타드", "몽쉘 통통",
        "칸쵸", "버터링", "다이제", "고래밥", "맛동산",
        "홈런볼", "빼빼로 초코", "빼빼로 아몬드", "콘초", "초코송이",
        "쫀드기", "누네띠네", "웨하스", "참크래커", "리츠",
        "오징어땅콩", "바나나킥", "치토스", "도리토스 나초", "썬칩",
        "꽃게랑", "자가비", "스윙칩", "예감", "오사쯔",
    ],
    "라면": [
        "불닭볶음면", "신라면", "진라면 매운맛", "진라면 순한맛",
        "너구리", "안성탕면", "짜파게티", "짜왕", "팔도비빔면",
        "삼양라면", "열라면", "김치라면", "튀김우동", "왕뚜껑",
        "컵누들 매콤", "오뚜기 진짬뽕", "틈새라면", "카레라면",
        "스낵면", "육개장사발면", "미역국라면", "참깨라면",
        "곰탕면", "콩나물면", "쌀국수 컵", "짬뽕라면",
        "맛있는라면", "사리곰탕면", "무파마라면", "하바네로라면",
    ],
    "도시락": [
        "참치마요 삼각김밥", "불고기 삼각김밥", "김치볶음밥 도시락", "제육볶음 도시락",
        "치킨마요 도시락", "돈까스 도시락", "비빔밥 도시락", "카레 도시락",
        "스팸김밥", "참치김밥", "소불고기 도시락", "닭갈비 도시락",
        "오므라이스 도시락", "함박스테이크 도시락", "불닭 도시락", "갈비 도시락",
        "새우초밥 세트", "연어초밥 세트", "유부초밥 세트", "샌드위치 햄치즈",
        "샌드위치 에그마요", "샌드위치 BLT", "핫도그 클래식", "핫도그 치즈",
        "주먹밥 참치", "주먹밥 불고기", "컵밥 제육", "컵밥 김치찌개",
        "떡볶이 컵", "순대 컵",
    ],
    "아이스크림": [
        "메로나", "비비빅", "수박바", "쌍쌍바", "파피코",
        "월드콘", "구구콘", "부라보콘", "옥동자", "더위사냥",
        "탱크보이", "죠스바", "스크류바", "폴라포", "셀렉션",
        "아맛나", "바밤바", "빵빠레", "투게더", "엔초",
        "하겐다즈 바닐라", "하겐다즈 초코", "찰떡아이스", "빵또아",
        "돼지바", "누가바", "보석바", "캔디바", "설레임", "와",
    ],
    "생활용품": [
        "미니 물티슈", "포켓 티슈", "일회용 마스크 5매", "손소독제 50ml",
        "칫솔 1개입", "여행용 치약", "면도기 1회용", "헤어밴드",
        "일회용 우비", "접이식 우산", "충전케이블 C타입", "충전케이블 8핀",
        "이어폰 유선", "밴드 10매입", "소화제", "두통약",
        "감기약", "안약", "립밤", "핫팩 1매",
    ],
    "유제품": [
        "서울우유 1L", "매일우유 500ml", "남양우유 200ml", "그릭요거트 100g",
        "액티비아 4개입", "비요뜨 초코", "비요뜨 딸기", "덴마크 드링킹요거트",
        "요플레 오리지널", "스트링치즈 4개입", "슬라이스치즈 10매", "크림치즈 200g",
        "버터 100g", "계란 10구", "계란 30구", "연세우유 900ml",
        "상하목장 우유 125ml", "매일 소화가잘되는우유", "풀무원 두부 300g", "순두부 350g",
    ],
    "주류": [
        "카스 355ml", "카스 500ml", "하이트 355ml", "테라 500ml",
        "클라우드 500ml", "필라이트 500ml", "참이슬 360ml", "처음처럼 360ml",
        "진로 360ml", "좋은데이 360ml", "새로 360ml", "하이볼 캔 355ml",
        "맥주 4캔 묶음", "소주 2병 묶음", "와인 미니 187ml", "막걸리 750ml",
        "카스 논알콜", "하이트제로", "백세주 375ml", "매화수 375ml",
    ],
}

REGIONS = {
    "서울 강남": ["역삼", "삼성", "논현", "신사", "압구정", "청담", "대치", "도곡", "개포", "일원", "수서", "세곡"],
    "서울 서초": ["서초", "방배", "잠원", "반포", "양재", "내곡", "남부터미널"],
    "서울 송파": ["잠실", "신천", "가락", "문정", "장지", "위례", "방이", "오금", "석촌"],
    "서울 강동": ["천호", "길동", "둔촌", "명일", "상일", "고덕", "강일", "암사"],
    "서울 마포": ["홍대", "합정", "상수", "망원", "연남", "성산", "상암", "공덕"],
    "서울 영등포": ["여의도", "영등포", "당산", "문래", "양평", "신길", "대림"],
    "서울 종로": ["종로", "광화문", "인사동", "혜화", "대학로", "종각"],
    "서울 용산": ["이태원", "한남", "용산역", "녹사평", "삼각지", "효창"],
    "서울 성동": ["성수", "왕십리", "금호", "옥수", "행당", "응봉"],
    "서울 광진": ["건대입구", "구의", "자양", "화양", "군자", "중곡"],
    "서울 강서": ["마곡", "발산", "화곡", "등촌", "가양", "김포공항"],
    "서울 구로": ["구로디지털", "신도림", "오류", "고척", "개봉"],
    "서울 관악": ["신림", "봉천", "서울대입구", "낙성대"],
    "서울 동작": ["노량진", "대방", "사당", "이수", "흑석"],
    "서울 노원": ["상계", "중계", "하계", "공릉", "월계"],
    "서울 중구": ["명동", "충무로", "을지로", "남대문"],
    "서울 서대문": ["신촌", "연희", "홍제", "이대"],
    "서울 동대문": ["회기", "답십리", "장안", "신설"],
    "서울 성북": ["성신여대", "돈암", "길음", "정릉"],
    "서울 강북": ["수유", "미아", "번동"],
    "서울 도봉": ["도봉", "창동", "방학"],
    "서울 중랑": ["면목", "상봉", "신내"],
    "서울 금천": ["가산", "독산", "금천"],
    "서울 양천": ["목동", "신정", "신월"],
    "경기 성남": ["분당", "정자", "서현", "야탑", "판교", "모란"],
    "경기 수원": ["영통", "광교", "매탄", "인계", "권선", "수원역"],
    "경기 용인": ["수지", "기흥", "동백", "죽전", "보정"],
    "경기 고양": ["일산", "삼송", "화정", "백석", "킨텍스"],
    "경기 화성": ["동탄", "병점", "봉담", "향남"],
    "경기 파주": ["운정", "금촌", "교하"],
    "경기 안양": ["범계", "인덕원", "평촌", "안양역"],
    "경기 부천": ["중동", "상동", "소사", "역곡"],
    "경기 하남": ["미사", "감일", "풍산"],
    "경기 김포": ["장기", "구래", "풍무"],
    "경기 광명": ["철산", "광명사거리", "하안"],
    "인천 부평": ["부평", "산곡", "십정"],
    "인천 남동": ["구월", "간석", "논현"],
    "인천 연수": ["송도", "연수", "청학"],
    "인천 계양": ["계양", "작전", "경인"],
    "부산 해운대": ["해운대", "반여", "우동", "재송"],
    "부산 서면": ["서면", "전포", "부전"],
    "부산 남구": ["대연", "문현", "용호"],
    "부산 동래": ["동래", "명륜", "온천"],
    "부산 수영": ["광안리", "수영", "민락"],
    "부산 사상": ["사상", "괘법"],
    "부산 사하": ["하단", "다대포"],
    "부산 금정": ["장전", "남산"],
    "대구 중구": ["동성로", "삼덕", "남산"],
    "대구 수성": ["수성", "범어", "만촌", "두산"],
    "대구 달서": ["월성", "상인", "죽전", "성서"],
    "대구 북구": ["칠곡", "관음", "침산"],
    "대전 서구": ["둔산", "관저", "갈마", "탄방"],
    "대전 유성": ["유성온천", "궁동", "봉명", "노은"],
    "대전 중구": ["대전역", "은행", "대흥"],
    "광주 서구": ["상무", "치평", "농성"],
    "광주 북구": ["운암", "용봉", "문흥"],
    "울산 남구": ["삼산", "달동", "무거"],
    "세종": ["나성", "보람", "조치원", "어진", "한솔", "도담"],
    "강원 춘천": ["명동", "석사", "후평", "퇴계"],
    "강원 원주": ["단계", "무실", "혁신도시"],
    "강원 강릉": ["교동", "입암", "포남", "경포"],
    "충북 청주": ["성안길", "율량", "복대", "가경"],
    "충남 천안": ["두정", "불당", "성정", "쌍용"],
    "전북 전주": ["객사", "덕진", "효자", "송천"],
    "전남 순천": ["연향", "왕지", "조례"],
    "전남 여수": ["학동", "문수", "돌산"],
    "전남 목포": ["하당", "용당", "상동"],
    "경북 포항": ["양덕", "죽도", "두호"],
    "경북 구미": ["원평", "공단", "형곡"],
    "경남 창원": ["상남", "중앙", "용호", "마산"],
    "경남 김해": ["내외", "삼계", "장유"],
    "제주 제주시": ["노형", "연동", "삼도", "이도"],
    "제주 서귀포": ["서귀포", "중문", "대정"],
}

OFC_NAMES = [
    "김지원", "박서연", "이준호", "최민지", "정다은",
    "한승우", "윤지혜", "오재현", "임수빈", "강동현",
    "조예린", "배성민", "신지수", "류현우", "황미라",
    "서영호", "전은정", "남기태", "권소영", "문재혁",
    "양하늘", "홍세진", "송민석", "차유리", "고태훈",
    "안지영", "노승현", "유다인", "장혁수", "피수진",
]

_STORE_SUFFIXES = ["점", "역점", "사거리점", "중앙점", "타워점", "1호점", "2호점", "3호점"]

_ISSUES_POOL = [
    "냉장 설비 온도 이상", "냉동고 성에 과다", "POS 단말기 간헐적 오류",
    "야간 알바 결근 빈발", "화장실 수도 고장", "간판 LED 일부 불량",
    "CCTV 3번 카메라 화질 저하", "에어컨 냉방 약함", "자동문 센서 오작동",
    "전기 누전 차단기 트립", "진열대 선반 파손", "바닥 타일 균열",
    "무인 계산대 카드 인식 불량", "프레시푸드 진열대 온도 높음",
    "쓰레기 수거 시간 변경 필요", "주차장 안내판 파손",
    "배달 대행 기사 불친절 민원", "고객 슬립 사고 (바닥 물기)",
    "담배 진열장 잠금장치 고장", "ATM기 용지 부족",
]

_PROMO_TYPES = ["1+1", "2+1", "할인"]
_DISCOUNTS = ["200원", "300원", "500원", "700원", "1000원"]


# ─────────────────────────────────────────────
# 프로모션 300건 자동 생성
# ─────────────────────────────────────────────

def _date(offset_days):
    return (datetime.now() + timedelta(days=offset_days)).strftime("%Y-%m-%d")


def _generate_promotions(count=300):
    promos = []
    all_products = []
    for cat, items in PRODUCT_CATALOG.items():
        for item in items:
            all_products.append((cat, item))
    random.shuffle(all_products)

    for i in range(count):
        cat, product = all_products[i % len(all_products)]
        promo_type = random.choice(_PROMO_TYPES)
        r = random.random()
        if r < 0.50:
            status, s_min, s_max, e_min, e_max = "진행중", -7, 0, 3, 10
        elif r < 0.75:
            status, s_min, s_max, e_min, e_max = "예정", 1, 5, 8, 21
        else:
            status, s_min, s_max, e_min, e_max = "종료", -30, -14, -10, -1
        start_offset = random.randint(s_min, s_max)
        end_offset = random.randint(e_min, e_max)
        if end_offset <= start_offset:
            end_offset = start_offset + random.randint(3, 10)
        promo = {
            "id": f"P{i+1:04d}", "name": product, "category": cat,
            "type": promo_type, "start": _date(start_offset),
            "end": _date(end_offset), "status": status,
        }
        if promo_type == "할인":
            promo["discount"] = random.choice(_DISCOUNTS)
        promos.append(promo)
    return promos


PROMOTIONS = _generate_promotions(300)


# ─────────────────────────────────────────────
# 점포 500개 자동 생성 — 지역+동네 이름 매칭
# ─────────────────────────────────────────────

def _generate_stores(count=500):
    stores = []
    all_locations = []
    for region, areas in REGIONS.items():
        for area in areas:
            suffix = random.choice(_STORE_SUFFIXES)
            all_locations.append((region, area, f"{area}{suffix}"))
    while len(all_locations) < count:
        region = random.choice(list(REGIONS.keys()))
        area = random.choice(REGIONS[region])
        suffix = random.choice(_STORE_SUFFIXES)
        seq = random.randint(2, 5)
        all_locations.append((region, area, f"{area}{seq}호점"))
    random.shuffle(all_locations)
    all_locations = all_locations[:count]

    for i, (region, area, name) in enumerate(all_locations, 1):
        ofc = random.choice(OFC_NAMES)
        if "강남" in region or "서초" in region or "송파" in region:
            daily_sales = round(random.randint(2500000, 5500000), -4)
        elif "서울" in region:
            daily_sales = round(random.randint(2000000, 5000000), -4)
        elif "경기" in region:
            daily_sales = round(random.randint(1800000, 4800000), -4)
        else:
            daily_sales = round(random.randint(1500000, 5000000), -4)
        sales_change = round(random.uniform(-20.0, 30.0), 1)
        waste_rate = round(random.uniform(0.5, 12.0), 1)
        promo_order_rate = random.randint(40, 100)
        quick_commerce_sales = random.randint(100000, 800000) if random.random() < 0.6 else 0
        quick_commerce_change = round(random.uniform(-5.0, 35.0), 1) if quick_commerce_sales > 0 else 0
        staff_count = random.randint(2, 12)
        open_year = random.randint(2008, 2025)
        issues = random.sample(_ISSUES_POOL, random.randint(1, 2)) if random.random() > 0.70 else []
        if sales_change >= 10 and waste_rate < 3.0 and not issues:
            status = "우수"
        elif sales_change < -5 or waste_rate > 6.0 or issues:
            status = "주의"
        else:
            status = "양호"
        stores.append({
            "id": f"S{i:04d}", "name": name, "region": region, "location": area,
            "ofc": ofc, "daily_sales": daily_sales, "sales_change": sales_change,
            "waste_rate": waste_rate, "promo_order_rate": promo_order_rate,
            "quick_commerce_sales": quick_commerce_sales,
            "quick_commerce_change": quick_commerce_change,
            "staff_count": staff_count, "open_year": open_year,
            "issues": issues, "status": status,
        })
    return stores


STORES = _generate_stores(1500)


MANUALS = [
    {"id": "M001", "title": "유통기한 경과 상품 처리 절차", "category": "재고관리", "content": "1. 즉시 판매대에서 분리\n2. POS 폐기 등록\n3. 사진 촬영 후 처리\n※ 프레시푸드 당일 폐기\n※ 재고처리한도 연 108만원"},
    {"id": "M002", "title": "발주 자동취소 기준", "category": "발주관리", "content": "• 24시간 미확인 시 자동 취소\n• 재고 200% 초과 시 경고\n• 단종 D-7 발주 차단\n• 긴급 발주: OFC 승인 필요"},
    {"id": "M003", "title": "프레시푸드 폐기 처리", "category": "재고관리", "content": "• 당일 소비기한 경과 즉시 폐기\n• POS 등록 필수\n• 월 폐기율 5% 초과 시 OFC 점검"},
    {"id": "M004", "title": "점포 위생점검 체크리스트", "category": "위생관리", "content": "[ ] 바닥 청결\n[ ] 냉장 0~5℃\n[ ] 냉동 -18℃ 이하\n[ ] 유통기한 확인\n※ 매월 1회 OFC 점검"},
    {"id": "M005", "title": "경영주 계약 갱신 절차", "category": "계약관리", "content": "1. 만료 6개월 전 의향 확인\n2. 3개월 전 조건 협의\n3. 2개월 전 계약 체결\n※ 10년 이상 건강검진 제공"},
    {"id": "M006", "title": "담배 성인인증 절차", "category": "판매관리", "content": "• 신분증 필수\n• 미성년자 판매 시 경고→영업정지→해지\n• 외모 판단 금지"},
    {"id": "M007", "title": "안심 운영 지원금 안내", "category": "계약관리", "content": "• 18시간 이상 운영 시 최소 운영비 보장\n• 수입 미달 시 회사 보조"},
    {"id": "M008", "title": "퀵커머스 주문 처리", "category": "배송관리", "content": "1. POS 알림 확인\n2. 5분 내 피킹\n3. 전용 포장\n4. 배달 기사 인계\n※ 15분 미처리 시 자동 취소"},
    {"id": "M009", "title": "교통카드 충전", "category": "결제관리", "content": "• POS 교통카드 충전 메뉴\n• 최소 1,000원 단위\n• 잔액 이전 가능"},
    {"id": "M010", "title": "택배 접수 절차", "category": "부가서비스", "content": "1. POS 택배 접수\n2. 정보 입력\n3. 결제 후 송장 출력\n4. 기사 수거"},
    {"id": "M011", "title": "CCTV 관리", "category": "보안관리", "content": "• 영상 보관 30일\n• 열람: 경찰 공문 필요\n• 외부 유출 금지"},
    {"id": "M012", "title": "화재 대피 매뉴얼", "category": "안전관리", "content": "1. 119 신고\n2. 고객 대피 유도\n3. 소화기 사용\n4. 가스/전기 차단\n5. 본사 보고"},
    {"id": "M013", "title": "멤버십 포인트 적립", "category": "고객관리", "content": "• GS&POINT 0.5% 적립\n• 1포인트=1원\n• KT/SK/LG 멤버십 제휴"},
    {"id": "M014", "title": "반품 교환 기준", "category": "고객관리", "content": "• 단순 변심: 24시간 + 영수증\n• 상품 하자: 즉시 교환 + 1개 추가\n• 프레시푸드: 당일만"},
    {"id": "M015", "title": "알바 채용 관리", "category": "인사관리", "content": "• 만 15세 이상\n• 주 15시간 이상 4대보험\n• 야간 50% 가산\n• 주휴수당 지급"},
    {"id": "M016", "title": "인테리어 리뉴얼", "category": "시설관리", "content": "• 5년 경과 시\n• 본사 비용 일부 부담\n• 공사 5~7일"},
    {"id": "M017", "title": "주류 판매 규정", "category": "판매관리", "content": "• 24시간 판매 가능\n• 미성년자/만취자 금지\n• 위반 시 영업정지"},
    {"id": "M018", "title": "에너지 절감 가이드", "category": "시설관리", "content": "• 야간 절전 모드 15% 절감\n• LED 전환 40% 절감\n• 절감 시 인센티브"},
    {"id": "M019", "title": "재고 실사 절차", "category": "재고관리", "content": "• 월 1회 실사\n• POS vs 실물 대조\n• 차이 시 OFC 보고\n• 정확도 99% 목표"},
    {"id": "M020", "title": "고객 클레임 대응", "category": "고객관리", "content": "1. 경청\n2. 사과\n3. 확인\n4. 해결\n5. 보고\n※ 폭언 시 112 신고"},
]


# ─────────────────────────────────────────────
# 데이터 조회 함수
# ─────────────────────────────────────────────

def get_active_promotions(category=None):
    result = [p for p in PROMOTIONS if p["status"] == "진행중"]
    if category:
        result = [p for p in result if p["category"] == category]
    return result

def get_upcoming_promotions(days=7):
    cutoff = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    return [p for p in PROMOTIONS if p["status"] == "예정" and p["start"] <= cutoff]

def get_starting_soon(days=3):
    cutoff = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    return [p for p in PROMOTIONS if p["status"] == "예정" and p["start"] <= cutoff]

def get_ending_soon(days=2):
    cutoff = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    return [p for p in PROMOTIONS if p["status"] == "진행중" and p["end"] <= cutoff]

def get_ended_promotions(days=14):
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return [p for p in PROMOTIONS if p["status"] == "종료" and p["end"] >= since]

def get_promo_by_category(category):
    return [p for p in PROMOTIONS if p["category"] == category]

def search_promotion(keyword):
    keyword = keyword.lower()
    return [p for p in PROMOTIONS if keyword in p["name"].lower() and p["status"] != "종료"]

def get_stores_by_ofc(ofc_name=None):
    if ofc_name:
        return [s for s in STORES if s["ofc"] == ofc_name]
    return STORES

def get_stores_by_region(region_keyword):
    kw = (region_keyword or "").strip()
    if not kw:
        return []
    result = []
    for s in STORES:
        if kw in s.get("region", "") or kw in s.get("location", "") or kw in s.get("name", ""):
            result.append(s)
    return result

def get_store_by_name(name):
    for s in STORES:
        if name in s["name"]:
            return s
    return None

def get_struggling_stores(ofc_name=None):
    stores = get_stores_by_ofc(ofc_name) if ofc_name else STORES
    return [s for s in stores if s["status"] == "주의"]

def get_top_stores(limit=5, ofc_name=None):
    stores = get_stores_by_ofc(ofc_name) if ofc_name else STORES
    return sorted(stores, key=lambda s: s["daily_sales"], reverse=True)[:limit]

def get_bottom_stores(limit=5, ofc_name=None):
    stores = get_stores_by_ofc(ofc_name) if ofc_name else STORES
    return sorted(stores, key=lambda s: s["daily_sales"])[:limit]

def get_high_waste_stores(threshold=5.0, ofc_name=None):
    stores = get_stores_by_ofc(ofc_name) if ofc_name else STORES
    return [s for s in stores if s["waste_rate"] > threshold]

def get_store_issues(ofc_name=None):
    stores = get_stores_by_ofc(ofc_name) if ofc_name else STORES
    return [s for s in stores if s["issues"]]

def get_quick_commerce_stores(ofc_name=None):
    stores = get_stores_by_ofc(ofc_name) if ofc_name else STORES
    return [s for s in stores if s.get("quick_commerce_sales", 0) > 0]

def get_region_summary():
    summary = {}
    for s in STORES:
        region = s["region"]
        if region not in summary:
            summary[region] = {"count": 0, "total_sales": 0, "issues": 0}
        summary[region]["count"] += 1
        summary[region]["total_sales"] += s["daily_sales"]
        summary[region]["issues"] += len(s["issues"])
    for region in summary:
        summary[region]["avg_sales"] = summary[region]["total_sales"] // summary[region]["count"]
    return summary

def search_manual(keyword):
    keyword = keyword.lower()
    return [m for m in MANUALS if keyword in m["title"].lower() or keyword in m["content"].lower() or keyword in m["category"].lower()]

def get_promo_stats():
    active = [p for p in PROMOTIONS if p["status"] == "진행중"]
    upcoming = [p for p in PROMOTIONS if p["status"] == "예정"]
    ended = [p for p in PROMOTIONS if p["status"] == "종료"]
    by_category = {}
    for p in active:
        by_category[p["category"]] = by_category.get(p["category"], 0) + 1
    by_type = {}
    for p in active:
        by_type[p["type"]] = by_type.get(p["type"], 0) + 1
    return {"active_count": len(active), "upcoming_count": len(upcoming), "ended_count": len(ended), "by_category": by_category, "by_type": by_type}