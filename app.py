# app.py
try:
    import pkg_resources
except ImportError:
    import types as _types, sys as _sys
    _pkg = _types.ModuleType('pkg_resources')
    _pkg.get_distribution = lambda name: _types.SimpleNamespace(version='unknown')
    _pkg.DistributionNotFound = Exception
    _sys.modules['pkg_resources'] = _pkg

import os, json, requests, atexit, signal, threading, random, re, time, base64, hmac, hashlib, urllib.parse
import warnings
import pytz
from datetime import datetime, timezone, timedelta, time as dtime

KST = timezone(timedelta(hours=9))
SEOUL_TZ = pytz.timezone("Asia/Seoul")  # ✅ APScheduler(run_daily)용

# =========================
# Console 경고(Warning) 정리
#  - 기능과 무관한 경고는 숨기고, 에러는 그대로 표시
# =========================
warnings.filterwarnings("ignore", category=FutureWarning)

warnings.filterwarnings(
    "ignore",
    message=r".*google\.api_core.*non-supported Python version.*",
)

warnings.filterwarnings(
    "ignore",
    message=r".*Glyph.*missing from current font.*",
)
warnings.filterwarnings(
    "ignore",
    message=r".*In FT2Font: Could not set the fontsize.*",
)

QUIET_LOG = True  # True면 불필요한 콘솔 로그 숨김




def _to_ts(v):
    """날짜·시간 문자열 또는 datetime을 timestamp(float) 로 변환"""
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, datetime):
            return v.timestamp()

        s = str(v).strip()

        # JSON에서 넘어오는 날짜 문자열 처리
        # 예: "2025. 11. 23. 오후 8:20" 형태
        try:
            dt = datetime.strptime(s, "%Y. %m. %d. %p %I:%M")
            return dt.replace(tzinfo=KST).timestamp()
        except:
            pass

        # 다른 날짜 형식 대비
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=KST).timestamp()
        except:
            pass

    except:
        return None
    return None


# === Google Calendar ===
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz
try:
    from caldav import DAVClient
except Exception as e:
    DAVClient = None
    print("[BOOT] caldav import failed:", e)


from http.server import BaseHTTPRequestHandler, HTTPServer
from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Updater, MessageHandler, Filters, CallbackQueryHandler

import io
import matplotlib
matplotlib.use("Agg")  # ✅ GUI 안 쓰고 이미지용 백엔드 사용

# POS 매출 연동 (okpos_card_watcher.py가 있을 때만)
try:
    from okpos_card_watcher import build_sales_summary_for_period  # 실제 POS 환경용

except ModuleNotFoundError:
    # 맥처럼 okpos_card_watcher.py가 없는 환경에서는
    # Cloudflare Worker(KV)에 쌓인 매출을 이용해서 매출 요약 텍스트를 만든다.
    #
    # period: "today" / "yesterday" / "this_month" / "prev_month"
    # open_unpaid: (옵션) 오늘 매출일 때만 계산전 테이블 수를 넘겨줄 수 있음
    def build_sales_summary_for_period(period, open_unpaid=None) -> str:
        from datetime import datetime, timedelta
        import requests
        import json

        # ===== 라벨 =====
        label_map = {
            "today": "오늘",
            "yesterday": "어제",
            "this_month": "당월",
            "prev_month": "전월",
        }
        label = label_map.get(period, "매출")

        # ===== 기간별 날짜 리스트 =====
        today = datetime.now(KST).date()

        def _period_dates(p: str):
            if p == "today":
                start = end = today
            elif p == "yesterday":
                start = end = today - timedelta(days=1)
            elif p == "this_month":
                start = today.replace(day=1)
                end = today
            elif p == "prev_month":
                first_this_month = today.replace(day=1)
                last_prev_month = first_this_month - timedelta(days=1)
                start = last_prev_month.replace(day=1)
                end = last_prev_month
            else:
                start = end = today

            days = (end - start).days
            return [
                (start + timedelta(days=i)).strftime("%Y%m%d")
                for i in range(days + 1)
            ]

        # ===== Worker에서 하루치 매출 JSONL 가져오기 =====
        WORKER_BASE = "https://okpos-proxy.ginix8557.workers.dev"

        def _fetch_sales_for_date(date_ymd: str):
            url = f"{WORKER_BASE}/sales?date={date_ymd}"
            try:
                resp = requests.get(url, timeout=3)
            except Exception:
                return []
            if resp.status_code != 200:
                return []
            text = resp.text.strip()
            if not text:
                return []

            records = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except Exception:
                    # 깨진 줄은 무시
                    continue
            return records

        # ===== 기간 전체 레코드 모으기 =====
        all_records = []
        for d in _period_dates(period):
            all_records.extend(_fetch_sales_for_date(d))

        if not all_records:
            return f"📊 {label} 매출\n\n등록된 매출이 없어요."

        # ===== 합계 / 카테고리 집계 =====

        def _aggregate_by_method(records):
            """
            결제수단별 매출 합계 계산
            - 카드 / 현금 / 서비스
            - 서비스:
              • 금액  = item_details 의 금액(원래 금액) 합계
              • 건수  = item_details 의 수량(qty) 합계
            """
            summary = {
                "total_amount": 0,
                "method": {
                    "card":   {"amount": 0, "count": 0},
                    "cash":   {"amount": 0, "count": 0},
                    "service": {"amount": 0, "count": 0},
                },
            }

            for rec in records:
                # 기본 금액들 안전하게 파싱
                try:
                    total = int(rec.get("total_amount", 0) or 0)
                except Exception:
                    total = 0

                try:
                    card = int(rec.get("card_amount", 0) or 0)
                except Exception:
                    card = 0

                try:
                    cash = int(rec.get("cash_amount", 0) or 0)
                except Exception:
                    cash = 0

                source = str(rec.get("source", "")).upper()
                event = str(rec.get("event", "")).upper()
                sign = -1 if event == "CANCEL" else 1

                # 총합 (포인트/서비스 포함한 순매출)
                summary["total_amount"] += total

                # ----- 서비스 매출 (원래 금액 + 수량(qty) 기준 건수) -----
                if source == "SERVICE":
                    item_details = rec.get("item_details") or []
                    menus = rec.get("menus") or []

                    svc_amount = 0
                    svc_count = 0

                    # 금액: item_details 의 amount 합계 (원래 금액)
                    if isinstance(item_details, list) and item_details:
                        for d in item_details:
                            try:
                                line_amt = int(d.get("amount", 0) or 0)
                            except Exception:
                                line_amt = 0
                            if line_amt:
                                svc_amount += abs(line_amt)

                            # 🔢 건수: amount 상관없이 qty 합계
                            try:
                                qty = int(d.get("qty", 0) or 0)
                            except Exception:
                                qty = 0
                            if qty:
                                svc_count += abs(qty)
                    else:
                        # item_details 가 없다면, menus 줄수라도 사용
                        if isinstance(menus, list) and menus:
                            svc_count = len(menus)
                        # 그래도 아무 정보가 없고 total 만 있다면 최소 1건 처리
                        if total != 0 and svc_amount == 0:
                            svc_amount = abs(total)
                            if svc_count == 0:
                                svc_count = 1

                    if svc_amount or svc_count:
                        summary["method"]["service"]["amount"] += svc_amount * sign
                        summary["method"]["service"]["count"] += svc_count * sign

                    # 서비스 레코드는 카드/현금 합계에는 포함하지 않고 여기서 끝
                    continue

                # ----- 카드 -----
                if card != 0:
                    summary["method"]["card"]["amount"] += card
                    summary["method"]["card"]["count"] += (1 if card > 0 else -1)

                # ----- 현금 -----
                if cash != 0:
                    summary["method"]["cash"]["amount"] += cash
                    summary["method"]["cash"]["count"] += (1 if cash > 0 else -1)

            return summary




        def _categorize_menus(records):
            """
            메뉴 카테고리별 매출 계산

            - 카페, 위탁, 미용, 회원권
            - 가능하면 item_details(메뉴별 name/qty/amount)를 기준으로
              카테고리별 금액/건수를 실제 금액대로 나눈다.
            - item_details 가 없으면 기존 menus / total_amount 기반 로직으로 fallback.
            """
            result = {
                "cafe": {
                    "amount": 0,
                    "count": 0,
                    "guest": 0,       # 입장료 건수 (이용객수)
                },
                "petcare": {
                    "amount": 0,
                    "count": 0,
                    "daycare": 0,     # 기본료 + 종일반 건수
                    "pre11": 0,       # 11시이전위탁 건수
                },
                "beauty": {
                    "amount": 0,
                    "count": 0,
                },
                "membership": {
                    "amount": 0,
                    "count": 0,
                },
            }

            # === 키워드 (POS 코드와 최대한 맞춰서) ===
            petcare_kw = [
                "기본료",
                "냠냠",
                "종일반",
                "호텔",
                "4kg",
                "8kg",
                "12kg",
                "16kg",
                "20kg",
                "8회",
                "12회",
                "20회",
            ]

            membership_kw = [
                "330,000원권",
                "570,000원권",
                "1,150,000원권",
                "1,150,000원 권",
            ]

            beauty_kw = ["미용"]

            core_petcare_kw = [
                "기본료",
                "종일반",
                "호텔",
                "위탁",
                "유치원",
                "데이케어",
                "11시이전위탁",
                "8회",
                "12회",
                "20회",
            ]
            weight_kw = ["4kg", "8kg", "12kg", "16kg", "20kg"]

            def classify_menu(text: str) -> str:
                """
                개별 메뉴 이름 하나에 대한 카테고리 판정
                - 반환값: "cafe" / "petcare" / "beauty" / "membership"
                """
                text = text or ""
                compact = text.replace(" ", "")

                # 1) 미용
                for k in beauty_kw:
                    if k in text:
                        return "beauty"

                # 2) 회원권
                for k in membership_kw:
                    if k in text:
                        return "membership"

                # 3) 위탁 관련
                has_core = any(k in text for k in core_petcare_kw)
                has_weight = any(k in text for k in weight_kw)

                # 3-1) 단독 키로수 메뉴 (예: "4kg", "~4kg") → 위탁
                if compact in weight_kw or any(compact == f"~{w}" for w in weight_kw):
                    return "petcare"

                # 3-2) 위탁 핵심 키워드가 들어가면 → 위탁
                if has_core:
                    return "petcare"

                # 3-3) 키로수 + "미만" 이 같이 들어 있으면 → 위탁
                if has_weight and "미만" in text:
                    return "petcare"

                # 3-4) 그 외 키로수(사료 4kg 등)는 카페
                if has_weight:
                    return "cafe"

                # 4) 나머지는 전부 카페
                return "cafe"

            # -------------------------
            # 레코드 반복
            # -------------------------
            for rec in records:
                event = (rec.get("event") or "").upper()
                try:
                    total = int(rec.get("total_amount", 0) or 0)
                except Exception:
                    total = 0

                # CANCEL 이면 음수, 나머지(SALE/OLD/SERVICE 등)는 양수
                sign = -1 if event == "CANCEL" else 1

                item_details = rec.get("item_details")

                # =====================================================
                # 1) item_details 있는 경우 → 메뉴별 금액 기준으로 정확히 합산
                # =====================================================
                if isinstance(item_details, list) and item_details:
                    per_cat_amount = {
                        "cafe": 0,
                        "petcare": 0,
                        "beauty": 0,
                        "membership": 0,
                    }

                    for d in item_details:
                        try:
                            name = str(d.get("name", "") or "")
                        except Exception:
                            name = ""

                        qty_raw = d.get("qty", 1)
                        try:
                            qty = int(qty_raw)
                        except Exception:
                            qty = 1

                        amt_raw = d.get("amount", 0)
                        try:
                            line_amt = int(amt_raw)
                        except Exception:
                            line_amt = 0

                        cat = classify_menu(name)
                        adj_amt = line_amt * sign

                        per_cat_amount[cat] += adj_amt
                        result[cat]["amount"] += adj_amt

                        # 카페 → 이용객수(입장료)
                        if cat == "cafe" and "입장료" in name:
                            result["cafe"]["guest"] += qty * sign

                        # 위탁 → 데이케어/11시이전위탁
                        if cat == "petcare":
                            if ("기본료" in name) or ("종일반" in name):
                                result["petcare"]["daycare"] += qty * sign
                            if "11시이전위탁" in name:
                                result["petcare"]["pre11"] += qty * sign

                    # 이 영수증에서 카테고리 금액이 실제로 있으면 건수 ±1
                    for cat, cat_amt in per_cat_amount.items():
                        if cat_amt > 0:
                            result[cat]["count"] += 1
                        elif cat_amt < 0:
                            result[cat]["count"] -= 1

                    # item_details 로 처리했으면 아래 menus 기반 로직은 건너뜀
                    continue

                # =====================================================
                # 2) fallback: item_details 가 없으면 기존 menus / total_amount 로 처리
                #    (과거 데이터 호환용, 한 영수증은 한 카테고리로만 분류)
                # =====================================================
                menus = rec.get("menus") or []
                if not menus:
                    continue

                has_beauty = False
                has_membership = False
                has_petcare = False

                for m in menus:
                    cat = classify_menu(str(m))
                    if cat == "beauty":
                        has_beauty = True
                    elif cat == "membership":
                        has_membership = True
                    elif cat == "petcare":
                        has_petcare = True

                # 우선순위: 미용 > 회원권 > 위탁 > 카페
                if has_beauty:
                    category = "beauty"
                elif has_membership:
                    category = "membership"
                elif has_petcare:
                    category = "petcare"
                else:
                    category = "cafe"

                result[category]["amount"] += total
                if total > 0:
                    result[category]["count"] += 1
                elif total < 0:
                    result[category]["count"] -= 1

                # ===== 카테고리별 부가 지표 (fallback일 때만, 1건 기준) =====
                if category == "cafe":
                    for m in menus:
                        text = str(m)
                        if "입장료" in text:
                            if total > 0:
                                result["cafe"]["guest"] += 1
                            elif total < 0:
                                result["cafe"]["guest"] -= 1

                if category == "petcare":
                    for m in menus:
                        text = str(m)
                        if ("기본료" in text) or ("종일반" in text):
                            if total > 0:
                                result["petcare"]["daycare"] += 1
                            elif total < 0:
                                result["petcare"]["daycare"] -= 1
                        if "11시이전위탁" in text:
                            if total > 0:
                                result["petcare"]["pre11"] += 1
                            elif total < 0:
                                result["petcare"]["pre11"] -= 1

            return result

        method_summary = _aggregate_by_method(all_records)
        category_summary = _categorize_menus(all_records)

        # ===== 문자열 포맷 =====
        total = method_summary.get("total_amount", 0)
        m = method_summary.get("method", {})
        card = m.get("card", {})
        cash = m.get("cash", {})
        service = m.get("service", {})

        cafe = category_summary.get("cafe", {})
        pet = category_summary.get("petcare", {})
        beauty = category_summary.get("beauty", {})
        membership = category_summary.get("membership", {})

        def _fmt_amount(v) -> str:
            try:
                return f"{int(v):,}"
            except Exception:
                return "0"

        def _fmt_count(v) -> str:
            try:
                return f"{int(v)}건"
            except Exception:
                return "0건"

        lines: list[str] = []

        # --- 헤더 ---
        lines.append(f"📊 {label} 매출")
        lines.append("")

        # --- 총금액 / 결제수단 ---
        lines.append("💰 총금액")
        lines.append(f"• 합계: {_fmt_amount(total)}원")
        lines.append(
            f"  - 💳 카드: {_fmt_amount(card.get('amount', 0))}원 "
            f"({_fmt_count(card.get('count', 0))})"
        )
        lines.append(
            f"  - 💵 현금: {_fmt_amount(cash.get('amount', 0))}원 "
            f"({_fmt_count(cash.get('count', 0))})"
        )
        lines.append(
            f"  - 🎁 서비스: {_fmt_amount(service.get('amount', 0))}원 "
            f"({_fmt_count(service.get('count', 0))})"
        )

        # --- 카테고리별 ---
        lines.append("")
        lines.append("📂 카테고리별")

        # 카페
        cafe_guest = int(cafe.get("guest", 0) or 0)
        lines.append("☕ 카페")
        lines.append(
            f"• 매출: {_fmt_amount(cafe.get('amount', 0))}원 "
            f"({_fmt_count(cafe.get('count', 0))})"
        )
        lines.append(f"• 이용객수: {cafe_guest}명")

        # 위탁
        pet_daycare = int(pet.get("daycare", 0) or 0)
        pet_pre11 = int(pet.get("pre11", 0) or 0)
        lines.append("")
        lines.append("🐾 위탁")
        lines.append(
            f"• 매출: {_fmt_amount(pet.get('amount', 0))}원 "
            f"({_fmt_count(pet.get('count', 0))})"
        )
        lines.append(f"• 데이케어: {pet_daycare}회")
        lines.append(f"• 11시이전위탁: {pet_pre11}회")

        # 미용
        beauty_amount = int(beauty.get("amount", 0) or 0)
        beauty_count = int(beauty.get("count", 0) or 0)
        if (beauty_amount != 0) or (beauty_count != 0):
            lines.append("")
            lines.append("💇 미용")
            lines.append(
                f"• 매출: {_fmt_amount(beauty_amount)}원 "
                f"({_fmt_count(beauty_count)})"
            )

        # 회원권
        membership_amount = int(membership.get("amount", 0) or 0)
        membership_count = int(membership.get("count", 0) or 0)
        if (membership_amount != 0) or (membership_count != 0):
            lines.append("")
            lines.append("🎫 회원권")
            lines.append(
                f"• 매출: {_fmt_amount(membership_amount)}원 "
                f"({_fmt_count(membership_count)})"
            )

        # --- 계산전 (오늘만, 값이 넘어온 경우에만 표시) ---
        if period == "today" and (open_unpaid is not None):
            try:
                open_val = int(open_unpaid)
            except Exception:
                open_val = None

            if open_val is not None:
                lines.append("")
                lines.append("🧾 계산전")
                lines.append(f"• 미결제 테이블: {open_val}건")

        return "\n".join(lines)



def fetch_open_unpaid() -> int:
    """
    Cloudflare Worker에서 오늘 날짜의 매출 JSONL을 가져와서,
    각 레코드 안에 포함된 open_unpaid 필드의 '마지막 값'을 사용한다.
    - 값이 없거나 실패하면 항상 0을 반환해서, 기존 동작에 영향 주지 않는다.
    """
    WORKER_BASE = "https://okpos-proxy.ginix8557.workers.dev"

    # 오늘 날짜(YYYYMMDD, KST 기준)
    today_ymd = datetime.now(KST).strftime("%Y%m%d")
    url = f"{WORKER_BASE}/sales?date={today_ymd}"

    try:
        resp = requests.get(url, timeout=3)
        if resp.status_code != 200:
            return 0
        text = resp.text.strip()
        if not text:
            return 0
    except Exception:
        return 0

    last_val = None

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue

        if "open_unpaid" in obj:
            try:
                last_val = int(obj["open_unpaid"])
            except Exception:
                # 이 줄이 이상하면 그냥 건너뛰고 다음 줄 본다
                continue

    if last_val is None:
        return 0
    return last_val



import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import platform
from matplotlib import font_manager
from typing import Any, Dict, List
import warnings  # 👈 추가

# ===== matplotlib 폰트 관련 경고 싹 정리 =====
#  - Apple Emoji / AppleGothic 이모지 글리프 부족 경고
#  - FT2Font fontsize 관련 경고
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="matplotlib",
)
# ============================================

import matplotlib.pyplot as plt
import matplotlib
import warnings



# ================== 캘린더 간트차트용 Matplotlib 설정 ==================

# 1) 폰트: 맥에서는 AppleGothic 하나만 사용 (한글/기호 다 처리)
matplotlib.rcParams["font.family"] = "AppleGothic"
matplotlib.rcParams["axes.unicode_minus"] = False  # 마이너스 깨짐 방지

# 2) 이모지/폰트 관련 UserWarning 숨기기
warnings.filterwarnings(
    "ignore",
    category= UserWarning,
    message = r"Glyph [0-9]+ \(.*\) missing from current font"
)

warnings.filterwarnings(
    "ignore",
    category= UserWarning,
    message = r"In FT2Font: Could not set the fontsize.*"
)
# ===============================================================


# (선택) 한글 폰트 설정 - OS에 따라 자동 선택
def set_korean_font():
    """
    - macOS : AppleGothic
    - font_name = "Malgun Gothic"
    - 그 외  : NanumGothic (없으면 시스템 기본)
    """
    try:
        system = platform.system()

        if system == "Windows":
            font_name = "Malgun Gothic"
        elif system == "Darwin":  # macOS
            font_name = "AppleGothic"
        else:
            # 리눅스 등
            font_name = "NanumGothic"

        plt.rcParams["font.family"] = font_name
        plt.rcParams["axes.unicode_minus"] = False

        if not QUIET_LOG:
            print(f"[FONT] 한글 폰트 설정: {font_name}")


    except Exception as e:
        # 문제 생겨도 최소한 마이너스 깨짐만 방지
        print(f"[FONT] 폰트 설정 오류 (무시하고 진행): {e}")
        plt.rcParams["axes.unicode_minus"] = False


set_korean_font()


# ==== Apple Emoji 폰트 등록 ====
try:
    emoji_path = "/System/Library/Fonts/Apple Color Emoji.ttc"
    if os.path.exists(emoji_path):
        font_manager.fontManager.addfont(emoji_path)
        plt.rcParams["font.family"] = ["AppleGothic", "Apple Color Emoji"]
except Exception as e:
    if not QUIET_LOG:
        print("[FONT] Apple Emoji 로드 실패:", e)




# ========= ENV =========
load_dotenv()

BOT_TOKEN   = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID     = str(os.getenv("CHAT_ID", "")).strip()
DEFAULT_THRESHOLD = float(os.getenv("THRESHOLD_PCT", "1.0"))
PORT        = int(os.getenv("PORT", "0"))

# Persistent state dir (Render: DATA_DIR=/data)
DATA_DIR    = os.getenv("DATA_DIR", "").strip() or "."
os.makedirs(DATA_DIR, exist_ok=True)

# iCloud 미리알림 스냅샷 파일 (단축어가 저장하는 위치)
# 단축어에서 이 경로로 저장하고 있으니까, 그대로 사용하면 돼.
REMINDER_SNAPSHOT_FILE = (
    "/Users/kiheonkim/Library/Mobile Documents/"
    "com~apple~CloudDocs/upbit-telebot-clean/reminder/snapshot.json"
)


# Naver Searchad API
NAVER_BASE_URL      = "https://api.naver.com"
NAVER_API_KEY       = os.getenv("NAVER_API_KEY", "").strip()
NAVER_API_SECRET    = os.getenv("NAVER_API_SECRET", "").strip()
NAVER_CUSTOMER_ID   = os.getenv("NAVER_CUSTOMER_ID", "").strip()
NAVER_CAMPAIGN_ID   = os.getenv("NAVER_CAMPAIGN_ID", "").strip()
NAVER_ADGROUP_ID    = os.getenv("NAVER_ADGROUP_ID", "").strip()
NAVER_ADGROUP_NAME  = os.getenv("NAVER_ADGROUP_NAME", "").strip()

# ==== iCloud Reminders (CalDAV) ====
ICLOUD_USER = os.getenv("ICLOUD_USER", "").strip()
ICLOUD_APP_PASSWORD = os.getenv("ICLOUD_APP_PASSWORD", "").strip()
ICLOUD_REMINDER_LIST = os.getenv("ICLOUD_REMINDER_LIST", "").strip()

# CalDAV 접속 기본 URL (애플 고정)
ICLOUD_CALDAV_URL = "https://caldav.icloud.com/"


# Naver Place (리뷰/노출 감시용)
NAVER_PLACE_ID      = os.getenv("NAVER_PLACE_ID", "").strip()
# ===== kimspayback (킴스 통계) =====
KIMSPAYBACK_STATS_URL   = os.getenv("KIMSPAYBACK_STATS_URL", "").strip()
KIMSPAYBACK_STATS_TOKEN = os.getenv("KIMSPAYBACK_STATS_TOKEN", "").strip()

# ===== 두젠틀보드 대시보드 push =====
DASHBOARD_PUSH_URL    = os.getenv("DASHBOARD_PUSH_URL", "").strip()
DASHBOARD_PUSH_SECRET = os.getenv("DASHBOARD_PUSH_SECRET", "").strip()

DATA_FILE = os.path.join(DATA_DIR, "portfolio.json")
LOCK_FILE = os.path.join(DATA_DIR, "bot.lock")
UPBIT     = "https://api.upbit.com/v1"

NAVER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 12; SM-G998N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ========= KEEPALIVE HTTP =========
class _Ok(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")
        except:
            pass

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b""
            body = raw.decode("utf-8", errors="ignore").strip()

            if self.path.startswith("/reminder"):
                # 미리알림 웹훅
                try:
                    payload = json.loads(body or "{}")
                except Exception:
                    payload = {}
                try:
                    handle_reminder_webhook(payload)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(b'{"ok":true}')
                except Exception as e:
                    print("[REMINDER] webhook 처리 오류:", e)
                    self.send_response(500)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(b"error")
            else:
                # 다른 POST는 404
                self.send_response(404)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"not found")
        except Exception as e:
            print("[HTTP] do_POST 에러:", e)

    def log_message(self, *a, **k):
        return


def _start_keepalive():
    if PORT <= 0:
        return
    def _run():
        try:
            httpd = HTTPServer(("", PORT), _Ok)
            httpd.serve_forever()
        except:
            pass
    threading.Thread(target=_run, daemon=True).start()

# ========= SINGLE INSTANCE LOCK =========
def _pid_alive(pid:int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except:
        return False

def _release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except:
        pass

def _acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                old = int((f.read() or "0").strip())
            if old and _pid_alive(old):
                print(f"[LOCK] Another bot instance is running (pid={old}). Exit.")
                raise SystemExit(0)
        except:
            pass
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(_release_lock)

def _setup_signals():
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda *_: (_release_lock(), exit(0)))
        except:
            pass

_acquire_lock()
_setup_signals()

# ========= STATE LOAD/SAVE =========
def _default_state():
    return {
        "coins": {},
        "chart_auto": True,  # 🔹 차트 자동 첨부 기본값 (True: 켜짐)
        "default_threshold_pct": DEFAULT_THRESHOLD,
        "pending": {},
        "naver": {
            "auto_enabled": False,
            "schedules": [],
            "last_applied": "",
            "last_known_bid": None,
            "adgroup_id": None,
            "abtest": None,
            "rank_watch": {
                "enabled": False,
                "keyword": "",
                "marker": "",
                "interval": 300,
                "last_rank": None,       # 기본(자연) 순위만 저장
                "last_check": 0.0,
            },
            "review_watch": {
                "enabled": False,
                "interval": 180,
                "last_count": None,
                "last_check": 0.0,
            },
        },
        "gcal": {
            "enabled": False,
            "calendars": {
                "dogentle.gn@gmail.com": {
                    "name": "📅 호텔",
                    "known_events": {},
                    "last_sync": None
                },
                "v5ed5c688kem61967f0g5jg57g@group.calendar.google.com": {
                    "name": "💼 강남점",
                    "known_events": {},
                    "last_sync": None
                },
                "t2of8dh2bllmfigb6ef8ugi7r8@group.calendar.google.com": {
                    "name": "🏠 댕큐",
                    "known_events": {},
                    "last_sync": None
                }
            }
        },

        # 🔔 iCloud 미리알림 감시 설정
        "reminder": {
            "enabled": False,         # 전체 기능 ON/OFF
            "interval_min": 60,       # 반복 알림 간격(분)
            "lists": {
                "출근": {"flag_only": True},    # 출근: 깃발 있는 항목만
                "회사": {"flag_only": False},   # 회사: 오늘까지(또는 기한 없음)
                "외출": {"flag_only": False},   # 외출: 오늘까지(또는 기한 없음)
            },
            "known": {},              # 단축어에서 받아온 최신 스냅샷
        },

        "modes": {},
         # 📊 kimspayback 통계 (아침 브리핑 + 클릭 급증 알림)
        "kims_stats": {
            "daily_brief": {
                "enabled": True,
                "hour": 9,
                "minute": 0,
            },
            "spike": {
                "click_enabled": False,    # 🖱 클릭 급증 감시
                "visit_enabled": False,    # 👀 방문(PV) 급증 감시

                "interval_sec": 60,

                "click_threshold": 3,      # 클릭 급증 기준(+N)
                "visit_threshold": 3,      # 방문 급증 기준(+N)

                "last_clicks": None,       # 직전 클릭 누적(내부 저장)
                "last_pv": None,           # 직전 PV 누적(내부 저장)
                "last_check": 0.0,
            },

        },       

    }

def load_state():
    if not os.path.exists(DATA_FILE):
        return _default_state()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
    except:
        return _default_state()

    d.setdefault("coins", {})
    d.setdefault("chart_auto", True)  # 🔹 차트 자동 첨부 옵션 기본값
    d.setdefault("default_threshold_pct", DEFAULT_THRESHOLD)
    d.setdefault("pending", {})
    nav = d.setdefault("naver", {})
    nav.setdefault("auto_enabled", False)
    nav.setdefault("schedules", [])
    nav.setdefault("last_applied", "")
    nav.setdefault("last_known_bid", None)
    nav.setdefault("adgroup_id", None)
    nav.setdefault("abtest", None)

    rw = nav.setdefault("rank_watch", {})
    rw.setdefault("enabled", False)
    rw.setdefault("keyword", "")
    rw.setdefault("marker", "")
    rw.setdefault("interval", 300)
    rw.setdefault("last_rank", None)
    rw.setdefault("last_check", 0.0)

    rv = nav.setdefault("review_watch", {})
    rv.setdefault("enabled", False)
    rv.setdefault("interval", 180)
    rv.setdefault("last_count", None)
    rv.setdefault("last_check", 0.0)

    # 🔔 미리알림 기본 설정
    rem = d.setdefault("reminder", {})
    rem.setdefault("enabled", False)
    rem.setdefault("interval_min", 60)
    lists = rem.setdefault("lists", {})
    lists.setdefault("출근", {"flag_only": True})
    lists.setdefault("회사", {"flag_only": False})
    lists.setdefault("외출", {"flag_only": False})
    rem.setdefault("known", {})


    d.setdefault("modes", {})
    # 📊 kimspayback 통계 기본값 보정
    ks = d.setdefault("kims_stats", {})
    dbf = ks.setdefault("daily_brief", {})
    dbf.setdefault("enabled", True)
    dbf.setdefault("hour", 9)
    dbf.setdefault("minute", 0)

    sp = ks.setdefault("spike", {})
    sp.setdefault("click_enabled", False)
    sp.setdefault("visit_enabled", False)
    sp.setdefault("interval_sec", 60)

    sp.setdefault("click_threshold", 3)
    sp.setdefault("visit_threshold", 3)

    sp.setdefault("last_clicks", None)
    sp.setdefault("last_pv", None)
    sp.setdefault("last_check", 0.0)



    # 코인 데이터 마이그레이션
    changed = False
    for m, info in d["coins"].items():
        info.setdefault("triggers", [])
        info.setdefault("prev_price", None)
        info.setdefault("last_notified_price", None)
        info.setdefault("last_alert_ts", None)
        for k in ("target_price", "stop_price"):
            if info.get(k):
                try:
                    v = float(info[k])
                    if v not in info["triggers"]:
                        info["triggers"].append(v)
                        changed = True
                except:
                    pass
                info[k] = None

    if changed:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DATA_FILE)

    return d

# ===== Google Calendar API 연결 =====
def init_gcal_service():
    """Google Calendar API 서비스 객체 생성"""
    cred_path = os.getenv("GCAL_CREDENTIALS_PATH")
    creds = service_account.Credentials.from_service_account_file(
        cred_path,
        scopes=["https://www.googleapis.com/auth/calendar.readonly"]
    )
    service = build("calendar", "v3", credentials=creds)
    return service

# ===== iCloud Reminders (CalDAV) 연결/조회 =====
def _init_icloud_client():
    """iCloud CalDAV 클라이언트 생성"""
    # 🔒 caldav 모듈이 없는 환경에서는 아예 비활성화
    if DAVClient is None:
        print("[REMINDER] caldav not available → iCloud reminder disabled")
        return None

    # 🔒 계정 정보 없으면 비활성화
    if not (ICLOUD_USER and ICLOUD_APP_PASSWORD):
        print("[REMINDER] iCloud credential missing → reminder disabled")
        return None

    url = ICLOUD_CALDAV_URL
    try:
        client = DAVClient(
            url,
            username=ICLOUD_USER,
            password=ICLOUD_APP_PASSWORD
        )
        return client
    except Exception as e:
        print("[REMINDER] iCloud CalDAV 로그인 실패:", e)
        return None


def _get_all_calendars_recursively(parent, collected):
    """CalDAV 캘린더 또는 그룹 아래의 모든 캘린더를 재귀적으로 수집"""
    try:
        # 현재 노드 자체가 캘린더라면 추가
        if hasattr(parent, "name") and parent.name:
            name = parent.name.strip()
            if name:  # 빈 이름은 무시
                collected[name] = parent

        # sub-calendars 있으면 모두 재귀 탐색
        if hasattr(parent, "subcomponents"):
            for child in parent.subcomponents():
                _get_all_calendars_recursively(child, collected)

    except Exception as e:
        print("[REMINDER] 재귀 캘린더 탐색 오류:", e)


def _get_reminder_collections(client):
    """
    iCloud Reminders 모든 목록 가져오기
    (최상위 + 하위 캘린더(sub-calendars)까지 전부 탐색)
    """
    try:
        principal = client.principal()  # ✅ 오타 수정
        home = principal.calendar_home_set()
        calendars = home.calendars()
    except Exception as e:
        print("[REMINDER] iCloud 캘린더 목록 조회 실패:", e)
        return {}

    collected = {}

    print("[REMINDER] === 전체 캘린더 재귀 탐색 시작 ===")

    for cal in calendars:
        try:
            _get_all_calendars_recursively(cal, collected)
        except Exception as e:
            print("[REMINDER] 최상위 캘린더 탐색 오류:", e)

    print("[REMINDER] ▼ 최종 수집된 모든 목록:")
    for name in collected.keys():
        print("  -", name)

    return collected




def _extract_due_info(vtodo):
    """
    VTODO에서 '오늘 마감인지', '기한 시각'을 계산.
    return: (today_due: bool, due_ts: float or None)
    """
    comp = vtodo.get("due")
    if not comp:
        return False, None

    val = comp.value
    if isinstance(val, datetime):
        dt = val.astimezone(KST)
    else:
        # 날짜만 있는 경우: 오전 9시로 고정
        dt = datetime.combine(val, datetime.min.time()).replace(
            hour=9, minute=0, tzinfo=KST
        )

    today = datetime.now(KST).date()
    if dt.date() != today:
        return False, None

    return True, dt.timestamp()


def _is_completed(vtodo):
    comp = vtodo.get("completed")
    return bool(comp and comp.value)


def _is_flagged(vtodo):
    """
    출근 리스트에서 '깃발'만 보는 용도.
    iCloud VTODO의 X-APPLE-FLAGS 를 간단히 체크.
    """
    try:
        x_flags = vtodo.get("x-apple-flags")
        if x_flags and str(x_flags.value).strip() != "0":
            return True
    except Exception:
        pass
    return False


def _collect_today_reminders():
    """
    iCloud Drive 에 저장된 snapshot.json 을 읽어서
    state["reminder"]["known"] 을 갱신하고,
    '출근/회사/외출' 전체 미리알림 목록을 반환한다.

    ⚠️ 여기서는 '미래 일정'도 모두 저장만 한다.
    - 오늘/과거/미래를 필터링하는 건
      build_today_reminder_snapshot / build_upcoming_reminder_snapshot /
      _refresh_reminder_known_from_snapshot 에서 처리한다.
    """
    global state

    rem = state.setdefault("reminder", {})
    known = rem.setdefault("known", {})
    lists_cfg = rem.setdefault("lists", {})
    lists_cfg.setdefault("출근", {"flag_only": True})
    lists_cfg.setdefault("회사", {"flag_only": False})
    lists_cfg.setdefault("외출", {"flag_only": False})

    # 스냅샷 파일이 없으면 known 을 비우고 종료
    if not os.path.exists(REMINDER_SNAPSHOT_FILE):
        print(f"[REMINDER] 스냅샷 파일이 없습니다: {REMINDER_SNAPSHOT_FILE}")
        rem["known"] = {}
        save_state()
        return []

    try:
        with open(REMINDER_SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print("[REMINDER] 스냅샷 파일 읽기 실패:", e)
        return []

    raw = payload.get("reminders")
    raw_items: List[Dict[str, Any]] = []

    # 1) 리스트 형태
    if isinstance(raw, list):
        raw_items = raw

    # 2) 여러 줄 JSON 문자열 형태
    elif isinstance(raw, str):
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        for ln in lines:
            try:
                obj = json.loads(ln)
                if isinstance(obj, dict):
                    raw_items.append(obj)
            except Exception as e:
                print("[REMINDER] JSON line parse 실패:", e, ln[:80])

    if not raw_items:
        print("[REMINDER] 스냅샷에 유효한 항목이 없습니다.")
        rem["known"] = {}
        save_state()
        return []

    def _parse_completed(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "y", "완료", "완료함")
        return False

    def _parse_flagged(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() not in ("0", "false", "no", "없음", "")
        return False

    new_known: Dict[str, Dict[str, Any]] = {}
    seen_uids = set()

    for r in raw_items:
        try:
            uid = str(r.get("id") or "").strip()
            if not uid:
                uid = f"{r.get('list')}/{r.get('title')}"

            list_name = str(r.get("list") or "").strip() or "기타"
            title = str(r.get("title") or "(제목 없음)")
            due_raw = r.get("due")
            completed = _parse_completed(r.get("completed"))
            flagged = _parse_flagged(r.get("flagged"))

            # 기한 → 타임스탬프 (실패하면 None)
            due_ts = _parse_shortcut_due_to_ts(due_raw)

            # ✅ 출근: 깃발만 감시
            if list_name == "출근":
                if lists_cfg.get("출근", {}).get("flag_only", True) and not flagged:
                    continue

            # ✅ 설정에 없는 리스트는 아예 무시
            if list_name not in lists_cfg:
                continue

            old = known.get(uid, {})
            last_alert_ts = float(old.get("last_alert_ts", 0.0) or 0.0)

            new_known[uid] = {
                "uid": uid,
                "list_name": list_name,
                "title": title,
                "due_raw": due_raw,
                "due_ts": due_ts,
                "completed": completed,
                "flagged": flagged,
                "last_alert_ts": last_alert_ts,
            }
            seen_uids.add(uid)

        except Exception as e:
            print("[REMINDER] 스냅샷 항목 파싱 실패:", e)
            continue

    # 스냅샷에서 빠진 항목은 known 에서 제거
    for uid in list(known.keys()):
        if uid not in seen_uids:
            known.pop(uid, None)

    rem["known"] = new_known
    rem["last_sync_ts"] = time.time()
    save_state()

    return list(new_known.values())



def _refresh_reminder_known_from_snapshot():
    """
    snapshot.json 을 읽어서 state["reminder"]["known"] 을 갱신한다.
    출근/회사/외출 리스트만 대상으로 적용.
    """
    global state

    rem = state.setdefault("reminder", {})
    lists_cfg = rem.setdefault("lists", {})
    old_known: Dict[str, Dict[str, Any]] = rem.get("known", {}) or {}

    items = _collect_today_reminders()
    if not items:
        # 스냅샷이 없으면 기존 known 유지
        return

    today = datetime.now(KST).date()
    new_known: Dict[str, Dict[str, Any]] = {}

    for it in items:
        try:
            uid = str(it.get("uid") or "")
            if not uid:
                continue

            list_name = str(it.get("list_name") or "")
            title = str(it.get("title") or "(제목 없음)")
            due_raw = it.get("due_raw")
            due_ts = it.get("due_ts")
            completed = bool(it.get("completed", False))
            flagged = bool(it.get("flagged", False))

            # 설정에 없는 리스트는 무시
            if list_name not in lists_cfg:
                continue

            # 출근: 깃발 달린 것만
            if list_name == "출근":
                if lists_cfg.get("출근", {}).get("flag_only", True) and not flagged:
                    continue

            # 회사/외출: 오늘까지(또는 과거)만
            if list_name in ("회사", "외출"):
                if isinstance(due_ts, (int, float)) and due_ts > 0:
                    due_date = datetime.fromtimestamp(due_ts, tz=KST).date()
                    if due_date > today:
                        # 미래 일정은 아직 감시 대상 아님
                        continue

            old = old_known.get(uid) or {}
            last_alert_ts = float(old.get("last_alert_ts", 0.0) or 0.0)

            new_known[uid] = {
                "uid": uid,
                "list_name": list_name,
                "title": title,
                "due_raw": due_raw,
                "due_ts": due_ts,
                "completed": completed,
                "flagged": flagged,
                "last_alert_ts": last_alert_ts,
            }
        except Exception as e:
            print("[REMINDER] _refresh_reminder_known_from_snapshot 항목 오류:", e)
            continue

    rem["known"] = new_known
    rem["last_sync_ts"] = time.time()
    save_state()
    # print(f"[REMINDER] snapshot 기반 known 갱신 완료 (항목 {len(new_known)}개)")

def _format_short_due(due_ts: Any) -> str:
    """
    due_ts(타임스탬프)를 KST 기준으로
    - 날짜만 있으면: MM/DD
    - 시간까지 있으면: MM/DD HH:MM
    로 짧게 표시한다.
    """
    if not isinstance(due_ts, (int, float)):
        return ""

    try:
        dt = datetime.fromtimestamp(float(due_ts), tz=KST)
    except Exception:
        return ""

    date_str = f"{dt.month:02d}/{dt.day:02d}"

    # 자정(00:00)이면 '날짜만 있는 일정'으로 간주
    if dt.hour == 0 and dt.minute == 0:
        return date_str

    return f"{date_str} {dt.hour:02d}:{dt.minute:02d}"


def build_today_reminder_snapshot():
    """
    오늘 업무 스냅샷 문자열 생성
    - 출근: 깃발 + 미완료 → 기한 상관없이 모두 표시
    - 회사/외출: 미완료 + 기한 있음 + 오늘까지(과거~오늘)만 표시
    """
    rem = state.setdefault("reminder", {})

    # 항상 최신 스냅샷 기준으로 known 을 갱신
    _collect_today_reminders()

    known = rem.get("known", {})
    if not known:
        return (
            "📋 오늘 업무\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "표시할 업무가 없어요.\n"
            "(아이폰에서 미리알림 스냅샷 단축어를 한 번 실행해 주세요.)"
        )

    today = datetime.now(KST).date()

    lines: list[str] = []
    lines.append("📋 오늘 업무")
    lines.append("━━━━━━━━━━━━━━━━━━")

    def _collect_items(list_name: str) -> list[dict]:
        items: list[dict] = []
        for v in known.values():
            if v.get("list_name") != list_name:
                continue
            if v.get("completed"):
                continue

            flagged = bool(v.get("flagged", False))
            due_ts = v.get("due_ts")

            if list_name == "출근":
                # ✅ 출근: 깃발 + 미완료 → 기한 상관없이 모두
                if not flagged:
                    continue
                items.append(v)
                continue

            if list_name in ("회사", "외출"):
                # ✅ 회사/외출: 미완료 + 기한 있는 것만 + 오늘까지
                if not isinstance(due_ts, (int, float)) or not due_ts:
                    continue
                try:
                    d = datetime.fromtimestamp(float(due_ts), tz=KST).date()
                except Exception:
                    continue
                if d > today:
                    continue
                items.append(v)
                continue

        # 기한, 제목 기준 정렬 (출근은 기한 없을 수 있으므로 0 우선)
        items.sort(
            key=lambda x: (
                float(x.get("due_ts") or 0.0),
                str(x.get("title") or ""),
            )
        )
        return items

    any_section = False

    # 🚀 출근
    commute_items = _collect_items("출근")
    if commute_items:
        any_section = True
        lines.append("")
        lines.append("🚀 출근")
        for v in commute_items:
            title = v.get("title") or "(제목 없음)"
            due_ts = v.get("due_ts")
            due_str = _format_short_due(due_ts)
            if due_str:
                lines.append(f"• {due_str} · {title}")
            else:
                lines.append(f"• {title}")

    # 🏢 회사
    company_items = _collect_items("회사")
    if company_items:
        any_section = True
        lines.append("")
        lines.append("🏢 회사")
        for v in company_items:
            title = v.get("title") or "(제목 없음)"
            due_str = _format_short_due(v.get("due_ts"))
            if due_str:
                lines.append(f"• {due_str} · {title}")
            else:
                lines.append(f"• {title}")

    # 🚶‍♂️ 외출
    outside_items = _collect_items("외출")
    if outside_items:
        any_section = True
        lines.append("")
        lines.append("🚶‍♂️ 외출")
        for v in outside_items:
            title = v.get("title") or "(제목 없음)"
            due_str = _format_short_due(v.get("due_ts"))
            if due_str:
                lines.append(f"• {due_str} · {title}")
            else:
                lines.append(f"• {title}")

    if not any_section:
        return (
            "📋 오늘 업무\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "표시할 업무가 없어요.\n"
            "(아이폰에서 미리알림 스냅샷 단축어를 한 번 실행해 주세요.)"
        )

    return "\n".join(lines)


def build_upcoming_reminder_snapshot(days: int = 7) -> str:
    """
    앞으로 N일간 업무 스냅샷
    - 출근: 깃발 + 미완료 → 기한 상관없이 모두 표시
    - 회사/외출: 미완료 + 기한 있음 + (오늘 이후 ~ N일 이내)만 표시
    """
    rem = state.setdefault("reminder", {})

    _collect_today_reminders()
    known = rem.get("known", {})
    if not known:
        return (
            f"📆 앞으로 {days}일 업무\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "표시할 업무가 없어요.\n"
            "(아이폰에서 미리알림 스냅샷 단축어를 한 번 실행해 주세요.)"
        )

    today = datetime.now(KST).date()
    end = today + timedelta(days=days)

    lines: list[str] = []
    lines.append(f"📆 앞으로 {days}일 업무")
    lines.append("━━━━━━━━━━━━━━━━━━")

    def _collect_items(list_name: str) -> list[dict]:
        items: list[dict] = []
        for v in known.values():
            if v.get("list_name") != list_name:
                continue
            if v.get("completed"):
                continue

            flagged = bool(v.get("flagged", False))
            due_ts = v.get("due_ts")

            if list_name == "출근":
                # ✅ 출근: 깃발 + 미완료 → 기한 상관없이 모두
                if not flagged:
                    continue
                items.append(v)
                continue

            if list_name in ("회사", "외출"):
                # ✅ 회사/외출: 미완료 + 기한 있는 것만 + 오늘 이후 ~ N일 이내
                if not isinstance(due_ts, (int, float)) or not due_ts:
                    continue
                try:
                    d = datetime.fromtimestamp(float(due_ts), tz=KST).date()
                except Exception:
                    continue
                if not (today < d <= end):
                    continue
                items.append(v)
                continue

        items.sort(
            key=lambda x: (
                float(x.get("due_ts") or 0.0),
                str(x.get("title") or ""),
            )
        )
        return items

    any_section = False

    # 🚀 출근
    commute_items = _collect_items("출근")
    if commute_items:
        any_section = True
        lines.append("")
        lines.append("🚀 출근")
        for v in commute_items:
            title = v.get("title") or "(제목 없음)"
            due_str = _format_short_due(v.get("due_ts"))
            if due_str:
                lines.append(f"• {due_str} · {title}")
            else:
                lines.append(f"• {title}")

    # 🏢 회사
    company_items = _collect_items("회사")
    if company_items:
        any_section = True
        lines.append("")
        lines.append("🏢 회사")
        for v in company_items:
            title = v.get("title") or "(제목 없음)"
            due_str = _format_short_due(v.get("due_ts"))
            if due_str:
                lines.append(f"• {due_str} · {title}")
            else:
                lines.append(f"• {title}")

    # 🚶‍♂️ 외출
    outside_items = _collect_items("외출")
    if outside_items:
        any_section = True
        lines.append("")
        lines.append("🚶‍♂️ 외출")
        for v in outside_items:
            title = v.get("title") or "(제목 없음)"
            due_str = _format_short_due(v.get("due_ts"))
            if due_str:
                lines.append(f"• {due_str} · {title}")
            else:
                lines.append(f"• {title}")

    if not any_section:
        return (
            f"📆 앞으로 {days}일 업무\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "표시할 업무가 없어요.\n"
            "(아이폰에서 미리알림 스냅샷 단축어를 한 번 실행해 주세요.)"
        )

    return "\n".join(lines)



def _parse_shortcut_due_to_ts(due_raw):
    """
    단축어에서 넘어온 'due' 값을 timestamp(초)로 변환.
    - 숫자면 그대로
    - ISO 형식 문자열이면 strptime / fromisoformat
    - '2025. 11. 24. 오전 12:00' 같은 한국식 형식도 직접 파싱
    """
    if due_raw is None or due_raw == "":
        return None

    # 이미 숫자(초)로 온 경우
    if isinstance(due_raw, (int, float)):
        try:
            return float(due_raw)
        except Exception:
            return None

    s = str(due_raw).strip()
    if not s:
        return None

    # ✅ 1) 한국식 형식: "2025. 11. 24. 오전 12:00"
    m = re.match(
        r"^(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(오전|오후)\s*(\d{1,2}):(\d{2})$",
        s,
    )
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        ampm = m.group(4)
        hour = int(m.group(5))
        minute = int(m.group(6))

        # 오전/오후 → 24시간제
        if ampm == "오전":
            if hour == 12:
                hour = 0
        else:  # "오후"
            if hour < 12:
                hour += 12

        dt = datetime(year, month, day, hour, minute, tzinfo=KST)
        return dt.timestamp()

    # ✅ 2) 한국식 날짜만 있는 형식: "2025. 11. 24."
    m = re.match(r"^(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        # 시간은 대충 오전 9시로 고정
        dt = datetime(year, month, day, 9, 0, tzinfo=KST)
        return dt.timestamp()

    # ✅ 3) 나머지(ISO 스타일 등)는 기존 포맷들로 시도
    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            else:
                dt = dt.astimezone(KST)
            return dt.timestamp()
        except Exception:
            continue

    # 마지막으로 fromisoformat도 시도
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        else:
            dt = dt.astimezone(KST)
        return dt.timestamp()
    except Exception:
        return None


def handle_reminder_webhook(payload: dict):
    """
    iOS 단축어에서 보낸 미리알림 JSON을 받아서
    state["reminder"]["known"]을 갱신한다.

    payload 예시:
    {
        "sent_at": "...",
        "reminders": [
            {
                "id": "...",        # 단축어에서 URL 또는 고유 ID로 설정
                "title": "...",
                "list": "출근" / "회사" / "외출",
                "due": "...",       # 문자열 또는 날짜
                "completed": ...,
                "flagged": ...,
            },
            ...
        ]
    }
    """
    global state

    rem = state.setdefault("reminder", {})
    lists_cfg = rem.setdefault("lists", {})
    known = rem.setdefault("known", {})

    items = payload.get("reminders") or []
    seen_uids = set()

    today = datetime.now(KST).date()

    for raw in items:
        try:
            uid = str(raw.get("id") or "").strip()
            if not uid:
                continue

            list_name = str(raw.get("list") or "").strip() or "기타"
            title = str(raw.get("title") or "(제목 없음)")
            due_raw = raw.get("due")
            completed_raw = raw.get("completed")
            flagged_raw = raw.get("flagged")

            # 완료 여부
            completed = False
            if isinstance(completed_raw, bool):
                completed = completed_raw
            elif isinstance(completed_raw, (int, float)):
                completed = bool(completed_raw)
            elif isinstance(completed_raw, str):
                completed = completed_raw.strip().lower() in (
                    "1", "true", "yes", "y", "완료", "완료함"
                )

            # 깃발 여부
            flagged = False
            if isinstance(flagged_raw, bool):
                flagged = flagged_raw
            elif isinstance(flagged_raw, (int, float)):
                flagged = bool(flagged_raw)
            elif isinstance(flagged_raw, str):
                flagged = flagged_raw.strip().lower() not in (
                    "0", "false", "no", "없음", ""
                )

            # 기한 파싱
            due_ts = _parse_shortcut_due_to_ts(due_raw)
            due_date = None
            if due_ts is not None:
                due_date = datetime.fromtimestamp(due_ts, tz=KST).date()

            # 1) 출근: 깃발 항목만
            if list_name == "출근":
                if lists_cfg.get("출근", {}).get("flag_only", True) and not flagged:
                    continue

            # 2) 회사/외출: '오늘 또는 과거'만 대상
            if list_name in ("회사", "외출"):
                if due_date is not None and due_date > today:
                    # 미래 예정이면 아직 알림 대상 아님
                    continue

            # 설정에 없는 리스트는 무시
            if list_name not in lists_cfg:
                continue

            info = known.get(uid, {})
            last_alert_ts = float(info.get("last_alert_ts", 0.0) or 0.0)

            known[uid] = {
                "uid": uid,
                "list_name": list_name,
                "title": title,
                "due_raw": due_raw,
                "due_ts": due_ts,
                "completed": completed,
                "flagged": flagged,
                "last_alert_ts": last_alert_ts,
            }
            seen_uids.add(uid)

        except Exception as e:
            print("[REMINDER] webhook 항목 파싱 오류:", e)
            continue

    # 이번 스냅샷에 없는 uid는 제거(완료/삭제된 것으로 간주)
    for uid in list(known.keys()):
        if uid not in seen_uids:
            known.pop(uid, None)

    rem["last_sync_ts"] = time.time()
    save_state()
    print(f"[REMINDER] webhook 처리 완료 (항목 {len(known)}개 유지)")


_state_lock = threading.Lock()

def save_state():
    with _state_lock:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DATA_FILE)

# ===== 두젠틀보드 대시보드 push 함수 =====

def push_to_dashboard(snap_type: str, payload: dict) -> None:
    """
    두젠틀보드 서버로 스냅샷을 push한다.
    실패해도 봇 동작에 영향 없음.
    snap_type: 'crypto' | 'naver'
    """
    if not DASHBOARD_PUSH_URL or not DASHBOARD_PUSH_SECRET:
        return
    try:
        r = requests.post(
            DASHBOARD_PUSH_URL,
            json={"type": snap_type, "payload": payload},
            headers={
                "X-Push-Secret": DASHBOARD_PUSH_SECRET,
                "Content-Type": "application/json",
            },
            timeout=5,
        )
        if not QUIET_LOG:
            print(f"[DASHBOARD] push {snap_type} → {r.status_code}")
    except Exception as e:
        if not QUIET_LOG:
            print(f"[DASHBOARD] push {snap_type} 실패: {e}")


def _build_crypto_payload() -> dict:
    """state['coins'] + Upbit 실시간 가격 → 대시보드용 코인 스냅샷"""
    coins_out = {}
    total_invested = 0.0
    total_eval = 0.0

    for market, info in state.get("coins", {}).items():
        try:
            cur = get_price(market)
        except Exception:
            cur = 0.0
        avg = float(info.get("avg_price") or 0.0)
        qty = float(info.get("qty") or 0.0)
        buy_amt  = avg * qty
        eval_amt = cur * qty
        pnl_krw  = eval_amt - buy_amt
        pnl_pct  = 0.0 if buy_amt == 0 else (eval_amt / buy_amt - 1) * 100

        total_invested += buy_amt
        total_eval     += eval_amt

        coins_out[market] = {
            "avg_price":     avg,
            "qty":           qty,
            "threshold_pct": float(info.get("threshold_pct") or state.get("default_threshold_pct", 1.0)),
            "triggers":      info.get("triggers", []),
            "current_price": cur,
            "pnl_pct":       round(pnl_pct, 2),
            "pnl_krw":       round(pnl_krw),
            "eval_krw":      round(eval_amt),
        }

    total_pnl = total_eval - total_invested
    total_pnl_pct = 0.0 if total_invested == 0 else (total_eval / total_invested - 1) * 100

    return {
        "coins":            coins_out,
        "total_invested_krw": round(total_invested),
        "total_eval_krw":     round(total_eval),
        "total_pnl_krw":      round(total_pnl),
        "total_pnl_pct":      round(total_pnl_pct, 2),
    }


def _build_naver_payload() -> dict:
    """state['naver'] → 대시보드용 네이버 광고 스냅샷"""
    nav = state.get("naver", {})
    rw  = nav.get("rank_watch", {})
    rv  = nav.get("review_watch", {})
    return {
        "last_known_bid":  nav.get("last_known_bid"),
        "auto_enabled":    nav.get("auto_enabled", False),
        "schedules_count": len(nav.get("schedules", [])),
        "rank_watch": {
            "enabled":       rw.get("enabled", False),
            "keyword":       rw.get("keyword", ""),
            "last_rank":     rw.get("last_rank"),
            "last_check_ts": rw.get("last_check", 0.0),
        },
        "review_watch": {
            "enabled":       rv.get("enabled", False),
            "last_count":    rv.get("last_count"),
            "last_check_ts": rv.get("last_check", 0.0),
        },
    }


def _dashboard_crypto_push_job(context):
    """60초마다 코인 스냅샷을 대시보드로 push"""
    if not state.get("coins"):
        return
    try:
        payload = _build_crypto_payload()
        push_to_dashboard("crypto", payload)
    except Exception as e:
        if not QUIET_LOG:
            print(f"[DASHBOARD] crypto push job 에러: {e}")

state = load_state()

if "default_threshold_pct" not in state:
    state["default_threshold_pct"] = float(DEFAULT_THRESHOLD)
    save_state()

# ========= MODE / KEYBOARD =========
def get_mode(cid):
    return state.setdefault("modes", {}).get(str(cid), "coin")

def set_mode(cid, mode):
    state.setdefault("modes", {})[str(cid)] = mode
    save_state()

def MAIN_KB(cid=None):
    """
    현재 모드에 따라 하단 ReplyKeyboard 구성

    mode_pa      : 개인비서 (도움말, 메뉴)
    mode_naver   : 네이버 광고 (기존 그대로)
    mode_coin    : 자산관리 (기존 코인 기능 그대로)
    mode_random  : 랜덤문구 (호텔, 도움말, 메뉴)
    """
    mode = get_mode(cid) if cid is not None else "mode_coin"

    # 1) 개인비서 모드
    if mode == "pa" or mode == "mode_pa":
        return ReplyKeyboardMarkup(
            [
                ["📅 캘린더 메뉴", "🗂 업무 스케줄러"],
                ["💰 매출", "📊 킴스 통계"],
                ["도움말", "메뉴"],
            ],
            resize_keyboard=True,
        )



    # 2) 네이버 광고 모드 (기존 그대로)
    if mode == "naver" or mode == "mode_naver":
        return ReplyKeyboardMarkup(
            [
                ["광고상태", "노출현황", "리뷰현황"],
                ["광고시간", "광고설정", "입찰추정"],
                ["광고자동", "노출감시", "리뷰감시"],
                ["도움말", "메뉴"],
            ],
            resize_keyboard=True,
        )

    # 3) 자산관리 모드 (기존 코인 기능 그대로 사용)
    if mode == "coin" or mode == "mode_coin":
        return ReplyKeyboardMarkup(
            [
                ["보기", "상태", "도움말"],
                ["코인", "가격", "임계값"],
                ["평단", "수량", "지정가"],
                ["차트", "차트알림", "메뉴"],  # 🔹 새 버튼 두 개 추가
            ],

            resize_keyboard=True,
        )

    # 4) 랜덤문구 모드 (호텔 랜덤문구 + 도움말 + 메뉴)
    if mode == "random" or mode == "mode_random":
        return ReplyKeyboardMarkup(
            [
                ["호텔", "도움말", "메뉴"],
            ],
            resize_keyboard=True,
        )

    # 예외/기본: 자산관리 모드로
    return ReplyKeyboardMarkup(
        [
            ["보기", "상태", "도움말"],
            ["코인", "가격", "임계값"],
            ["평단", "수량", "지정가"],
            ["메뉴"],
        ],
        resize_keyboard=True,
    )

def mode_inline_kb():
    """
    상단 모드 선택 인라인 키보드
    1. 개인비서
    2. 네이버
    3. 자산관리
    4. 랜덤문구
    """
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("개인비서", callback_data="mode_pa")],
            [InlineKeyboardButton("네이버", callback_data="mode_naver")],
            [InlineKeyboardButton("자산관리", callback_data="mode_coin")],
            [InlineKeyboardButton("랜덤문구", callback_data="mode_random")],
        ]
    )


COIN_MODE_KB = ReplyKeyboardMarkup(
    [["추가", "삭제"], ["취소"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)
CANCEL_KB = ReplyKeyboardMarkup(
    [["취소"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)
NAVER_SCHEDULE_MENU_KB = ReplyKeyboardMarkup(
    [
        ["추가", "삭제"],
        ["전체초기화", "취소"],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)

def coin_kb(include_cancel=True):
    syms = [m.split("-")[1] for m in state["coins"].keys()] or ["BTC", "ETH", "SOL"]
    rows = [syms[i:i+3] for i in range(0, len(syms), 3)]
    if include_cancel:
        rows.append(["취소"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)

# ========= UTIL =========
def only_owner(update):
    return (not CHAT_ID) or (str(update.effective_chat.id) == CHAT_ID)

def krw_symbol(sym):
    s = sym.upper().strip()
    return s if "-" in s else "KRW-" + s

def fmt(n):
    try:
        x = float(n)
        return f"{x:,.0f}" if abs(x) >= 1 else f"{x:,.6f}".rstrip("0").rstrip(".")
    except:
        return str(n)
# ========= kimspayback (킴스 통계) =========
def _kims_stats_fetch(params: dict) -> dict:
    """
    kimspayback stats_api.php 호출해서 JSON(dict) 반환
    실패 시 {"_error": "..."} 형태로 반환
    """
    if not KIMSPAYBACK_STATS_URL or not KIMSPAYBACK_STATS_TOKEN:
        return {"_error": "KIMSPAYBACK_STATS_URL 또는 TOKEN이 설정되지 않았어요. (.env 확인)"}

    q = dict(params or {})
    q["token"] = KIMSPAYBACK_STATS_TOKEN

    try:
        r = requests.get(KIMSPAYBACK_STATS_URL, params=q, timeout=8)
    except Exception as e:
        return {"_error": f"요청 실패: {e}"}

    if r.status_code != 200:
        return {"_error": f"HTTP {r.status_code}: {r.text[:120]}"}

    try:
        return r.json()
    except Exception:
        return {"_error": "응답 JSON 파싱 실패"}

def _kims_stats_format(d: dict, top_n: int = 5) -> str:
    """
    stats_api.php 결과를 텔레그램용 텍스트로 변환
    """
    if not isinstance(d, dict):
        return "⚠️ 통계 데이터가 이상해요."

    if d.get("_error"):
        return f"⚠️ 킴스 통계 조회 실패\n{d.get('_error')}"

    basic = d.get("basic", {}) or {}
    pv = int(basic.get("pv", 0) or 0)
    uv = int(basic.get("uv", 0) or 0)

    by_ref = d.get("by_referrer", {}) or {}
    direct = int(by_ref.get("direct", 0) or 0)
    naver = int(by_ref.get("naver", 0) or 0)
    google = int(by_ref.get("google", 0) or 0)
    other = int(by_ref.get("other", 0) or 0)

    by_dev = d.get("by_device", {}) or {}
    desktop = int(by_dev.get("desktop", 0) or 0)
    mobile = int(by_dev.get("mobile", 0) or 0)
    dev_other = int(by_dev.get("other", 0) or 0)

    clicks = d.get("clicks", {}) or {}
    telegram_total = int(clicks.get("telegram_total", 0) or 0)
    signup_by_card = clicks.get("signup_by_card", {}) or {}

    # 가입하기 클릭(카드 전부 0 포함)
    signup_lines = []
    if isinstance(signup_by_card, dict) and signup_by_card:
        # label 기준으로 보기 좋게 정렬
        items = []
        for cid, info in signup_by_card.items():
            if isinstance(info, dict):
                label = str(info.get("label", cid))
                cnt = int(info.get("count", 0) or 0)
            else:
                label = str(cid)
                cnt = int(info or 0)
            items.append((label, cnt))
        items.sort(key=lambda x: x[0])
        for label, cnt in items:
            signup_lines.append(f"• {label} 가입하기: {cnt}")
    else:
        signup_lines.append("• (데이터 없음)")

    # 상세 TOP N
    top_detail = d.get("top_detail_ids", []) or []
    top_lines = []
    if isinstance(top_detail, list) and top_detail:
        for row in top_detail[:max(0, int(top_n))]:
            try:
                did = str(row.get("detail_id", ""))
                cnt = int(row.get("pv", 0) or 0)
            except Exception:
                continue
            if did:
                top_lines.append(f"• {did}: {cnt}")
    # top_lines 없으면 아예 섹션 생략 가능

    lines = []
    lines.append("👀 요약")
    lines.append(f"• PV: {pv} / UV: {uv}")
    lines.append("")
    lines.append("🌐 유입경로")
    lines.append(f"• direct: {direct} / naver: {naver} / google: {google} / other: {other}")
    lines.append("")
    lines.append("📱 기기")
    lines.append(f"• desktop: {desktop} / mobile: {mobile} / other: {dev_other}")
    lines.append("")
    lines.append("🖱 가입하기 클릭")
    lines.extend(signup_lines)
    lines.append("")
    lines.append("💬 텔레그램 문의 클릭")
    lines.append(f"• 총합: {telegram_total}")

    if top_lines:
        lines.append("")
        lines.append("🏷 상세페이지 PV TOP")
        lines.extend(top_lines)

    return "\n".join(lines)

def _kims_stats_show_range(update, range_key: str, title: str):
    d = _kims_stats_fetch({"range": range_key})
    msg = f"📊 킴스 통계 ({title})\n\n" + _kims_stats_format(d, top_n=5)
    reply(update, msg, kb=KIMS_STATS_MENU_KB)

def _kims_sum_clicks(d: dict) -> int:
    """
    today/yesterday 통계 JSON에서 '총 클릭수'를 계산
    = telegram_total + (signup_by_card 전체 합)
    """
    if not isinstance(d, dict) or d.get("_error"):
        return 0

    clicks = d.get("clicks", {}) or {}
    telegram_total = int(clicks.get("telegram_total", 0) or 0)

    signup_by_card = clicks.get("signup_by_card", {}) or {}
    signup_sum = 0
    if isinstance(signup_by_card, dict):
        for _, info in signup_by_card.items():
            if isinstance(info, dict):
                signup_sum += int(info.get("count", 0) or 0)
            else:
                try:
                    signup_sum += int(info or 0)
                except Exception:
                    pass

    return telegram_total + signup_sum


def _kims_get_pv_uv(d: dict) -> tuple[int, int]:
    if not isinstance(d, dict) or d.get("_error"):
        return 0, 0
    basic = d.get("basic", {}) or {}
    pv = int(basic.get("pv", 0) or 0)
    uv = int(basic.get("uv", 0) or 0)
    return pv, uv


def _kims_daily_brief_job(ctx):
    """
    🌅 매일 아침 브리핑:
    - 어제 통계 + 전날 대비 UV 증감(▲▼)
    """
    # 어제
    d_y = _kims_stats_fetch({"range": "yesterday"})
    pv_y, uv_y = _kims_get_pv_uv(d_y)
    clicks_y = _kims_sum_clicks(d_y)

    # 그 전날(어제의 전날): from/to로 계산
    now = datetime.now(KST).date()
    yday = now - timedelta(days=1)
    day_before = now - timedelta(days=2)

    d_b = _kims_stats_fetch({
        "from": day_before.strftime("%Y-%m-%d"),
        "to": day_before.strftime("%Y-%m-%d"),
    })
    _, uv_b = _kims_get_pv_uv(d_b)

    # 증감(uv 기준)
    delta = uv_y - uv_b
    if uv_b > 0:
        pct = (delta / uv_b) * 100.0
    else:
        pct = 0.0

    if delta > 0:
        arrow = "▲"
        delta_str = f"+{delta}"
        pct_str = f"(+{pct:.1f}%)" if uv_b > 0 else ""
    elif delta < 0:
        arrow = "▼"
        delta_str = f"{delta}"
        pct_str = f"({pct:.1f}%)" if uv_b > 0 else ""
    else:
        arrow = "⏺"
        delta_str = "0"
        pct_str = "(0.0%)" if uv_b > 0 else ""

    # 메시지 구성(깔끔 버전)
    lines = []
    lines.append("🌅 아침 브리핑 (어제)")
    lines.append("")
    lines.append(f"👀 요약: PV {pv_y} / UV {uv_y}")
    lines.append(f"📈 전날 대비(UV): {arrow} {delta_str} {pct_str}".rstrip())
    lines.append("")
    lines.append(f"🖱 클릭 총합: {clicks_y}")
    lines.append(f"  - 가입하기+문의 합산")
    lines.append("")
    lines.append("📌 자세한 항목은 📊 킴스 통계에서 확인 가능")

    # 지정 CHAT_ID로 전송
    try:
        send_ctx(ctx, "\n".join(lines))
    except Exception:
        pass


def _kims_spike_watch_loop(ctx):
    """
    🚨 클릭 + 방문(PV) 급증 감시
    - 각각 ON/OFF 가능
    - 각각 기준(click_threshold / visit_threshold) 적용
    """
    ks = state.setdefault("kims_stats", {})
    sp = ks.setdefault("spike", {})

    click_on = bool(sp.get("click_enabled", False))
    visit_on = bool(sp.get("visit_enabled", False))
    if not (click_on or visit_on):
        return

    interval_sec = int(sp.get("interval_sec", 60) or 60)

    now_ts = time.time()
    last_check = float(sp.get("last_check", 0.0) or 0.0)
    if last_check and (now_ts - last_check < max(10, interval_sec * 0.8)):
        return

    d = _kims_stats_fetch({"range": "today"})

    # 현재 값
    cur_clicks = _kims_sum_clicks(d)
    cur_pv, _ = _kims_get_pv_uv(d)

    prev_clicks = sp.get("last_clicks", None)
    prev_pv = sp.get("last_pv", None)

    # 기준값 갱신
    sp["last_clicks"] = cur_clicks
    sp["last_pv"] = cur_pv
    sp["last_check"] = now_ts
    save_state()

    # 첫 실행(또는 기준값 없음)은 기준만 잡고 종료
    if prev_clicks is None or prev_pv is None:
        return

    try:
        prev_clicks = int(prev_clicks)
        prev_pv = int(prev_pv)
    except Exception:
        return

    delta_clicks = cur_clicks - prev_clicks
    delta_pv = cur_pv - prev_pv

    alerts = []

    if click_on:
        th = int(sp.get("click_threshold", 3) or 3)
        if delta_clicks >= th:
            alerts.append(f"🖱 클릭 급증: +{delta_clicks} (기준 {th})")

    if visit_on:
        th = int(sp.get("visit_threshold", 3) or 3)
        if delta_pv >= th:
            alerts.append(f"👀 방문 급증(PV): +{delta_pv} (기준 {th})")

    if alerts:
        msg = (
            "🚨 급증 감지!\n\n"
            + "\n".join(alerts)
            + f"\n\n📊 오늘 누적\n• 클릭: {cur_clicks}\n• PV: {cur_pv}"
        )
        try:
            send_ctx(ctx, msg)
        except Exception:
            pass


def recent_alert_line(last_ts, now_ts=None):
    """
    마지막 알림 시각(last_ts) 기준으로 몇 분 지났는지 한 줄 문자열로 반환.
    last_ts: time.time() 값 또는 None
    """
    if not last_ts:
        return "⏰ 최근알림 : -\n"

    if now_ts is None:
        now_ts = time.time()

    try:
        diff_min = int((float(now_ts) - float(last_ts)) / 60)
    except:
        return "⏰ 최근알림 : -\n"

    if diff_min <= 0:
        return "⏰ 최근알림 : 방금 전\n"
    return f"⏰ 최근알림 : {diff_min}분 전\n"


def get_ticker(market):
    r = requests.get(f"{UPBIT}/ticker", params={"markets": market}, timeout=5)
    r.raise_for_status()
    return r.json()[0]

def get_price(market):
    return float(get_ticker(market)["trade_price"])

def get_candles(market, minutes=10, count=50):
    """
    업비트 분봉 캔들 조회 (기본 10분봉)
    return: 오래된 순서대로 [{time, open, high, low, close}, ...]
    """
    try:
        url = f"https://api.upbit.com/v1/candles/minutes/{minutes}"
        params = {"market": market, "count": count}
        res = requests.get(url, params=params, timeout=3)
        res.raise_for_status()
        data = res.json()
    except:
        return []

    candles = []
    # 업비트 응답이 최신 → 과거 순서라서 뒤집어줌
    for c in reversed(data):
        try:
            t = datetime.strptime(c["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S")
            candles.append({
                "time": t,
                "open": float(c["opening_price"]),
                "high": float(c["high_price"]),
                "low": float(c["low_price"]),
                "close": float(c["trade_price"]),
            })
        except:
            continue
    return candles

def make_chart_image(candles, title):
    """
    단순 캔들 차트 생성 (양봉=빨강, 음봉=파랑, 최고/최저가 표시)
    """
    if not candles:
        return None

    times = [c["time"] for c in candles]
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows  = [c["low"]  for c in candles]
    closes= [c["close"] for c in candles]

    # 최고/최저 계산
    high_price = max(highs)
    low_price  = min(lows)
    high_idx   = highs.index(high_price)
    low_idx    = lows.index(low_price)
    high_time  = times[high_idx]
    low_time   = times[low_idx]

    fig, ax = plt.subplots(figsize=(8, 3))

    # ✅ 캔들 간격에 비례해서 자동 계산 (차트 깔끔)
    if len(times) >= 2:
        # 한 캔들 시간 간격(일 단위) * 0.6 정도로 몸통 폭 설정
        interval = (times[1] - times[0]).total_seconds() / (24 * 60 * 60)
        width = interval * 0.6
    else:
        width = 0.01

    for t, o, h, l, c in zip(times, opens, highs, lows, closes):
        x = mdates.date2num(t)
        # 양봉 = 종가 >= 시가 → 빨강 / 음봉 = 파랑
        up = c >= o
        color = 'red' if up else 'blue'

        # 꼬리
        ax.plot([x, x], [l, h], color=color, linewidth=1)

        # 몸통
        body_bottom = min(o, c)
        body_height = max(abs(c - o), 0.0001)  # 0 높이 방지
        rect = Rectangle(
            (x - width / 2, body_bottom),
            width,
            body_height,
            edgecolor=color,
            facecolor=color,
            linewidth=1,
        )
        ax.add_patch(rect)

    # 최고/최저 마커
    x_high = mdates.date2num(high_time)
    x_low  = mdates.date2num(low_time)
    ax.scatter([x_high], [high_price], marker='^', color='red', s=40)
    ax.scatter([x_low],  [low_price],  marker='v', color='blue', s=40)
    ax.text(x_high, high_price, f" 최고 {high_price:,.0f}",
            color='red', fontsize=8, va='bottom', ha='center')

    ax.text(x_low, low_price, f" 최저 {low_price:,.0f}",
            color='blue', fontsize=8, va='top', ha='center')


    ax.set_title(title, fontsize=12)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.xaxis_date()
    plt.xticks(rotation=45)

    ax.set_ylim(min(lows) * 0.998, max(highs) * 1.002)

    plt.subplots_adjust(left=0.24, right=0.98, top=0.88, bottom=0.15)


    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf


def norm_threshold(th):
    if th is None:
        return float(state.get("default_threshold_pct", DEFAULT_THRESHOLD))
    try:
        return float(th)
    except:
        return float(state.get("default_threshold_pct", DEFAULT_THRESHOLD))

def status_emoji(info, cur):
    avg = float(info.get("avg_price", 0.0))
    qty = float(info.get("qty", 0.0))
    if qty <= 0:
        if avg <= 0:
            return "⚪️"
        return "🟡"
    if avg <= 0:
        return "⚪️"
    return "🔴" if cur > avg else "🔵"

def reply(update, text, kb=None):
    cid = update.effective_chat.id
    update.message.reply_text(text, reply_markup=(kb or MAIN_KB(cid)))

def send_ctx(ctx, text):
    if not CHAT_ID:
        return
    try:
        cid = int(CHAT_ID)
    except:
        cid = CHAT_ID
    try:
        ctx.bot.send_message(chat_id=cid, text=text, reply_markup=MAIN_KB(cid))
    except:
        pass

def send_chart_with_text(ctx, market, title10, title60, text):
    """
    10분봉 차트 → 60분봉 차트 → 기존 텍스트 알림 순서로 전송
    """
    if not CHAT_ID:
        return
    try:
        cid = int(CHAT_ID)
    except:
        cid = CHAT_ID

    # 10분봉
    try:
        candles10 = get_candles(market, minutes=10)
        img10 = make_chart_image(candles10, title10)
        if img10:
            ctx.bot.send_photo(chat_id=cid, photo=img10)
    except:
        pass

    # 60분봉
    try:
        candles60 = get_candles(market, minutes=60)
        img60 = make_chart_image(candles60, title60)
        if img60:
            ctx.bot.send_photo(chat_id=cid, photo=img60)
    except:
        pass

    # 텍스트 (형식 그대로 유지)
    try:
        ctx.bot.send_message(chat_id=cid, text=text, reply_markup=MAIN_KB(cid))
    except:
        pass
def send_alert_with_optional_chart(ctx, market, title10, title60, text):
    """
    코인 알림 전용:
    - chart_auto == True  → 차트 + 텍스트
    - chart_auto == False → 텍스트만
    """
    if state.get("chart_auto", True):
        # 기존 방식대로 차트 + 텍스트
        send_chart_with_text(ctx, market, title10, title60, text)
        return

    # 차트 끈 경우: 텍스트만 보내기
    if not CHAT_ID:
        return
    try:
        cid = int(CHAT_ID)
    except:
        cid = CHAT_ID

    try:
        ctx.bot.send_message(chat_id=cid, text=text, reply_markup=MAIN_KB(cid))
    except:
        pass


def pretty_sym(sym: str) -> str:
    sym = sym.upper()
    market = "KRW-" + sym
    info = state["coins"].get(market, {})
    try:
        cur = get_price(market)
    except:
        cur = 0.0
    e = status_emoji(info, cur) if info else "⚪️"
    return f"{e} {sym} {e}"

# ========= 코인 정렬/포맷 =========
def sorted_coin_items():
    items = []
    for m, info in state["coins"].items():
        try:
            t = get_ticker(m)
            cur = float(t.get("trade_price", 0.0))
            vol = float(t.get("acc_trade_price_24h", 0.0))
        except:
            cur = 0.0
            vol = 0.0

        avg = float(info.get("avg_price", 0.0))
        qty = float(info.get("qty", 0.0))

        if qty > 0:
            group = 1
            primary = -(avg * qty)
        elif avg > 0:
            group = 2
            primary = -vol
        else:
            group = 3
            primary = -vol

        items.append((group, primary, m, info, cur))

    items.sort(key=lambda x: (x[0], x[1], x[2]))
    return items

def format_triggers(info):
    trigs = info.get("triggers", [])
    return "없음" if not trigs else " | ".join(fmt(t) for t in sorted(set(trigs)))

def status_line(mkt, info, cur):
    sym = mkt.split("-")[1]
    avg = float(info.get("avg_price", 0.0))
    qty = float(info.get("qty", 0.0))

    buy_amt = avg * qty
    eval_amt = cur * qty
    pnl_w = eval_amt - buy_amt
    pnl_p = 0.0 if buy_amt == 0 else (eval_amt / buy_amt - 1) * 100

    th = norm_threshold(info.get("threshold_pct", None))
    lastp = info.get("last_notified_price", None)
    trig = format_triggers(info)

    head = f"{pretty_sym(sym)}"  # 예: 🔴 SOL 🔴 / 🔵 SOL 🔵 자동 반영

    return (
        f"{head}\n"
        f"• 평단가: {fmt(avg)} 원\n"
        f"• 수량: {qty}\n"
        f"• 💵 총 매수금액: {fmt(buy_amt)} 원\n"
        f"• 📊 평가금액: {fmt(eval_amt)} 원\n"
        f"• 📉 평가손익: {pnl_p:+.2f}% ({fmt(pnl_w)} 원)\n"
        f"• 임계: {th}%\n"
        f"• 마지막 통지: {fmt(lastp) if lastp else '없음'}\n"
        f"• 트리거: {trig}\n"
    )





def view_block(mkt, info, cur):
    sym = mkt.split("-")[1]
    avg = float(info.get("avg_price", 0.0))
    qty = float(info.get("qty", 0.0))

    buy_amt = avg * qty             # 총 매수금액
    eval_amt = cur * qty            # 현재 평가금액
    pnl_w = eval_amt - buy_amt      # 평가손익 (원)
    pnl_p = 0.0 if buy_amt == 0 else (eval_amt / buy_amt - 1) * 100

    th = norm_threshold(info.get("threshold_pct", None))
    trig = format_triggers(info)

    head = f"{pretty_sym(sym)}"  # 🔴/🔵/🟡/⚪️ + 코인명 자동

    return (
        f"{head}\n"
        f"💵 평단가: {fmt(avg)} 원\n"
        f"💰 총 매수금액: {fmt(buy_amt)} 원\n"
        f"📊 평가금액: {fmt(eval_amt)} 원\n"
        f"📉 평가손익: {pnl_p:+.2f}% ({fmt(pnl_w)} 원)\n"
        f"🎯 임계: {th}%\n"
        f"📌 트리거: {trig}\n"
    )


# ========= HOTEL (랜덤 후기 3줄) =========
REVIEWS = [
    [
        "{휴가기간|일주일|며칠|주말} 동안 맡겼는데 너무 좋았어요!",
        "시설도 깔끔하고 아이가 노는 영상을 자주 보내주셔서 안심됐어요.",
        "사장님이 세심하게 챙겨주셔서 다음에도 꼭 맡길 거예요."
    ],
    [
        "{한 달|휴가기간|며칠|일주일} 동안 맡겼는데 완전 만족이에요!",
        "사진이랑 영상으로 아이 소식을 자주 받아서 마음이 놓였어요.",
        "시설도 깨끗하고 분위기도 좋아서 또 이용하려구요."
    ],
    [
        "{며칠|휴가기간|연휴|주말} 동안 맡겼는데 정말 잘 지냈어요.",
        "하루에도 몇 번씩 사진과 영상 보내주셔서 걱정이 싹 사라졌어요.",
        "사장님이 너무 친절해서 믿음이 가는 곳이에요."
    ],
    [
        "{휴가기간|일주일|며칠|연휴} 동안 맡겼는데 대만족이에요!",
        "시설도 깨끗하고 아이가 즐겁게 노는 모습이 영상으로 와서 행복했어요.",
        "두젠틀은 진짜 믿고 맡길 수 있는 곳이에요."
    ],
    [
        "{한 달|휴가기간|며칠|일주일} 동안 맡겼는데 너무 만족스러웠어요.",
        "영상으로 아이가 노는 모습 보내주셔서 매일 안심됐어요.",
        "시설도 깔끔하고 사장님도 세심하게 케어해주셨어요."
    ],
    [
        "{며칠|휴가기간|연휴|주말} 동안 이용했는데 최고였어요.",
        "사진이랑 영상으로 아이 근황 알려주셔서 든든했어요.",
        "시설도 깨끗하고 아이가 밝아져서 너무 만족입니다."
    ],
    [
        "{휴가기간|일주일|3일|며칠} 동안 맡겼는데 정말 마음에 들었어요.",
        "영상으로 아이 상태를 바로 확인할 수 있어서 걱정이 줄었어요.",
        "사장님이 세심하게 챙겨주셔서 믿고 맡길 수 있었습니다."
    ],
    [
        "{한 달|휴가기간|며칠|연휴} 동안 맡겼는데 너무 좋았어요.",
        "사진, 영상으로 아이 소식을 자주 받아서 마음이 편했어요.",
        "시설도 깨끗하고 케어가 꼼꼼해서 정말 만족했어요."
    ],
    [
        "{일주일|휴가기간|며칠|연휴} 동안 맡겼는데 완전 만족이에요.",
        "아이 영상을 수시로 보내주셔서 매일 안심됐어요.",
        "시설도 좋고 분위기도 밝아서 또 맡길 예정이에요."
    ],
    [
        "{한 달|휴가기간|며칠|주말} 동안 맡겼는데 진짜 최고였어요.",
        "하루에도 여러 번 사진, 영상 보내주셔서 믿음이 갔어요.",
        "아이도 행복해 보여서 또 이용하려구요."
    ],
]

def _expand_braces(text: str) -> str:
    def repl(match):
        options = match.group(1).split("|")
        return random.choice(options).strip()
    return re.sub(r"{([^}]+)}", repl, text)

def build_random_hotel_review() -> str:
    first_lines  = [r[0] for r in REVIEWS]
    second_lines = [r[1] for r in REVIEWS]
    third_lines  = [r[2] for r in REVIEWS]
    l1 = _expand_braces(random.choice(first_lines))
    l2 = _expand_braces(random.choice(second_lines))
    l3 = _expand_braces(random.choice(third_lines))
    return "\n".join([l1, l2, l3])

# ========= HELP =========

HELP_MENU = (
    "📌 모드를 선택하세요.\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "1️⃣ 개인비서\n"
    "2️⃣ 네이버\n"
    "3️⃣ 자산관리\n"
    "4️⃣ 랜덤문구\n"
    "\n"
    "각 모드를 누르면 아래 키보드가 바뀌고,\n"
    "그 모드 전용 명령만 사용할 수 있습니다.\n"
)

HELP_PA = (
    "🤖 [개인비서 모드 안내]\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "지금은 일정/미리알림 도우미 기능만 들어있어요.\n"
    "\n"
    "📅 캘린더 메뉴 : 캘린더 감시/간트차트 기능 메뉴를 엽니다.\n"
    "📡 캘린더 감시 ON/OFF : 구글 캘린더 일정 변화를 자동으로 감시해 알려줍니다.\n"
    "🗓 간트차트(7/30일) : 앞으로 일정들을 막대그래프로 한눈에 볼 수 있습니다.\n"
    "⏰ 미리알림 알림 : iCloud 미리알림(출근/회사)을 감시해서 오늘 마감인 항목을 반복으로 알려줍니다.\n"
    "📋 오늘 미리알림 : 지금 기준으로 오늘 마감인 미리알림 목록을 한 번에 보여줍니다.\n"
    "\n"
    "❓ 도움말 : 이 안내를 다시 보여줍니다.\n"
    "🏠 메뉴   : 모드 선택 화면으로 돌아갑니다.\n"
)



HELP_NAVER = (
    "📢 [네이버 모드 안내]\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "네이버 검색광고 / 플레이스 상태를 확인하고 관리하는 모드입니다.\n"
    "아래 버튼으로 명령을 선택하세요.\n"
    "\n"
    "📊 광고상태 : 현재 입찰가, 자동 적용 여부, 감시 상태를 요약해서 보여줍니다.\n"
    "📈 노출현황 : 주요 키워드의 현재 순위를 확인합니다.\n"
    "📝 리뷰현황 : 네이버 플레이스 리뷰 개수 변화만 따로 보여줍니다.\n"
    "\n"
    "⏰ 광고시간 : 시간대별 목표 입찰가 시간표를 관리합니다.\n"
    "⚙️ 광고설정 : 기본 입찰가, 대상 그룹 등 설정을 변경합니다.\n"
    "🎯 입찰추정 : 1순위 근처로 필요한 입찰가를 자동으로 계산합니다.\n"
    "\n"
    "🤖 광고자동 : 저장된 시간표대로 입찰가를 자동 적용할지 ON/OFF.\n"
    "👁️ 노출감시 : 지정한 키워드 순위를 주기적으로 확인하고 변동 시 알려줍니다.\n"
    "🔔 리뷰감시 : 새 리뷰가 생기면 알려줍니다.\n"
    "\n"
    "❓ 도움말 : 이 안내를 다시 보여줍니다.\n"
    "🏠 메뉴   : 모드 선택 화면으로 돌아갑니다."
)

HELP_COIN = (
    "💰 [자산관리 모드 안내]\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "코인 보유 현황과 알림을 관리하는 모드입니다.\n"
    "(기존 코인 가격알림 기능과 동일하게 동작합니다.)\n"
    "\n"
    "📋 보기 : 등록된 코인들의 요약 정보를 보여줍니다.\n"
    "📊 상태 : 각 코인의 평단, 수량, 평가손익, 트리거를 상세히 보여줍니다.\n"
    "\n"
    "➕ 코인 : 감시할 코인을 추가합니다.\n"
    "💵 가격 : 현재 가격을 확인합니다.\n"
    "🎯 임계값 : 변동률 기준(%)을 설정해, 넘으면 알림을 받습니다.\n"
    "🖼️ 차트알림 : 코인 알림에 차트를 자동으로 붙일지 ON/OFF 합니다.\n"
    "\n"
    "📌 평단 : 해당 코인의 평단가를 등록/수정합니다.\n"
    "📦 수량 : 보유 수량을 등록/수정합니다.\n"
    "📍 지정가 : 특정 가격(트리거)을 등록해서, 도달 시 알림을 받습니다.\n"
    "🖼️ 차트 : 원하는 코인의 10분봉/60분봉 차트를 바로 확인합니다.\n"
    "\n"
    "❓ 도움말 : 이 안내를 다시 보여줍니다.\n"
    "🏠 메뉴   : 모드 선택 화면으로 돌아갑니다."
)

HELP_RANDOM = (
    "✨ [랜덤문구 모드 안내]\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "홍보 문구, 리뷰 문구 등 랜덤 텍스트를 뽑아주는 모드입니다.\n"
    "현재는 '호텔' 문구만 제공하며, 앞으로 종류가 늘어날 예정입니다.\n"
    "\n"
    "🏨 호텔 : 반려견 호텔/위탁용 랜덤 문구를 한 개 보여줍니다.\n"
    "❓ 도움말 : 이 안내를 다시 보여줍니다.\n"
    "🏠 메뉴   : 모드 선택 화면으로 돌아갑니다."
)

REMINDER_MENU_KB = ReplyKeyboardMarkup(
    [
        ["📡 업무 감시 ON/OFF", "⏱️ 감시 주기 설정"],
        ["📋 오늘 업무", "📆 7일 업무"],
        ["🌙 심야 시간 안내", "◀️ 뒤로 가기"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)

GCAL_MENU_KB = ReplyKeyboardMarkup(
    [
        ["📡 캘린더 감시 ON/OFF"],
        ["🗓 7일 간트차트 보기", "🗓 30일 간트차트 보기"],
        ["◀️ 뒤로가기"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)

SALES_MENU_KB = ReplyKeyboardMarkup(
    [
        ["📆 오늘 매출", "🕰 어제 매출"],
        ["🗓 당월 매출", "📜 전월 매출"],
        ["◀️ 뒤로 가기"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)
KIMS_STATS_MENU_KB = ReplyKeyboardMarkup(
    [
        ["📆 오늘", "🕰 어제"],
        ["🗓 7일", "🗓 30일"],
        ["📅 기간조회", "🚨 급증알림 설정"],
        ["◀️ 뒤로 가기"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)
KIMS_SPIKE_MENU_KB = ReplyKeyboardMarkup(
    [
        ["🖱 클릭 알림 ON/OFF", "👀 방문 알림 ON/OFF"],
        ["🧷 기준 선택(클릭)", "🧷 기준 선택(방문)"],
        ["◀️ 통계로", "◀️ 뒤로 가기"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)

KIMS_THRESHOLD_KB = ReplyKeyboardMarkup(
    [
        ["기준 3", "기준 5", "기준 10"],
        ["취소"],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)


def get_help_by_mode(cid):
    """
    현재 채팅의 모드에 맞는 도움말 텍스트를 반환
    """
    mode = get_mode(cid)

    if mode in ("pa", "mode_pa"):
        return HELP_PA
    if mode in ("naver", "mode_naver"):
        return HELP_NAVER
    if mode in ("coin", "mode_coin"):
        return HELP_COIN
    if mode in ("random", "mode_random"):
        return HELP_RANDOM

    return HELP_MENU

# ========= PENDING =========
def set_pending(cid, action, step="symbol", data=None):
    p = state["pending"].setdefault(str(cid), {})
    p.update({"action": action, "step": step, "data": data or {}})
    save_state()

def clear_pending(cid):
    state["pending"].pop(str(cid), None)
    save_state()

def get_pending(cid):
    return state["pending"].get(str(cid))

# ========= COIN ACTION HELPERS =========
def ensure_coin(m):
    c = state["coins"].setdefault(
        m,
        {
            "avg_price": 0.0,
            "qty": 0.0,
            "threshold_pct": None,
            "last_notified_price": None,
            "prev_price": None,
            "triggers": [],
            "last_alert_ts": None,
        }
    )
    c.setdefault("triggers", [])
    c.setdefault("prev_price", None)
    c.setdefault("last_notified_price", None)
    c.setdefault("last_alert_ts", None)
    return c


def act_add(update, symbol):
    m = krw_symbol(symbol)
    ensure_coin(m)
    save_state()
    reply(update, f"추가 완료: {pretty_sym(m.split('-')[1])}")

def act_del(update, symbol):
    m = krw_symbol(symbol)
    if m in state["coins"]:
        state["coins"].pop(m)
        save_state()
        reply(update, f"삭제 완료: {pretty_sym(m.split('-')[1])}")
    else:
        reply(update, "해당 코인이 없습니다.")

def act_price(update, symbol):
    m = krw_symbol(symbol)
    try:
        p = get_price(m)
        reply(update, f"{pretty_sym(m.split('-')[1])} 현재가 {fmt(p)} 원")
    except:
        reply(update, "가격 조회 실패")

def act_setavg(update, symbol, value):
    m = krw_symbol(symbol)
    c = ensure_coin(m)
    c["avg_price"] = float(value)
    save_state()
    reply(update, f"{pretty_sym(m.split('-')[1])} 평단 {fmt(value)} 원")

def act_setqty(update, symbol, value):
    m = krw_symbol(symbol)
    c = ensure_coin(m)
    c["qty"] = float(value)
    save_state()
    reply(update, f"{pretty_sym(m.split('-')[1])} 수량 {value}")

def act_setrate_default(update, value):
    state["default_threshold_pct"] = float(value)
    save_state()
    reply(update, f"기본 임계값 {value}%")

def act_setrate_symbol(update, symbol, value):
    m = krw_symbol(symbol)
    c = ensure_coin(m)
    c["threshold_pct"] = float(value)
    save_state()
    reply(update, f"{pretty_sym(m.split('-')[1])} 개별 임계값 {value}%")

# ========= TRIGGERS =========
def _trigger_list_text(c):
    trigs = c.get("triggers", [])
    if not trigs:
        return "트리거: 없음"
    lines = [f"{i+1}. {fmt(v)}" for i, v in enumerate(sorted(trigs))]
    return "트리거 목록\n" + "\n".join(lines)

def trigger_add(symbol, mode, value):
    m = krw_symbol(symbol)
    c = ensure_coin(m)
    if mode == "direct":
        target = float(value)
    else:
        if mode == "cur_pct":
            base = get_price(m)
        else:
            base = float(c.get("avg_price", 0.0))
            if base <= 0:
                raise ValueError("평단가가 없습니다.")
        pct = float(value)
        target = base * (1 + pct/100.0)
    c["triggers"].append(float(target))
    save_state()
    return target

def trigger_delete(symbol, indices):
    m = krw_symbol(symbol)
    c = ensure_coin(m)
    trigs = sorted(list(c.get("triggers", [])))
    kept = [v for i, v in enumerate(trigs, start=1) if i not in indices]
    c["triggers"] = kept
    save_state()
    return len(trigs) - len(kept)

def trigger_clear(symbol):
    m = krw_symbol(symbol)
    c = ensure_coin(m)
    n = len(c.get("triggers", []))
    c["triggers"] = []
    save_state()
    return n

# ========= NAVER API HELPERS =========
def naver_enabled():
    return bool(
        NAVER_API_KEY and NAVER_API_SECRET and NAVER_CUSTOMER_ID and
        (NAVER_ADGROUP_ID or NAVER_ADGROUP_NAME)
    )

def _naver_signature(timestamp, method, uri):
    msg = f"{timestamp}.{method}.{uri}"
    digest = hmac.new(
        NAVER_API_SECRET.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("utf-8")

def _naver_request(method, uri, params=None, body=None):
    if not naver_enabled():
        raise RuntimeError("NAVER API 미설정")
    ts = str(int(time.time() * 1000))
    sig = _naver_signature(ts, method, uri)
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": NAVER_API_KEY,
        "X-Customer": NAVER_CUSTOMER_ID,
        "X-Signature": sig,
    }
    url = NAVER_BASE_URL + uri
    if method == "GET":
        return requests.get(url, headers=headers, params=params, timeout=5)
    elif method == "PUT":
        return requests.put(url, headers=headers, params=params, json=body, timeout=5)
    else:
        raise ValueError("Unsupported method")

def _naver_get_adgroup_id():
    nav = state.setdefault("naver", {})

    if NAVER_ADGROUP_ID:
        nav["adgroup_id"] = NAVER_ADGROUP_ID
        save_state()
        return NAVER_ADGROUP_ID

    if nav.get("adgroup_id"):
        return nav["adgroup_id"]

    if not NAVER_ADGROUP_NAME:
        return None

    params = {}
    if NAVER_CAMPAIGN_ID:
        params["nccCampaignId"] = NAVER_CAMPAIGN_ID

    try:
        r = _naver_request("GET", "/ncc/adgroups", params=params)
    except Exception as e:
        print("[NAVER] adgroups 조회 실패:", e)
        return None

    if r.status_code != 200:
        print("[NAVER] adgroups 조회 실패:", r.status_code, r.text)
        return None

    try:
        groups = r.json()
    except:
        return None

    for g in groups:
        if g.get("name") == NAVER_ADGROUP_NAME:
            nav["adgroup_id"] = g.get("nccAdgroupId")
            save_state()
            return nav["adgroup_id"]

    print("[NAVER] 대상 광고그룹 이름 없음:", NAVER_ADGROUP_NAME)
    return None

def naver_get_bid():
    adgroup_id = _naver_get_adgroup_id()
    if not adgroup_id:
        return None
    r = _naver_request("GET", f"/ncc/adgroups/{adgroup_id}")
    if r.status_code != 200:
        print("[NAVER] adgroup 조회 실패:", r.status_code, r.text)
        return None
    data = r.json()
    bid = data.get("bidAmt")
    nav = state.setdefault("naver", {})
    nav["last_known_bid"] = bid
    save_state()
    return bid

def naver_set_bid(new_bid: int):
    adgroup_id = _naver_get_adgroup_id()
    if not adgroup_id:
        return False, "대상 광고그룹(ID)을 찾지 못했습니다. .env 설정을 확인하세요."

    r = _naver_request("GET", f"/ncc/adgroups/{adgroup_id}")
    if r.status_code != 200:
        return False, f"현재 설정 조회 실패 (code {r.status_code})"

    body = r.json()
    old_bid = body.get("bidAmt")

    try:
        new_bid = int(new_bid)
    except:
        return False, "입찰가는 숫자만 가능합니다."

    if old_bid == new_bid:
        nav = state.setdefault("naver", {})
        nav["last_known_bid"] = old_bid
        save_state()
        return False, f"이미 {new_bid}원으로 설정되어 있습니다."

    body["bidAmt"] = new_bid

    r2 = _naver_request("PUT", f"/ncc/adgroups/{adgroup_id}", body=body)
    if r2.status_code != 200:
        return False, f"변경 실패 (code {r2.status_code})"

    res = r2.json()
    applied = res.get("bidAmt")
    nav = state.setdefault("naver", {})
    nav["last_known_bid"] = applied
    save_state()

    if applied == new_bid:
        return True, f"입찰가가 {old_bid} → {applied}원으로 변경되었습니다."
    else:
        return False, "API 응답이 예상과 다릅니다."

# ========= NAVER 검색 URL =========
def _naver_search_url(keyword: str) -> str:
    q = urllib.parse.quote(keyword)
    # 최신 place 검색 탭 기준
    return f"https://search.naver.com/search.naver?where=place&sm=tab_nx.place&query={q}"

# ========= APOLLO STATE 파서 & 순위 계산 =========
def _extract_js_object(s: str, start_idx: int):
    depth = 0
    in_str = False
    esc = False
    started = False
    for i in range(start_idx, len(s)):
        ch = s[i]
        if not started:
            if ch == "{":
                started = True
                depth = 1
            else:
                continue
            continue
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return s[start_idx:i+1]
    return None

def _extract_apollo_state(html: str):
    idx = html.find("__APOLLO_STATE__")
    if idx < 0:
        return None
    brace = html.find("{", idx)
    if brace < 0:
        return None
    obj = _extract_js_object(html, brace)
    if not obj:
        return None
    js = (
        obj.replace("undefined", "null")
           .replace("!0", "true")
           .replace("!1", "false")
    )
    try:
        return json.loads(js)
    except Exception as e:
        print("[NAVER] __APOLLO_STATE__ JSON 파싱 실패:", e)
        return None

def _normalize(s: str) -> str:
    return re.sub(r"\s+", "", str(s or ""))

def _match_name(name: str, marker: str) -> bool:
    if not name or not marker:
        return False

    nn = _normalize(name)
    mm = _normalize(marker)

    # marker 전체가 포함되면 매칭
    if mm and mm in nn:
        return True

    # marker를 공백으로 쪼개서 각각이 이름 안에 포함되는지 검사 (정규화 기준)
    tokens = [t for t in re.split(r"\s+", marker.strip()) if t]
    if tokens and all(_normalize(t) in nn for t in tokens):
        return True

    return False


def _get_name_id(apollo, ref):
    node = apollo.get(ref, {}) or {}
    name = node.get("name") or node.get("businessName") or node.get("title")
    bid = node.get("id") or node.get("businessId")

    # attraction 하위에 실제 place 정보가 있을 수 있음
    if (not name or not bid) and "attraction" in node:
        ref2 = node["attraction"].get("__ref")
        if ref2:
            n2 = apollo.get(ref2, {}) or {}
            if not name:
                name = n2.get("name") or n2.get("businessName") or n2.get("title")
            if not bid:
                bid = n2.get("id") or n2.get("businessId")

    if bid is not None:
        bid = str(bid).strip()
    return name, bid

def _is_target_place(bid: str, name: str, marker: str) -> bool:
    """
    우리 매장인지 판단:
    1) NAVER_PLACE_ID 가 설정되어 있으면 bid(플레이스 ID) 우선 비교
    2) 아니면 기존 이름/marker 매칭(_match_name) 사용
    """
    # 1) PLACE ID 우선
    if NAVER_PLACE_ID:
        try:
            if bid and str(bid).strip() == str(NAVER_PLACE_ID).strip():
                return True
        except:
            pass

    # 2) PLACE ID가 없거나 다르면 이름 기준
    return _match_name(name, marker)


def detect_place_ranks(html: str, marker: str):
    """
    광고/기본 둘 다 계산:
    - 광고 순위: adBusinesses(...) 순서
    - 기본 순위: attractions(...).businesses(...).items 순서
    반환: {"ad": ad_rank or None, "organic": organic_rank or None} 또는 None
    """
    if not marker:
        return None

    apollo = _extract_apollo_state(html)
    if not apollo:
        return None

    root = apollo.get("ROOT_QUERY", {})

    # 광고 순위
    ad_rank = None
    ad_key = next((k for k in root.keys() if k.startswith("adBusinesses(")), None)
    if ad_key:
        try:
            ad_items = root[ad_key].get("items", [])
            idx = 0
            for it in ad_items:
                ref = it.get("__ref")
                if not ref:
                    continue

                name, bid = _get_name_id(apollo, ref)
                if not name:
                    continue

                idx += 1
                # 우리 매장(PLACE ID 또는 marker 기준)에만 ad_rank 부여
                if ad_rank is None and _is_target_place(bid, name, marker):
                    ad_rank = idx
        except Exception as e:
            print("[NAVER] adBusinesses 파싱 실패:", e)

    # 기본 순위
    org_rank = None
    att_key = next((k for k in root.keys() if k.startswith("attractions(")), None)
    if att_key:
        att = root.get(att_key, {})
        biz_key = next((k for k in att.keys() if k.startswith("businesses(")), None)
        if biz_key:
            biz = att.get(biz_key, {})
            items = biz.get("items", [])
            idx = 0
            for it in items:
                ref = it.get("__ref")
                if not ref:
                    continue

                name, bid = _get_name_id(apollo, ref)
                if not name:
                    continue

                idx += 1
                # 우리 매장(PLACE ID 또는 marker 기준)에만 organic 순위 부여
                if org_rank is None and _is_target_place(bid, name, marker):
                    org_rank = idx


    if ad_rank is None and org_rank is None:
        return None

    return {"ad": ad_rank, "organic": org_rank}

def _fmt_rank(v):
    return f"{v}위" if isinstance(v, int) and v > 0 else "정보 없음"

# ========= NAVER STATUS / SCHEDULE =========
def send_naver_status(update):
    nav = state.setdefault("naver", {})
    auto_enabled = bool(nav.get("auto_enabled"))
    schedules = nav.get("schedules") or []
    rw = nav.get("rank_watch", {}) or {}
    rv = nav.get("review_watch", {}) or {}

    # 자동 변경 상태
    auto_icon = "🟢" if auto_enabled else "🔴"
    auto_text = "켜짐" if auto_enabled else "꺼짐"

    lines = []

    # 헤더
    lines.append("📢 네이버 광고 상태")
    lines.append("")
    lines.append(f"⚙️ 자동 변경: {auto_icon} {auto_text}")

    # 광고 시간표
    if schedules:
        lines.append("🕒 광고 시간표:")
        for s in schedules:
            t = s.get("time", "")
            bid = s.get("bid")
            try:
                bid = int(bid)
            except:
                pass
            lines.append(f"· [{_days_to_label(s.get('days'))}] {t} → {bid}원")
    else:
        lines.append("🕒 광고 시간표: 없음 (광고시간 명령으로 설정)")

    # 현재 입찰가
    current_bid = None
    try:
        if naver_enabled():
            current_bid = naver_get_bid()
    except:
        current_bid = None

    if current_bid is not None:
        try:
            current_int = int(current_bid)
        except:
            current_int = current_bid
        lines.append(f"🎯 현재 입찰가: {current_int}원")
    else:
        lines.append("🎯 현재 입찰가: 확인 실패")

    # 마지막 자동 적용
    last_applied = nav.get("last_applied") or "없음"
    lines.append(f"📅 마지막 자동 적용: {last_applied}")
    lines.append("")

    # ========= 노출 감시 =========
    if rw.get("enabled"):
        kw = rw.get("keyword") or "미설정"
        iv_sec = float(rw.get("interval", 300))
        iv_min = iv_sec / 60.0
        if abs(iv_min - round(iv_min)) < 1e-6:
            iv_str = f"{int(round(iv_min))}분"
        else:
            iv_str = f"{iv_min:.1f}분"

        last_rank = rw.get("last_rank", None)
        rank_str = _fmt_rank(last_rank) if last_rank is not None else "정보 없음"

        lines.append("👁‍🗨 노출 감시: 🟢 ON")
        lines.append(f"• 키워드: `{kw}`")
        lines.append(f"• 체크 간격: {iv_str}")
        lines.append(f"• 최근 기본 순위: {rank_str}")
    else:
        lines.append("👁‍🗨 노출 감시: 🔴 OFF")

    lines.append("")

    # ========= 리뷰 감시 =========
    if rv.get("enabled"):
        iv_sec = float(rv.get("interval", 180))
        iv_min = iv_sec / 60.0
        if abs(iv_min - round(iv_min)) < 1e-6:
            iv_str = f"{int(round(iv_min))}분"
        else:
            iv_str = f"{iv_min:.1f}분"

        last = rv.get("last_count") or {}
        visit = blog = total = None

        if isinstance(last, dict):
            visit = last.get("visit")
            blog = last.get("blog")
            total = last.get("total")

        lines.append("📝 리뷰 감시: 🟢 ON")
        lines.append(f"• 체크 간격: {iv_str}")

        if isinstance(last, dict) and any(v is not None for v in (visit, blog, total)):
            if visit is not None:
                lines.append(f"• 방문자 리뷰: {visit}")
            if blog is not None:
                lines.append(f"• 블로그 리뷰: {blog}")
            if total is not None:
                lines.append(f"• 총 합계: {total}")
        else:
            # 예전 형식(last_count가 숫자/문자일 때) 대비
            lines.append(f"• 마지막 리뷰수: {last}")
    else:
        lines.append("📝 리뷰 감시: 🔴 OFF")

    reply(update, "\n".join(lines))

# ========= NAVER 요일/시간표 파서 =========
_KO_DAY_MAP = {
    "월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6,
}
_KO_DAY_ORDER = ["월", "화", "수", "목", "금", "토", "일"]

def _days_to_label(days):
    """
    days: [0..6] 또는 None
    """
    if not days:
        return "매일"
    s = sorted(set(int(x) for x in days if isinstance(x, int)))
    if s == [5, 6]:
        return "주말"
    if s == [0, 1, 2, 3, 4]:
        return "평일"
    rev = {v: k for k, v in _KO_DAY_MAP.items()}
    return ",".join(rev.get(x, "?") for x in s)

def _parse_days_prefix(raw_no_space: str):
    """
    raw_no_space 예:
      '주말13:00/200'
      '월,화13:00/200'
      '토13:00/0'
      '13:00/200'  (요일 없음)
    return: (days_list_or_none, rest_string)
    """
    s = (raw_no_space or "").strip()
    if not s:
        return None, s

    if s.startswith("주말"):
        return [5, 6], s[len("주말"):]
    if s.startswith("평일"):
        return [0, 1, 2, 3, 4], s[len("평일"):]

    # 앞부분에서 요일 문자(월화수목금토일) 또는 ','만 연속되는 구간을 prefix로 본다
    i = 0
    allowed = set(list(_KO_DAY_MAP.keys()) + [","])
    while i < len(s) and s[i] in allowed:
        i += 1

    prefix = s[:i]
    rest = s[i:]
    if not prefix:
        return None, s  # 요일 없음

    tokens = [x for x in prefix.split(",") if x]
    days = []
    for tok in tokens:
        # '월화수' 같이 붙여 쓴 것도 허용
        for ch in tok:
            if ch in _KO_DAY_MAP:
                days.append(_KO_DAY_MAP[ch])

    days = sorted(set(days))
    if not days:
        return None, s
    return days, rest

def _parse_schedule_add_input(text: str):
    """
    입력 예:
      '13:00/200'
      '주말 13:00/0'
      '월,화,수 13:00/200'
    return: (days_or_none, 'HH:MM', bid_int) or raise
    """
    raw = (text or "").replace(" ", "")
    days, rest = _parse_days_prefix(raw)
    ts, vs = rest.split("/", 1)
    datetime.strptime(ts, "%H:%M")  # validate
    bid = int(vs.replace(",", ""))
    return days, ts, bid

def _parse_schedule_del_input(text: str):
    """
    삭제 입력 예:
      '11:00'          -> 해당 시간 전체 삭제
      '주말 11:00'     -> 주말만 삭제
      '월,화 11:00'    -> 월/화만 삭제
    return: (days_or_none, 'HH:MM') or raise
    """
    raw = (text or "").replace(" ", "")
    days, rest = _parse_days_prefix(raw)
    datetime.strptime(rest, "%H:%M")
    return days, rest


def naver_schedule_loop(context):
    if not naver_enabled():
        return

    nav = state.setdefault("naver", {})
    if not nav.get("auto_enabled"):
        return

    schedules = nav.get("schedules") or []
    if not schedules:
        return

    now = datetime.now(KST)
    current_hm = now.strftime("%H:%M")
    today = now.strftime("%Y-%m-%d")
    w = now.weekday()  # 월0 ... 일6

    for s in schedules:
        t = s.get("time")
        bid = s.get("bid")
        days = s.get("days")  # 없으면 매일

        if not t:
            continue

        # ✅ 요일 조건: days가 있으면 오늘 요일이 포함될 때만
        if days:
            try:
                if int(w) not in [int(x) for x in days]:
                    continue
            except:
                # days가 이상하면 안전하게 스킵
                continue

        if current_hm == t:
            # ✅ 요일별로 같은 시간에 다른 bid가 가능하니 key에 weekday 포함
            key = f"{today} w{w} {t} {bid}"
            if nav.get("last_applied") == key:
                continue

            success, msg = naver_set_bid(int(bid))
            nav["last_applied"] = key
            save_state()

            try:
                label = _days_to_label(days)
                if success:
                    send_ctx(context, f"✅ [네이버 광고 자동 변경]\n({label} {t})\n{msg}")
                else:
                    send_ctx(context, f"⚠️ [네이버 광고 자동 변경 실패]\n({label} {t})\n{msg}")
            except:
                pass


# ========= NAVER 입찰추정 (기존 로직) =========
def detect_ad_position(html: str, marker: str):
    if not marker:
        return None
    idx = html.find(marker)
    if idx < 0:
        return None
    last_rank = None
    for m in re.finditer(r'data-cr-rank="(\d+)"', html):
        pos = m.start()
        rank = int(m.group(1))
        if pos < idx:
            last_rank = rank
        else:
            break
    if last_rank is not None:
        return last_rank
    return 1

def start_naver_abtest(cid, keyword, marker, start_bid, max_bid, step, interval):
    nav = state.setdefault("naver", {})
    nav["abtest"] = {
        "chat_id": cid,
        "keyword": keyword,
        "marker": marker,
        "current_bid": int(start_bid),
        "max_bid": int(max_bid),
        "step": int(step),
        "interval": int(interval),
        "last_check": 0,
        "phase": "set",
        "status": "running",
    }
    save_state()

def naver_abtest_loop(context):
    nav = state.setdefault("naver", {})
    ab = nav.get("abtest")
    if not ab or ab.get("status") != "running":
        return

    cid = ab.get("chat_id")
    now = time.time()
    interval = int(ab.get("interval", 60))
    step = int(ab.get("step", 10))
    cur_bid = int(ab.get("current_bid", 0))
    max_bid = int(ab.get("max_bid", 0))
    keyword = ab.get("keyword", "")
    marker = ab.get("marker", "")
    phase = ab.get("phase", "set")

    if not (cid and keyword and cur_bid > 0 and step > 0):
        ab["status"] = "stopped"
        save_state()
        return

    if phase == "set":
        success, msg = naver_set_bid(cur_bid)
        if not success:
            ab["status"] = "stopped"
            save_state()
            try:
                context.bot.send_message(
                    chat_id=cid,
                    text=f"⚠️ [입찰추정 종료] 입찰 설정 실패: {msg}",
                    reply_markup=MAIN_KB(cid),
                )
            except:
                pass
            return

        ab["phase"] = "check"
        ab["last_check"] = now
        save_state()
        try:
            context.bot.send_message(
                chat_id=cid,
                text=f"🔧 [입찰추정] {cur_bid}원으로 설정. {interval}초 후 노출 위치 확인.",
                reply_markup=MAIN_KB(cid),
            )
        except:
            pass
        return

    if phase == "check":
        last = float(ab.get("last_check", 0))
        if now - last < interval:
            return

        html = ""
        try:
            url = _naver_search_url(keyword)
            r = requests.get(url, headers=NAVER_HEADERS, timeout=5)
            html = r.text
        except Exception as e:
            print("[NAVER] 검색 결과 조회 실패:", e)

        pos = detect_ad_position(html, marker) if html else None

        if pos == 1:
            ab["status"] = "done"
            save_state()
            try:
                context.bot.send_message(
                    chat_id=cid,
                    text=(
                        f"✅ [입찰추정 완료]\n"
                        f"키워드 '{keyword}' 1순위 추정 입찰가: {cur_bid}원\n"
                        f"(검색 페이지 구조/개인화에 따라 실제와 다를 수 있습니다.)"
                    ),
                    reply_markup=MAIN_KB(cid),
                )
            except:
                pass
            return

        next_bid = cur_bid + step
        if max_bid and next_bid > max_bid:
            ab["status"] = "done"
            save_state()
            try:
                context.bot.send_message(
                    chat_id=cid,
                    text=(
                        f"⚠️ [입찰추정 종료]\n"
                        f"최대 입찰가 {max_bid}원을 초과하여 중단했습니다.\n"
                        f"{cur_bid}원까지 올렸지만 1순위로 추정되지 않았습니다."
                    ),
                    reply_markup=MAIN_KB(cid),
                )
            except:
                pass
            return

        ab["current_bid"] = next_bid
        ab["phase"] = "set"
        ab["last_check"] = now
        save_state()
        try:
            context.bot.send_message(
                chat_id=cid,
                text=f"ℹ️ [입찰추정] 1순위 아님 → {next_bid}원으로 재시도합니다.",
                reply_markup=MAIN_KB(cid),
            )
        except:
            pass

# ========= NAVER 노출감시 (광고/기본 동시 확인) =========
def naver_rank_watch_loop(context):
    nav = state.setdefault("naver", {})
    cfg = nav.setdefault("rank_watch", {})
    if not cfg.get("enabled"):
        return

    keyword = (cfg.get("keyword") or "").strip()
    marker = (cfg.get("marker") or "").strip()
    interval = int(cfg.get("interval", 300))
    last_check = float(cfg.get("last_check", 0.0))
    now = time.time()

    if not (keyword and marker):
        return
    if now - last_check < interval:
        return

    html = ""
    try:
        url = _naver_search_url(keyword)
        r = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        html = r.text
    except Exception as e:
        print("[NAVER] 노출감시 조회 실패:", e)
        return

    cfg["last_check"] = now

    res = detect_place_ranks(html, marker) if html else None
    if not res:
        print("[NAVER] 노출감시: 지정 문구 결과 없음")
        save_state()
        return

    ad_rank = res.get("ad")
    org_rank = res.get("organic")
    prev_org = cfg.get("last_rank")

    if org_rank is not None:
        search_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔍 네이버 검색 확인", url=url)
        ]])
        if prev_org is None:
            try:
                cid = int(CHAT_ID)
                context.bot.send_message(
                    chat_id=cid,
                    text=(
                        f"📡 [노출감시 시작]\n"
                        f"키워드 '{keyword}'\n"
                        f"광고 : {_fmt_rank(ad_rank)}\n"
                        f"기본 : {_fmt_rank(org_rank)} (광고 제외)"
                    ),
                    reply_markup=search_kb,
                )
            except:
                pass
        elif org_rank != prev_org:
            try:
                cid = int(CHAT_ID)
                context.bot.send_message(
                    chat_id=cid,
                    text=(
                        f"📡 [노출감시] 순위 변경\n"
                        f"키워드 '{keyword}'\n"
                        f"이전 기본 : {_fmt_rank(prev_org)} → 현재 기본 : {_fmt_rank(org_rank)}\n"
                        f"광고 : {_fmt_rank(ad_rank)}"
                    ),
                    reply_markup=search_kb,
                )
            except:
                pass
        cfg["last_rank"] = org_rank

    save_state()
    push_to_dashboard("naver", _build_naver_payload())

# ========= NAVER 리뷰감시 =========
def _parse_review_count_from_html(html: str):
    """
    네이버 플레이스 HTML에서 방문자리뷰, 블로그리뷰 개수를 파싱
    """
    try:
        # 1. og:description 메타태그 추출
        m = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            html
        )
        if not m:
            
            return {"visit": 0, "blog": 0, "source": "none"}

        desc = m.group(1)
        

        # 2. 숫자 추출 (예: 방문자리뷰 523 · 블로그리뷰 172)
        visit = blog = 0
        vm = re.search(r"방문자리뷰\s*([0-9,]+)", desc)
        bm = re.search(r"블로그리뷰\s*([0-9,]+)", desc)
        if vm:
            visit = int(vm.group(1).replace(",", ""))
        if bm:
            blog = int(bm.group(1).replace(",", ""))

        
        return {"visit": visit, "blog": blog, "source": "og:desc"}

    except Exception as e:
        print(f"[ERROR] parse_review_count_from_html: {e}")
        return {"visit": 0, "blog": 0, "source": "exception"}





def get_place_review_stats():
    """
    네이버 플레이스 페이지에서 리뷰 개수를 파싱 (방문자/블로그)
    """
    try:
        if not NAVER_PLACE_ID:
            print("[WARN] NAVER_PLACE_ID not set")
            return {"visit": 0, "blog": 0, "source": "none"}

        url = f"https://pcmap.place.naver.com/place/{NAVER_PLACE_ID}"
        
        r = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        r.encoding = "utf-8"
        

        if r.status_code != 200:
            print("[ERROR] HTTP response not 200")
            return {"visit": 0, "blog": 0, "source": "http_error"}

        stats = _parse_review_count_from_html(r.text)
        
        return stats

    except Exception as e:
        print(f"[ERROR] get_place_review_stats: {e}")
        return {"visit": 0, "blog": 0, "source": "exception"}






def get_latest_visitor_reviews(count: int = 3) -> list:
    """
    네이버 플레이스 GraphQL API로 최신 방문자 리뷰 내용을 가져온다.
    반환: [{"nickname": str, "body": str, "created": str}, ...]
    """
    if not NAVER_PLACE_ID:
        return []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 12; SM-G998N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Content-Type": "application/json",
            "Referer": f"https://m.place.naver.com/place/{NAVER_PLACE_ID}/review/visitor",
            "x-wtm-graphql": "eyJ0eXBlIjoiUExBQ0UifQ==",
        }
        query = """
        query getVisitorReviews($id: String!, $page: Int) {
          visitorReviews(input: {businessId: $id, page: $page, display: %d}) {
            items {
              id
              body
              created
              author { nickname }
            }
          }
        }
        """ % count
        payload = {"query": query, "variables": {"id": NAVER_PLACE_ID, "page": 1}}
        r = requests.post(
            "https://api.place.naver.com/graphql",
            json=payload,
            headers=headers,
            timeout=10,
        )
        if r.status_code != 200:
            return []
        items = r.json().get("data", {}).get("visitorReviews", {}).get("items", [])
        return [
            {
                "nickname": it.get("author", {}).get("nickname", ""),
                "body": (it.get("body") or "").strip(),
                "created": it.get("created", ""),
            }
            for it in items
            if it.get("body", "").strip()
        ]
    except Exception as e:
        print(f"[NAVER] 리뷰 내용 조회 실패: {e}")
        return []


def naver_review_watch_loop(context):
    nav = state.setdefault("naver", {})
    cfg = nav.setdefault("review_watch", {})
    if not cfg.get("enabled"):
        return
    if not NAVER_PLACE_ID:
        return

    now = time.time()
    interval = int(cfg.get("interval", 180))
    last_check = float(cfg.get("last_check", 0.0))

    if now - last_check < interval:
        return

    stats = get_place_review_stats()
    cfg["last_check"] = now

    if not stats:
        print("[NAVER] 리뷰감시: 리뷰 수 파싱 실패")
        save_state()
        push_to_dashboard("naver", _build_naver_payload())
        return

    v = int(stats.get("visit", 0))
    b = int(stats.get("blog", 0))
    t = int(stats.get("total", 0)) or (v + b)

    # 이전 값 (하위호환: 예전에는 숫자만 저장했으므로 처리)
    last = cfg.get("last_count")
    if not isinstance(last, dict):
        cfg["last_count"] = {"visit": v, "blog": b, "total": t}
        save_state()
        push_to_dashboard("naver", _build_naver_payload())
        try:
            send_ctx(
                context,
                "⭐️ [리뷰감시 시작]\n"
                f"🧍 방문자 리뷰: {v}\n"
                f"📝 블로그 리뷰: {b}\n"
                f"💯 총합: {t}"
            )
        except:
            pass
        return

    lv = int(last.get("visit", 0))
    lb = int(last.get("blog", 0))
    lt = int(last.get("total", 0))

    dv = max(0, v - lv)
    db = max(0, b - lb)

    if dv > 0 or db > 0:
        cfg["last_count"] = {"visit": v, "blog": b, "total": t}
        save_state()
        push_to_dashboard("naver", _build_naver_payload())

        msg = "🆕 [리뷰감시] 신규 리뷰 감지!\n"
        if dv > 0:
            msg += f"🧍 방문자 리뷰 +{dv} → {v}\n"
        if db > 0:
            msg += f"📝 블로그 리뷰 +{db} → {b}\n"
        msg += f"💯 총합: {t}"

        reviews = get_latest_visitor_reviews(count=min(dv, 3) if dv > 0 else 1)
        if reviews:
            msg += "\n\n─────────────────"
            for rv in reviews:
                msg += f"\n⭐ {rv['nickname']} ({rv['created']})\n{rv['body']}"

        try:
            send_ctx(context, msg)
        except:
            pass
    else:
        save_state()
        push_to_dashboard("naver", _build_naver_payload())

# ===== Google Calendar 감시 루프 =====
def gcal_watch_loop(ctx):
    """
    주기적으로 3개 구글 캘린더를 확인해서
    - 새 일정 추가
    - 일정 내용/시간 변경
    - 일정 삭제
    - 시작 하루 전
    - 시작 1시간 전
    - 종료 1시간 전
    - 종료 시점
    을 텔레그램으로 알림.
    감시 범위: 지금 기준 과거 7일 ~ 앞으로 60일.
    """
    global state

    gcal = state.setdefault("gcal", {})
    if not gcal.get("enabled"):
        return

    # API 클라이언트 생성
    try:
        service = init_gcal_service()
    except Exception as e:
        print("[GCAL] init error:", e)
        return

    now = datetime.now(KST)

    calendars = gcal.setdefault("calendars", {})

    # state 파일에 gcal 정보가 없을 때 기본 세팅
    if not calendars:
        calendars.update({
            "dogentle.gn@gmail.com": {
                "name": "📅 호텔",
                "known_events": {},
                "last_sync": None,
            },
            "v5ed5c688kem61967f0g5jg57g@group.calendar.google.com": {
                "name": "💼 강남점",
                "known_events": {},
                "last_sync": None,
            },
            "t2of8dh2bllmfigb6ef8ugi7r8@group.calendar.google.com": {
                "name": "🏠 댕큐",
                "known_events": {},
                "last_sync": None,
            },
        })

    # 캘린더별로 순회
    for cal_id, info in calendars.items():
        cal_name = info.get("name", cal_id)
        known = info.get("known_events", {}) or {}

        # 이번에 API로 가져온 이벤트 ID들
        current_ids = set()

        try:
            events_result = service.events().list(
                calendarId=cal_id,
                timeMin=(now - timedelta(days=7)).isoformat(),
                timeMax=(now + timedelta(days=60)).isoformat(),
                singleEvents=True,
                orderBy="startTime",
            ).execute()
        except Exception as e:
            print(f"[GCAL] fetch error {cal_id}:", e)
            continue

        events = events_result.get("items", [])

        # ----- 이벤트 목록 처리 -----
        for event in events:
            event_id = event.get("id")
            if not event_id:
                continue

            current_ids.add(event_id)

            summary = event.get("summary", "(제목 없음)")

            # 시작/종료 시간 파싱
            start_raw = event.get("start", {}).get("dateTime") or event.get("start", {}).get("date")
            end_raw = event.get("end", {}).get("dateTime") or event.get("end", {}).get("date")
            if not start_raw or not end_raw:
                continue

            # dateTime or 종일(date)
            if "T" in start_raw:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(KST)
                end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).astimezone(KST)
            else:
                # 종일 일정: 종료일은 보통 +1일 들어오므로 그대로 사용
                start_dt = datetime.fromisoformat(start_raw + "T00:00:00+09:00")
                end_dt = datetime.fromisoformat(end_raw + "T00:00:00+09:00")

            start_str = start_dt.isoformat()
            end_str = end_dt.isoformat()

            # 이전 상태 불러오기 (구버전 형태였으면 새 포맷으로 마이그레이션)
            old = known.get(event_id) or {}
            notified = old.get("notified")
            if not isinstance(notified, dict):
                # 예전 플래그(added 등)만 있었던 경우를 위해 기본값으로 초기화
                notified = {
                    "created": False,
                    "start_1d": False,
                    "start_1h": False,
                    "start_0h": False,  # ✅ 일정 시작 시점 알림용 플래그
                    "end_1h": False,
                    "end": False,
                }


            prev_summary = old.get("summary")
            prev_start = old.get("start")
            prev_end = old.get("end")

            # ----- 1) 일정 생성 알림 -----
            if not notified.get("created", False):
                msg = (
                    f"{cal_name} 새 일정 등록\n"
                    f"🗓 {summary}\n"
                    f"⏰ {start_dt:%m/%d %H:%M} ~ {end_dt:%m/%d %H:%M}"
                )
                send_ctx(ctx, msg)
                notified["created"] = True

            # ----- 2) 일정 내용/시간 변경 알림 -----
            # 이전에 한 번이라도 저장된 값이 있고, 내용이 달라졌다면
            if (
                prev_summary is not None
                and (
                    prev_summary != summary
                    or prev_start != start_str
                    or prev_end != end_str
                )
            ):
                msg = (
                    f"{cal_name} 일정 변경\n"
                    f"📝 {summary}\n"
                    f"⏰ {start_dt:%m/%d %H:%M} ~ {end_dt:%m/%d %H:%M}"
                )
                send_ctx(ctx, msg)

            # ----- 3) 시간 기준 알림들 -----
            # 시작 1일 전
            if (
                not notified.get("start_1d", False)
                and now >= start_dt - timedelta(days=1)
                and now < start_dt
            ):
                msg = (
                    f"{cal_name} 일정 하루 전 알림\n"
                    f"🗓 {summary}\n"
                    f"내일 {start_dt:%m/%d %H:%M} 시작"
                )
                send_ctx(ctx, msg)
                notified["start_1d"] = True

            # 시작 1시간 전
            if (
                not notified.get("start_1h", False)
                and now >= start_dt - timedelta(hours=1)
                and now < start_dt
            ):
                msg = (
                    f"{cal_name} 일정 1시간 전\n"
                    f"🕒 {summary}\n"
                    f"{start_dt:%m/%d %H:%M} 시작 예정"
                )
                send_ctx(ctx, msg)
                notified["start_1h"] = True

            # 일정 시작 시점
            if (
                not notified.get("start_0h", False)
                and now >= start_dt
                and now < start_dt + timedelta(minutes=5)
            ):
                msg = (
                    f"{cal_name} 일정 시작\n"
                    f"▶️ {summary}\n"
                    f"{start_dt:%m/%d %H:%M} 지금 시작"
                )
                send_ctx(ctx, msg)
                notified["start_0h"] = True

            # 종료 1시간 전
            if (
                not notified.get("end_1h", False)
                and now >= end_dt - timedelta(hours=1)
                and now < end_dt
            ):
                msg = (
                    f"{cal_name} 종료 1시간 전\n"
                    f"📍 {summary}\n"
                    f"{end_dt:%m/%d %H:%M} 종료 예정"
                )
                send_ctx(ctx, msg)
                notified["end_1h"] = True


            # 종료 시점
            if (
                not notified.get("end", False)
                and now >= end_dt
            ):
                msg = (
                    f"{cal_name} 일정 종료\n"
                    f"✅ {summary}\n"
                    f"{end_dt:%m/%d %H:%M} 종료"
                )
                send_ctx(ctx, msg)
                notified["end"] = True

            # 최신 상태 저장
            known[event_id] = {
                "summary": summary,
                "start": start_str,
                "end": end_str,
                "notified": notified,
            }

        # ----- 4) 삭제된 일정 감지 -----
        # 이번 fetch에 없는 ID들 = known에는 있었는데 사라진 일정들
        known_ids = set(known.keys())
        deleted_ids = known_ids - current_ids

        for del_id in list(deleted_ids):
            old = known.get(del_id) or {}
            notif = old.get("notified") or {}
            summary_old = old.get("summary", "(제목 없음)")
            start_old_str = old.get("start")

            # 시작 시간이 미래였던 일정만 "삭제" 알림 대상
            future_event = False
            if start_old_str:
                try:
                    dt = datetime.fromisoformat(start_old_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=KST)
                    else:
                        dt = dt.astimezone(KST)
                    if dt > now:
                        future_event = True
                except Exception:
                    # 파싱 실패 시엔 보수적으로 미래 일정으로 본다
                    future_event = True

            # 한 번이라도 등록된 적 있고, 아직 시작 전이었다면 → 삭제 알림
            if future_event and notif.get("created", False):
                msg = (
                    f"{cal_name} 일정 삭제\n"
                    f"🗑 {summary_old}"
                )
                send_ctx(ctx, msg)

            # 어떤 경우든 더 이상 추적하지 않음
            known.pop(del_id, None)

        info["known_events"] = known

    gcal["calendars"] = calendars
    state["gcal"] = gcal
    save_state()

# ===== Google Calendar 간트차트 생성 =====
GCAL_GANTT_COLORS = {
    "dogentle.gn@gmail.com": "#AECBFA",   # 호텔(파스텔 블루)
    "v5ed5c688kem61967f0g5jg57g@group.calendar.google.com": "#FFCDD2",  # 강남점(파스텔 레드/핑크)
    "t2of8dh2bllmfigb6ef8ugi7r8@group.calendar.google.com": "#E1C8FF",  # 댕큐(파스텔 라벤더)
}

# ===== Google Calendar 간트차트 (Apple Calendar + Emoji 스타일) =====

from datetime import date

def get_week_of_month(d: date) -> int:
    """해당 날짜가 그 달의 몇 주차인지 계산 (월요일=0 기준)"""
    first = d.replace(day=1)
    offset = first.weekday()
    week = (d.day + offset - 1) // 7 + 1
    return week

def make_week_range_title(start_d: date, end_d: date) -> str:
    """시작~끝 날짜를 사용해 '11월 4주차 일정' 또는 '11월 4~5주차 일정' 등으로 제목 생성"""
    s_year, s_month = start_d.year, start_d.month
    e_year, e_month = end_d.year, end_d.month
    s_week = get_week_of_month(start_d)
    e_week = get_week_of_month(end_d)

    # 같은 달 & 같은 주차
    if (s_year == e_year) and (s_month == e_month) and (s_week == e_week):
        return f"{s_year}년 {s_month}월 {s_week}주차 일정"

    # 같은 달 내 연속 주차
    if (s_year == e_year) and (s_month == e_month):
        return f"{s_year}년 {s_month}월 {s_week}~{e_week}주차 일정"

    # 달이 넘어가는 경우
    return f"{s_year}년 {s_month}월 {s_week}주차 ~ {e_year}년 {e_month}월 {e_week}주차 일정"

def build_gcal_gantt_image(days: int = 7):
    """앞으로 days일 구글 캘린더 일정 간트차트 PNG 생성
    - 제목 : 'YYYY년 M월 D일 ~ YYYY년 M월 D일 일정'  (주차 표시 없음)
    - Y축 라벨 : 차트 영역 바깥(왼쪽)으로 빼서 막대와 겹치지 않게 표시
    """
    global state

    # ===== 날짜 범위 =====
    now = datetime.now(KST)
    start_day = datetime(now.year, now.month, now.day, tzinfo=KST)
    end_day = start_day + timedelta(days=days)
    end_for_title = end_day - timedelta(days=1)

    # ===== Google Calendar API =====
    try:
        service = init_gcal_service()
    except Exception:
        return None

    # ===== 캘린더 구조 =====
    gcal = state.setdefault("gcal", {})
    calendars = gcal.setdefault("calendars", {})

    if not calendars:
        calendars.update({
            "dogentle.gn@gmail.com": {"name": "호텔"},
            "v5ed5c688kem61967f0g5jg57g@group.calendar.google.com": {"name": "강남점"},
            "t2of8dh2bllmfigb6ef8ugi7r8@group.calendar.google.com": {"name": "댕큐"},
        })
        state["gcal"] = gcal
        save_state()

    # ===== 파스텔 색상 =====
    COLOR_MAP = {
        "dogentle.gn@gmail.com": "#AECBFA",
        "v5ed5c688kem61967f0g5jg57g@group.calendar.google.com": "#FFCDD2",
        "t2of8dh2bllmfigb6ef8ugi7r8@group.calendar.google.com": "#E1C8FF",
    }

    # ===== 이모지 제거 함수 =====
    import re
    emoji_pattern = re.compile(
        "["  
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "]+",
        flags=re.UNICODE,
    )

    def remove_emoji(text):
        return emoji_pattern.sub("", str(text or "")).strip()

    # ===== 이벤트 수집 =====
    events = []
    for cal_id, info in calendars.items():
        raw_cal_name = info.get("name") or cal_id          # 원본 (📅 포함 가능)
        cal_name = remove_emoji(raw_cal_name) or raw_cal_name  # 표시용(이모지 제거)

        try:
            result = service.events().list(
                calendarId=cal_id,
                timeMin=start_day.isoformat(),
                timeMax=end_day.isoformat(),
                singleEvents=True,
                orderBy="startTime",
            ).execute()
        except Exception:
            continue

        for ev in result.get("items", []):
            if ev.get("status") == "cancelled":
                continue

            summary = remove_emoji(ev.get("summary", "(제목 없음)"))

            s_raw = ev.get("start", {}).get("dateTime") or ev.get("start", {}).get("date")
            e_raw = ev.get("end", {}).get("dateTime") or ev.get("end", {}).get("date")
            if not s_raw or not e_raw:
                continue

            try:
                if "T" in s_raw:
                    sdt = datetime.fromisoformat(s_raw.replace("Z", "+00:00")).astimezone(KST)
                    edt = datetime.fromisoformat(e_raw.replace("Z", "+00:00")).astimezone(KST)
                else:
                    sdt = datetime.fromisoformat(s_raw + "T00:00:00+09:00")
                    edt = datetime.fromisoformat(e_raw + "T00:00:00+09:00")
            except Exception:
                continue

            # 범위 밖은 제외
            if edt <= start_day or sdt >= end_day:
                continue

            events.append({
                "cal_id": cal_id,
                "cal_name": cal_name,         # 이모지 제거된 이름
                "raw_cal_name": raw_cal_name, # 원본 이름 (marker 판별용)
                "summary": summary,
                "start": max(sdt, start_day),
                "end": min(edt, end_day),
            })

    if not events:
        return None

    events.sort(key=lambda x: x["start"])

    # ===== Figure =====
    n = len(events)
    height = max(3, min(6, 0.37 * n + 2))
    fig, ax = plt.subplots(figsize=(8, height))
    ax.set_facecolor("#F7F7F7")
    for s in ax.spines.values():
        s.set_visible(False)

    # ===== bar 그리기 =====
    bar_h = 0.25
    for idx, ev in enumerate(events):
        s = mdates.date2num(ev["start"])
        e = mdates.date2num(ev["end"])
        w = max(e - s, 1 / 24)  # 최소 1시간 너비
        color = COLOR_MAP.get(ev["cal_id"], "#CCCCCC")

        ax.add_patch(
            Rectangle(
                (s, idx - bar_h / 2),
                w,
                bar_h,
                linewidth=0,
                facecolor=color,
                alpha=0.9,
            )
        )

    # ===== 축 범위 =====
    ax.set_ylim(-0.5, len(events) - 0.5)
    ax.set_xlim(
        mdates.date2num(start_day),
        mdates.date2num(end_day) + 0.1,
    )

    # ===== X 축 =====
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    plt.xticks(rotation=45, fontsize=7, color="#888888")
    ax.set_yticks([])

    # ===== 캘린더별 기호 =====
    def marker(name):
        name = str(name or "")
        if "호텔" in name:
            return "●"
        if "강남" in name:
            return "◆"
        if "댕큐" in name:
            return "■"
        return "•"

    # ===== 라벨 (차트 바깥으로 빼기) =====
    for idx, ev in enumerate(events):
        mk = marker(ev["raw_cal_name"])
        label = f"{mk} {ev['cal_name']} · {ev['summary']}"
        if len(label) > 30:
            label = label[:28] + "…"

        # 축의 왼쪽 끝(x=0.0)에서 시작해서, 왼쪽으로 70포인트 밀어서
        # 여백 안에 들어가도록 배치 (ha='right'로 정렬)
        ax.annotate(
            label,
            xy=(0.0, idx),                          # 축의 왼쪽 경계
            xycoords=("axes fraction", "data"),
            xytext=(-50, 0),                        # 왼쪽으로 70pt 이동
            textcoords="offset points",
            fontsize=8,
            color="#444444",
            va="center",
            ha="right",
            clip_on=False,
        )

    # ===== Grid =====
    ax.grid(
        which="major",
        axis="x",
        linestyle="--",
        linewidth=0.3,
        color="#E4E4E4",
        alpha=0.4,
    )

    # ===== 제목 (주차 제거, 날짜 범위로 표시) =====
    if start_day.year == end_for_title.year:
        if start_day.month == end_for_title.month:
            title = (
                f"{start_day.year}년 {start_day.month}월 "
                f"{start_day.day}일 ~ {end_for_title.day}일 일정"
            )
        else:
            title = (
                f"{start_day.year}년 {start_day.month}월 {start_day.day}일 ~ "
                f"{end_for_title.month}월 {end_for_title.day}일 일정"
            )
    else:
        title = (
            f"{start_day.year}년 {start_day.month}월 {start_day.day}일 ~ "
            f"{end_for_title.year}년 {end_for_title.month}월 {end_for_title.day}일 일정"
        )

    ax.set_title(
        title,
        fontsize=12,
        fontweight="bold",
        color="#333",
        pad=16,
    )

    # 차트 왼쪽에 라벨 들어갈 자리 확보
    plt.subplots_adjust(left=0.30, right=0.98, top=0.88, bottom=0.15)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=160)
    plt.close(fig)
    buf.seek(0)
    return buf



# ===== iCloud 미리알림 감시 루프 =====
def reminder_watch_loop(ctx):
    """
    snapshot.json 을 읽어서,
    설정에 맞는 미리알림에 대해 interval_min 분마다 반복 알림을 보낸다.
    (00:00~07:59 심야에는 알림만 멈춤)

    ✅ 개선: 같은 루프에서 여러 건 감지되면 "한 번에 1개 메시지"로 묶어서 보냄
    """
    now = datetime.now(KST)

    # ⏸ 00:00 ~ 07:59 사이에는 알림만 잠깐 멈춤
    if 0 <= now.hour < 8:
        return

    global state

    rem = state.setdefault("reminder", {})
    if not rem.get("enabled"):
        return

    _refresh_reminder_known_from_snapshot()

    interval_min = int(rem.get("interval_min", 60))
    now_ts = time.time()
    known = rem.get("known") or {}
    if not known:
        save_state()
        return

    # =========================
    # 1) 이번 루프에서 보낼 항목들을 모은다
    # =========================
    buckets = {}  # { list_name: [line, line, ...] }
    sent_uids = []  # 실제로 이번에 알림 대상으로 잡힌 uid들

    def pick_icon(list_name: str) -> str:
        if list_name == "출근":
            return "🚀"
        if list_name == "회사":
            return "🏢"
        if list_name == "외출":
            return "🚶"
        return "⏰"

    for uid, info in list(known.items()):
        try:
            # 완료일(due_ts)이 없는 항목은 자동 감시 제외
            due_ts = info.get("due_ts")
            if not due_ts:
                continue
            if info.get("completed"):
                continue

            last = float(info.get("last_alert_ts", 0.0) or 0.0)
            if last and now_ts - last < interval_min * 60:
                continue

            list_name = (info.get("list_name") or "").strip()
            title = info.get("title") or "(제목 없음)"
            short_due = _format_short_due(due_ts)

            icon = pick_icon(list_name)
            header = f"{icon} {list_name}" if list_name else f"{icon} (분류 없음)"

            # 한 줄로 깔끔하게: - 제목 (날짜)
            line = f"- {title}"
            if short_due:
                line += f" ({short_due})"

            buckets.setdefault(header, []).append(line)
            sent_uids.append(uid)

        except Exception as e:
            print("[REMINDER] reminder_watch_loop 항목 처리 오류:", e)
            continue

    # =========================
    # 2) 모은 게 있으면 한 번만 보낸다
    # =========================
    if buckets:
        # 너무 길어지는 걸 막기 위해 섹션별 최대 N개만 보여주고 나머지는 요약
        MAX_PER_SECTION = 10

        total_count = sum(len(v) for v in buckets.values())
        msg_lines = [f"⏰ 미리알림 {total_count}건"]

        # 보기 좋게 아이콘 순서를 어느 정도 고정(원하면 나중에 바꿔도 됨)
        ordered_headers = []
        for h in ["🚀 출근", "🏢 회사", "🚶 외출"]:
            if h in buckets:
                ordered_headers.append(h)
        # 나머지 헤더들
        for h in buckets.keys():
            if h not in ordered_headers:
                ordered_headers.append(h)

        for header in ordered_headers:
            items = buckets.get(header) or []
            if not items:
                continue

            msg_lines.append("")  # 빈 줄
            msg_lines.append(header)

            if len(items) <= MAX_PER_SECTION:
                msg_lines.extend(items)
            else:
                msg_lines.extend(items[:MAX_PER_SECTION])
                msg_lines.append(f"... 외 {len(items) - MAX_PER_SECTION}건")

        big_msg = "\n".join(msg_lines).strip()

        try:
            send_ctx(ctx, big_msg)
        except Exception:
            # 전송 실패해도 last_alert_ts 업데이트는 하지 않는 게 안전함
            # (안 그러면 "보냈다고 착각"하고 다음에 안 울릴 수 있음)
            save_state()
            return

        # =========================
        # 3) 실제로 알림 대상으로 잡힌 것만 last_alert_ts 업데이트
        # =========================
        for uid in sent_uids:
            try:
                info = known.get(uid)
                if isinstance(info, dict):
                    info["last_alert_ts"] = now_ts
            except Exception:
                pass

    save_state()

def naver_review_check_once(update):
    if not NAVER_PLACE_ID:
        reply(update, "NAVER_PLACE_ID가 설정되어 있지 않습니다. .env에 플레이스 ID를 입력하세요.")
        return

    stats = get_place_review_stats()
    if not stats:
        reply(update, "리뷰현황 조회에 실패했습니다.")
        return

    v = int(stats.get("visit", 0))
    b = int(stats.get("blog", 0))
    t = int(stats.get("total", 0)) or (v + b)

    nav = state.setdefault("naver", {})
    cfg = nav.setdefault("review_watch", {})
    cfg["last_count"] = {"visit": v, "blog": b, "total": t}
    save_state()

    reply(
        update,
        "📊 리뷰현황\n"
        f"🧍 방문자 리뷰: {v}\n"
        f"📝 블로그 리뷰: {b}\n"
        f"💯 총합: {t}"
    )


# ========= 즉시 노출 조회 =========
def naver_rank_check_once(update):
    nav = state.setdefault("naver", {})
    cfg = nav.setdefault("rank_watch", {})

    keyword = (cfg.get("keyword") or "").strip()
    marker = (cfg.get("marker") or "").strip()

    if not (keyword and marker):
        reply(
            update,
            "노출감시 설정이 되어 있지 않습니다.\n"
            "먼저 '노출감시' 명령으로 키워드와 식별 문구를 설정해 주세요."
        )
        return

    try:
        url = _naver_search_url(keyword)
        r = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        html = r.text
        res = detect_place_ranks(html, marker)
    except Exception as e:
        print("[NAVER] 노출현황 조회 실패:", e)
        reply(update, "노출현황 조회 중 오류가 발생했습니다.")
        return

    search_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔍 네이버 검색 확인", url=url)
    ]])
    if not res:
        update.message.reply_text(
            "📡 노출현황 알림\n"
            f"🔍 키워드: '{keyword}'\n"
            "⚠️ 검색 결과에서 지정한 매장을 찾지 못했습니다.\n"
            "설정하신 키워드/문구를 다시 한 번 확인해 주세요.",
            reply_markup=search_kb,
        )
    else:
        ad_rank = res.get("ad")
        org_rank = res.get("organic")
        if org_rank is not None:
            cfg["last_rank"] = org_rank
            save_state()
        update.message.reply_text(
            "📡 노출현황 알림\n"
            f"🔍 키워드: '{keyword}'\n"
            f"💚 광고 노출: {_fmt_rank(ad_rank)}\n"
            f"📍 기본 노출: {_fmt_rank(org_rank)} (광고 제외)",
            reply_markup=search_kb,
        )

# ========= INLINE MODE HANDLER =========
def on_mode_select(update, context):
    """
    인라인 모드 선택 콜백 처리:
    - mode_pa      : 개인비서
    - mode_naver   : 네이버
    - mode_coin    : 자산관리(코인)
    - mode_random  : 랜덤문구
    """
    q = update.callback_query
    cid = q.message.chat_id
    data = q.data

    # 다른 사람이 누르면 무시 (기존 CHAT_ID 보호 로직 유지)
    if CHAT_ID and str(cid) != CHAT_ID:
        try:
            q.answer()
        except:
            pass
        return

    # 공통으로 로딩표시 제거
    try:
        q.answer()
    except:
        pass

    # 모드 분기
    if data == "mode_pa":
        set_mode(cid, "pa")
        text = "개인비서 모드로 전환되었습니다."
    elif data == "mode_naver":
        set_mode(cid, "naver")
        text = "네이버 모드로 전환되었습니다."
    elif data == "mode_coin":
        # 기존 코인 기능 = 자산관리 모드
        set_mode(cid, "coin")
        text = "자산관리 모드로 전환되었습니다."
    elif data == "mode_random":
        set_mode(cid, "random")
        text = "랜덤문구 모드로 전환되었습니다."
    else:
        # 우리가 관리하지 않는 콜백이면 무시
        return

    # 새 모드 안내 + 해당 모드용 ReplyKeyboard 전송
    try:
        context.bot.send_message(
            chat_id=cid,
            text=text,
            reply_markup=MAIN_KB(cid),
        )
    except:
        pass


# ========= TEXT HANDLER =========
def on_text(update, context):
    if not only_owner(update):
        return

    text = (update.message.text or "").strip()
    cid = update.effective_chat.id

    # ===== 모드별 도움말 =====
    if text in ("도움말", "help", "HELP"):
        reply(update, get_help_by_mode(cid))
        return

    # 호텔 랜덤 리뷰
    if (
        text == "호텔"
        or text.startswith("/호텔")
        or text.lower().startswith("/hotel")
    ):
        update.message.reply_text(build_random_hotel_review())
        return

    # ===== pending 상태 복원 =====
    pend = get_pending(cid)
    action, step, data = None, None, {}
    if pend:
        action = pend.get("action")
        step = pend.get("step")
        data = pend.get("data", {}) or {}

        # 공통 취소 처리
        if text == "취소":
            clear_pending(cid)
            reply(update, "취소되었습니다.")
            return
    # ===== 킴스 통계: 기간조회 pending 처리 =====
    if action == "kims_stats_period" and step == "input":
        if text == "취소":
            clear_pending(cid)
            reply(update, "취소되었습니다.", kb=KIMS_STATS_MENU_KB)
            return

        raw = text.replace(" ", "")
        if "~" not in raw:
            reply(update, "형식이 잘못됐어요.\n예) 2026-01-01~2026-01-08", kb=CANCEL_KB)
            return

        a, b = raw.split("~", 1)
        a = a.strip()
        b = b.strip()

        # 날짜 형식 검증
        try:
            datetime.strptime(a, "%Y-%m-%d")
            datetime.strptime(b, "%Y-%m-%d")
        except Exception:
            reply(update, "날짜 형식은 YYYY-MM-DD 이어야 해요.\n예) 2026-01-01~2026-01-08", kb=CANCEL_KB)
            return

        clear_pending(cid)

        d = _kims_stats_fetch({"from": a, "to": b})
        msg = f"📊 킴스 통계 ({a} ~ {b})\n\n" + _kims_stats_format(d, top_n=5)
        reply(update, msg, kb=KIMS_STATS_MENU_KB)
        return
    # ===== 급증 알림: 기준 선택 pending 처리 =====
    if action == "kims_spike_threshold" and step in ("click", "visit"):
        if text == "취소":
            clear_pending(cid)
            reply(update, "취소했어요.", kb=KIMS_SPIKE_MENU_KB)
            return

        m = re.match(r"^기준\s*(\d+)$", text.strip())
        if not m:
            reply(update, "버튼(기준 3/5/10)으로 선택해 주세요.", kb=KIMS_THRESHOLD_KB)
            return

        th = int(m.group(1))
        if th <= 0:
            reply(update, "기준은 양수여야 해요.", kb=KIMS_THRESHOLD_KB)
            return

        ks = state.setdefault("kims_stats", {})
        sp = ks.setdefault("spike", {})

        if step == "click":
            sp["click_threshold"] = th
            save_state()
            clear_pending(cid)
            reply(update, f"🖱 클릭 급증 기준을 {th}로 설정했어요.", kb=KIMS_SPIKE_MENU_KB)
            return

        if step == "visit":
            sp["visit_threshold"] = th
            save_state()
            clear_pending(cid)
            reply(update, f"👀 방문(PV) 급증 기준을 {th}로 설정했어요.", kb=KIMS_SPIKE_MENU_KB)
            return

    # ===== 개인비서 모드: 캘린더 + 업무 스케줄러 =====
    mode = get_mode(cid)
    if mode in ("pa", "mode_pa"):
        # ✅ 킴스 통계 메뉴 (개인비서 모드에서 최우선)
        # =========================
        # 🚨 급증 알림 설정 메뉴
        # =========================
        if text in ("🚨 급증알림 설정", "급증알림 설정"):
            reply(update, "🚨 급증 알림 설정입니다.", kb=KIMS_SPIKE_MENU_KB)
            return

        # 설정 메뉴에서 통계 메뉴로 돌아가기
        if text in ("◀️ 통계로",):
            reply(update, "📊 킴스 통계입니다. 기간을 선택해 주세요.", kb=KIMS_STATS_MENU_KB)
            return

        # -------------------------
        # 토글: 클릭 감시 ON/OFF
        # -------------------------
        if text in ("🖱 클릭 알림 ON/OFF",):
            ks = state.setdefault("kims_stats", {})
            sp = ks.setdefault("spike", {})
            cur = bool(sp.get("click_enabled", False))
            newv = not cur
            sp["click_enabled"] = newv

            # ON으로 켜는 순간: 기준 선택 유도
            if newv:
                # 기준값 초기화(현재 누적 기준)
                d = _kims_stats_fetch({"range": "today"})
                sp["last_clicks"] = _kims_sum_clicks(d)
                pv, _ = _kims_get_pv_uv(d)
                sp["last_pv"] = pv
                sp["last_check"] = time.time()
                save_state()

                set_pending(cid, "kims_spike_threshold", "click", {})
                reply(update, "🖱 클릭 급증 기준을 선택해 주세요.", kb=KIMS_THRESHOLD_KB)
            else:
                save_state()
                reply(update, "🖱 클릭 급증 알림을 껐어요.", kb=KIMS_SPIKE_MENU_KB)
            return

        # -------------------------
        # 토글: 방문(PV) 감시 ON/OFF
        # -------------------------
        if text in ("👀 방문 알림 ON/OFF",):
            ks = state.setdefault("kims_stats", {})
            sp = ks.setdefault("spike", {})
            cur = bool(sp.get("visit_enabled", False))
            newv = not cur
            sp["visit_enabled"] = newv

            if newv:
                d = _kims_stats_fetch({"range": "today"})
                sp["last_clicks"] = _kims_sum_clicks(d)
                pv, _ = _kims_get_pv_uv(d)
                sp["last_pv"] = pv
                sp["last_check"] = time.time()
                save_state()

                set_pending(cid, "kims_spike_threshold", "visit", {})
                reply(update, "👀 방문(PV) 급증 기준을 선택해 주세요.", kb=KIMS_THRESHOLD_KB)
            else:
                save_state()
                reply(update, "👀 방문(PV) 급증 알림을 껐어요.", kb=KIMS_SPIKE_MENU_KB)
            return

        # -------------------------
        # 기준 선택(수동 진입)
        # -------------------------
        if text in ("🧷 기준 선택(클릭)",):
            set_pending(cid, "kims_spike_threshold", "click", {})
            reply(update, "🖱 클릭 급증 기준을 선택해 주세요.", kb=KIMS_THRESHOLD_KB)
            return

        if text in ("🧷 기준 선택(방문)",):
            set_pending(cid, "kims_spike_threshold", "visit", {})
            reply(update, "👀 방문(PV) 급증 기준을 선택해 주세요.", kb=KIMS_THRESHOLD_KB)
            return


        # 0) 캘린더 서브 메뉴 열기
        if text in ("📅 캘린더 메뉴", "캘린더 메뉴"):
            gcal = state.setdefault("gcal", {})
            enabled = bool(gcal.get("enabled", False))
            status = "📡 감시 중" if enabled else "📴 감시 꺼짐"
            reply(
                update,
                "📅 캘린더 메뉴입니다.\n"
                f"현재 상태: {status}\n\n"
                "원하는 기능을 선택해 주세요.",
                kb=GCAL_MENU_KB,
            )
            return

        # 1) 구글 캘린더 알림 토글
        if text in ("📅 캘린더 알림", "캘린더 알림"):
            gcal = state.setdefault("gcal", {})
            current = bool(gcal.get("enabled", False))
            gcal["enabled"] = not current
            save_state()
            if gcal["enabled"]:
                reply(update, "📅 구글 캘린더 알림을 켰어요.")
            else:
                reply(update, "📴 구글 캘린더 알림을 껐어요.")
            return


        # 예전 텍스트 명령 호환
        if text in ("캘린더 알림 켜", "캘린더알림켜"):
            gcal = state.setdefault("gcal", {})
            gcal["enabled"] = True
            save_state()
            reply(update, "📅 구글 캘린더 알림을 켰어요.")
            return

        if text in ("캘린더 알림 꺼", "캘린더알림꺼"):
            gcal = state.setdefault("gcal", {})
            gcal["enabled"] = False
            save_state()
            reply(update, "📴 구글 캘린더 알림을 껐어요.")
            return

        # 1-1) 캘린더 감시 ON/OFF (새 메뉴용)
        if text in ("📡 캘린더 감시 ON/OFF", "캘린더 감시"):
            gcal = state.setdefault("gcal", {})
            current = bool(gcal.get("enabled", False))
            gcal["enabled"] = not current
            save_state()
            if gcal["enabled"]:
                msg = "📡 캘린더 감시를 켰어요.\n앞으로 일정 변화를 자동으로 감시해서 알려줄게요."
            else:
                msg = "📴 캘린더 감시를 껐어요.\n더 이상 일정 변경 알림을 보내지 않아요."
            reply(update, msg, kb=GCAL_MENU_KB)
            return

        # 1-2) 7일 / 30일 간트차트
        if text in ("🗓 7일 간트차트 보기", "7일 간트차트"):
            buf = build_gcal_gantt_image(7)
            if not buf:
                reply(update, "앞으로 7일 안에 표시할 일정이 없어요.", kb=GCAL_MENU_KB)
            else:
                try:
                    context.bot.send_photo(
                        chat_id=cid,
                        photo=buf,
                        caption="🗓 앞으로 7일 일정 간트차트",
                        reply_markup=GCAL_MENU_KB,
                    )
                except Exception as e:
                    print("[GCAL GANTT] send error (7d):", e)
                    reply(update, "차트를 보내는 중에 오류가 났어요.", kb=GCAL_MENU_KB)
            return

        if text in ("🗓 30일 간트차트 보기", "30일 간트차트"):
            buf = build_gcal_gantt_image(30)
            if not buf:
                reply(update, "앞으로 30일 안에 표시할 일정이 없어요.", kb=GCAL_MENU_KB)
            else:
                try:
                    context.bot.send_photo(
                        chat_id=cid,
                        photo=buf,
                        caption="🗓 앞으로 30일 일정 간트차트",
                        reply_markup=GCAL_MENU_KB,
                    )
                except Exception as e:
                    print("[GCAL GANTT] send error (30d):", e)
                    reply(update, "차트를 보내는 중에 오류가 났어요.", kb=GCAL_MENU_KB)
            return


        # 2) 업무 스케줄러 서브 메뉴 열기
        if text in ("🗂 업무 스케줄러", "업무 스케줄러"):
            reply(update, "어떤 작업을 할까요?", kb=REMINDER_MENU_KB)
            return

        # 3) 매출 서브 메뉴 열기
        if text in ("💰 매출", "매출"):
            reply(update, "어떤 매출을 보고 싶나요?", kb=SALES_MENU_KB)
            return
        # 3-1) 킴스 통계 서브 메뉴 열기
        if text in ("📊 킴스 통계", "킴스 통계", "통계"):
            reply(update, "📊 킴스 통계입니다. 기간을 선택해 주세요.", kb=KIMS_STATS_MENU_KB)
            return

        # 3-2) 킴스 통계 조회: 오늘/어제/7일/30일
        if text in ("📆 오늘", "오늘"):
            _kims_stats_show_range(update, "today", "오늘")
            return

        if text in ("🕰 어제", "어제"):
            _kims_stats_show_range(update, "yesterday", "어제")
            return

        if text in ("🗓 7일", "7일"):
            _kims_stats_show_range(update, "week", "최근 7일")
            return

        if text in ("🗓 30일", "30일"):
            _kims_stats_show_range(update, "month", "최근 30일")
            return

        # 3-3) 기간조회: 입력 유도 (YYYY-MM-DD~YYYY-MM-DD)
        if text in ("📅 기간조회", "기간조회"):
            set_pending(cid, "kims_stats_period", "input", {})
            reply(update, "기간을 이렇게 입력해 주세요:\n예) 2026-01-01~2026-01-08", kb=CANCEL_KB)
            return
    

        # 3) 업무 감시 ON/OFF
        if text in (
            "📡 업무 감시 ON/OFF",
            "업무 감시 ON/OFF",
            "⏰ 미리알림 알림",
            "미리알림 알림",
            "미리알림알림",
        ):
            rem = state.setdefault("reminder", {})
            current = bool(rem.get("enabled", False))
            rem["enabled"] = not current
            save_state()
            if rem["enabled"]:
                reply(
                    update,
                    "📡 업무 감시를 켰어요.\n"
                    "• 출근: 깃발 표시된 항목만 감시\n"
                    "• 회사/외출: 미완료 + 오늘까지의 업무만 감시",
                    kb=REMINDER_MENU_KB,
                )
            else:
                reply(update, "📴 업무 감시를 껐어요.", kb=REMINDER_MENU_KB)
            return

        # 4) 감시 주기 설정 안내 (버튼) → 실제 변경은 '미리알림 간격 X'
        if text in ("⏱️ 감시 주기 설정", "감시 주기 설정"):
            reply(
                update,
                "변경할 주기를 분 단위 숫자로 보내 주세요.\n"
                "예) 미리알림 간격 30",
                kb=REMINDER_MENU_KB,
            )
            return

        # 예전 텍스트 명령 호환 (미리알림 간격 60)
        if text.startswith("미리알림 간격"):
            try:
                num = int(text.replace("미리알림 간격", "").strip())
                rem = state.setdefault("reminder", {})
                rem["interval_min"] = max(1, num)
                save_state()
                reply(
                    update,
                    f"⏱️ 감시 주기를 {num}분으로 설정했어요.",
                    kb=REMINDER_MENU_KB,
                )
            except Exception:
                reply(update, "간격은 숫자(분)으로 입력해 주세요. 예) 미리알림 간격 60")
            return

        # 새 방식: 숫자만 보내면 감시 주기 변경
        if text.isdigit():
            try:
                num = int(text)
                rem = state.setdefault("reminder", {})
                rem["interval_min"] = max(1, num)
                save_state()
                reply(
                    update,
                    f"⏱️ 감시 주기를 {num}분으로 설정했어요.",
                    kb=REMINDER_MENU_KB,
                )
            except Exception:
                reply(
                    update,
                    "간격은 숫자(분)으로 입력해 주세요. 예) 60",
                    kb=REMINDER_MENU_KB,
                )
            return

        # 5) 오늘/7일 업무 요약
        if text in ("📋 오늘 업무", "오늘 미리알림", "미리알림 확인", "미리알림"):
            msg = build_today_reminder_snapshot()
            reply(update, msg, kb=REMINDER_MENU_KB)
            return

        if text in ("📆 7일 업무", "7일 업무"):
            msg = build_upcoming_reminder_snapshot(7)
            reply(update, msg, kb=REMINDER_MENU_KB)
            return

        # 6) 심야 시간 안내
        if text in ("🌙 심야 시간 안내", "심야 시간 안내"):
            reply(
                update,
                "🌙 심야 시간(00:00 ~ 08:00)에는\n"
                "자동 감시가 켜져 있어도 알림이 가지 않아요.",
                kb=REMINDER_MENU_KB,
            )
            return

        # 7) 매출 조회: 오늘 / 어제 / 당월 / 전월
        if text in ("📆 오늘 매출", "오늘 매출"):
            open_unpaid = fetch_open_unpaid()

            msg = build_sales_summary_for_period("today", open_unpaid=open_unpaid)
            reply(update, msg, kb=SALES_MENU_KB)
            return

        if text in ("🕰 어제 매출", "어제 매출"):
            msg = build_sales_summary_for_period("yesterday")
            reply(update, msg, kb=SALES_MENU_KB)
            return

        if text in ("🗓 당월 매출", "당월 매출"):
            msg = build_sales_summary_for_period("this_month")
            reply(update, msg, kb=SALES_MENU_KB)
            return

        if text in ("📜 전월 매출", "전월 매출"):
            msg = build_sales_summary_for_period("prev_month")
            reply(update, msg, kb=SALES_MENU_KB)
            return

        # 8) 뒤로 가기 → 기본 개인비서 키보드 복귀
        if text in ("◀️ 뒤로 가기", "뒤로 가기"):
            reply(update, "개인비서 기본 메뉴로 돌아왔어요.", kb=MAIN_KB(cid))
            return

    # ===== 코인 플로우 =====
    if action == "coin" and step == "mode":
        if text not in ["추가", "삭제"]:
            reply(update, "‘추가/삭제’ 중 선택하세요.", kb=COIN_MODE_KB)
        else:
            next_action = "coin_add" if text == "추가" else "coin_del"
            set_pending(cid, next_action, "symbol", {})
            reply(
                update,
                f"{text}할 코인을 선택하거나 직접 입력하세요.",
                kb=coin_kb(),
            )
        return

    if action in ["coin_add", "coin_del"] and step == "symbol":
        symbol = text.upper()
        if action == "coin_add":
            act_add(update, symbol)
        else:
            act_del(update, symbol)
        clear_pending(cid)
        return

    if step == "symbol" and action in ["price", "setavg", "setqty", "setrate_coin"]:
        symbol = text.upper()
        data["symbol"] = symbol
        if action == "price":
            act_price(update, symbol)
            clear_pending(cid)
            return

        else:
            set_pending(cid, action, "value", data)
            label = {
                "setavg": "평단가(원)",
                "setqty": "수량",
                "setrate_coin": "임계값(%)",
            }[action]
            reply(
                update,
                f"{symbol} {label} 값을 숫자로 입력하세요.",
                kb=CANCEL_KB,
            )
            return

    if step == "value" and action in ["setavg", "setqty", "setrate_coin"]:
        v = text.replace(",", "")
        try:
            float(v)
        except ValueError:
            reply(update, "숫자만 입력하세요. 취소는 ‘취소’", kb=CANCEL_KB)
            return
        symbol = data.get("symbol", "")
        if action == "setavg":
            act_setavg(update, symbol, v)
        elif action == "setqty":
            act_setqty(update, symbol, v)
        elif action == "setrate_coin":
            act_setrate_symbol(update, symbol, v)
        clear_pending(cid)
        return

    # ===== 차트 플로우 =====
    if action == "chart" and step == "symbol":
        sym = text.upper()
        clear_pending(cid)

        # 마켓 문자열 변환 (예: BTC → KRW-BTC)
        market = krw_symbol(sym)
        title10 = f"{sym} 10분봉"
        title60 = f"{sym} 60분봉"

        # 수동 차트 요청은 항상 차트 + 텍스트 전송
        send_chart_with_text(
            context,
            market,
            title10,
            title60,
            f"🖼️ {sym} 차트입니다.",
        )
        return


    # ===== 지정가(트리거) 플로우 =====
    if action == "trigger":
        # 1) 코인 심볼 입력
        if step == "symbol":
            data["symbol"] = text.upper()
            set_pending(cid, "trigger", "menu", data)
            reply(
                update,
                "동작을 선택하세요.",
                kb=ReplyKeyboardMarkup(
                    [["추가", "삭제"], ["목록", "초기화"], ["취소"]],
                    resize_keyboard=True,
                    one_time_keyboard=True,
                ),
            )
            return

        sym = data.get("symbol", "").upper()

        # 2) 메뉴 선택
        if step == "menu":
            if text not in ["추가", "삭제", "목록", "초기화", "취소"]:
                reply(
                    update,
                    "‘추가/삭제/목록/초기화/취소’ 중 선택하세요.",
                    kb=ReplyKeyboardMarkup(
                        [["추가", "삭제"], ["목록", "초기화"], ["취소"]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    ),
                )
                return

            if text == "목록":
                m = krw_symbol(sym)
                c = ensure_coin(m)
                reply(
                    update,
                    _trigger_list_text(c),
                    kb=ReplyKeyboardMarkup(
                        [["추가", "삭제"], ["목록", "초기화"], ["취소"]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    ),
                )
                return

            if text == "초기화":
                n = trigger_clear(sym)
                reply(
                    update,
                    f"트리거 {n}개 삭제됨.",
                    kb=ReplyKeyboardMarkup(
                        [["추가", "삭제"], ["목록", "초기화"], ["취소"]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    ),
                )
                return

            if text == "삭제":
                m = krw_symbol(sym)
                c = ensure_coin(m)
                if not c.get("triggers"):
                    reply(
                        update,
                        "등록된 트리거가 없습니다.",
                        kb=ReplyKeyboardMarkup(
                            [["추가", "삭제"], ["목록", "초기화"], ["취소"]],
                            resize_keyboard=True,
                            one_time_keyboard=True,
                        ),
                    )
                    return
                set_pending(cid, "trigger", "delete_select", data)
                reply(
                    update,
                    _trigger_list_text(c) + "\n삭제할 번호를 입력(예: 1 또는 1,3)",
                    kb=CANCEL_KB,
                )
                return

            if text == "추가":
                set_pending(cid, "trigger", "add_mode", data)
                reply(
                    update,
                    "입력 방식을 선택하세요.",
                    kb=ReplyKeyboardMarkup(
                        [["직접가격", "현재가±%", "평단가±%"], ["취소"]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    ),
                )
                return

            if text == "취소":
                clear_pending(cid)
                reply(update, "취소되었습니다.")
                return

        # 3) 삭제 번호 선택
        if step == "delete_select":
            nums = []
            for part in text.replace(" ", "").split(","):
                if part.isdigit():
                    nums.append(int(part))
            if not nums:
                reply(
                    update,
                    "번호를 올바르게 입력하세요. 예: 1 또는 1,3",
                    kb=CANCEL_KB,
                )
                return
            cnt = trigger_delete(sym, set(nums))
            clear_pending(cid)
            reply(update, f"{cnt}개 삭제 완료.")
            return

        # 4) 추가 모드 선택
        if step == "add_mode":
            if text not in ["직접가격", "현재가±%", "평단가±%"]:
                reply(
                    update,
                    "‘직접가격/현재가±%/평단가±%’ 중 선택하세요.",
                    kb=ReplyKeyboardMarkup(
                        [["직접가격", "현재가±%", "평단가±%"], ["취소"]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    ),
                )
                return
            data["mode"] = (
                "direct"
                if text == "직접가격"
                else "cur_pct"
                if text == "현재가±%"
                else "avg_pct"
            )
            set_pending(cid, "trigger", "add_value", data)
            msg = (
                "가격(원)을 입력하세요."
                if data["mode"] == "direct"
                else "변화율(%)을 입력하세요. 예: 5 또는 -5"
            )
            reply(update, msg, kb=CANCEL_KB)
            return

        # 5) 추가 값 입력
        if step == "add_value":
            v = text.replace("%", "").replace(",", "")
            try:
                float(v)
            except ValueError:
                reply(update, "숫자만 입력하세요.", kb=CANCEL_KB)
                return
            try:
                trg = trigger_add(sym, data["mode"], float(v))
            except ValueError as e:
                reply(update, f"오류: {e}", kb=CANCEL_KB)
                return
            clear_pending(cid)
            reply(update, f"트리거 등록: {sym} {fmt(trg)}원")
            return

    # ===== 네이버 수동 입찰 =====
    if action == "naver_manual" and step == "value":
        v = text.replace(",", "")
        try:
            bid = int(v)
        except ValueError:
            reply(update, "숫자만 입력하세요. 취소는 ‘취소’", kb=CANCEL_KB)
            return
        success, msg = naver_set_bid(bid)
        clear_pending(cid)
        reply(update, f"✅ {msg}" if success else f"⚠️ {msg}")
        return

    # ===== 네이버 시간표 관리 =====
    if action == "naver_schedule":
        nav = state.setdefault("naver", {})
        schedules = nav.get("schedules") or []
        t = text.strip()

        # 메뉴 단계
        if step == "menu":
            if t == "추가":
                set_pending(cid, "naver_schedule", "add", {})
                reply(
                    update,
                    "추가할 '요일 시간/입찰가'를 입력하세요.\n"
                    "예: 13:00/200\n"
                    "예: 주말 13:00/0\n"
                    "예: 월,화,수 13:00/200",

                    kb=CANCEL_KB,
                )
                return

            if t == "삭제":
                if not schedules:
                    clear_pending(cid)
                    reply(update, "삭제할 시간표가 없습니다.")
                    return
                set_pending(cid, "naver_schedule", "del", {})
                reply(
                    update,
                    "삭제할 '요일 시간'을 입력하세요.\n"
                    "예: 11:00 (그 시간 전체 삭제)\n"
                    "예: 주말 11:00 (주말만 삭제)\n"
                    "예: 월,화 11:00",

                    kb=CANCEL_KB,
                )
                return

            if t in ["전체초기화", "전체 초기화"]:
                if not schedules:
                    clear_pending(cid)
                    reply(update, "이미 시간표가 비어 있습니다.")
                    return
                set_pending(cid, "naver_schedule", "clear_confirm", {})
                reply(
                    update,
                    "⚠️ 모든 광고 시간표를 삭제할까요? (예/아니오)",
                    kb=CANCEL_KB,
                )
                return

            if t in ["취소", "cancel", "CANCEL"]:
                clear_pending(cid)
                reply(update, "시간표 설정을 취소했습니다.")
                return

            # 잘못된 입력 → 메뉴 재표시
            lines = [
                "잘못된 입력입니다. 아래 버튼 중에서 선택하세요.",
                "",
                "현재 시간표:",
            ]
            if schedules:
                for s in schedules:
                    lines.append(
                        f"· {s.get('time')} → {s.get('bid')}원"
                    )
            else:
                lines.append("· 없음")
            reply(
                update,
                "\n".join(lines),
                kb=NAVER_SCHEDULE_MENU_KB,
            )
            return

        # 추가 단계: "요일 HH:MM/가격" (요일 생략 가능)
        if step == "add":
            try:
                days, ts, bid = _parse_schedule_add_input(t)
            except Exception:
                reply(
                    update,
                    "형식이 올바르지 않습니다.\n"
                    "예: 13:00/200\n"
                    "예: 주말 13:00/0\n"
                    "예: 월,화,수 13:00/200",
                    kb=CANCEL_KB,
                )
                return

            # ✅ 같은 'time+days' 조합은 덮어쓰기
            def _same_days(a, b):
                aa = sorted(a or [])
                bb = sorted(b or [])
                return aa == bb

            new = []
            for s in schedules:
                if s.get("time") == ts and _same_days(s.get("days"), days):
                    continue
                new.append(s)

            item = {"time": ts, "bid": bid}
            if days:
                item["days"] = sorted(set(days))
            new.append(item)

            # 보기 좋게 정렬: time → days(문자열) 순
            new.sort(key=lambda x: ((x.get("time") or ""), _days_to_label(x.get("days"))))

            nav["schedules"] = new
            nav.setdefault("auto_enabled", False)
            nav["last_applied"] = ""
            save_state()
            clear_pending(cid)

            lines = ["✅ 시간표 추가/수정 완료.", "", "현재 시간표:"]
            for s in new:
                lines.append(
                    f"· [{_days_to_label(s.get('days'))}] {s.get('time')} → {s.get('bid')}원"
                )
            reply(update, "\n".join(lines))
            return


        # 삭제 단계: "요일 HH:MM" (요일 생략 가능)
        if step == "del":
            try:
                days, ts = _parse_schedule_del_input(t)
            except Exception:
                reply(
                    update,
                    "형식이 올바르지 않습니다.\n"
                    "예: 11:00\n"
                    "예: 주말 11:00\n"
                    "예: 월,화 11:00",
                    kb=CANCEL_KB,
                )
                return

            def _same_days(a, b):
                aa = sorted(a or [])
                bb = sorted(b or [])
                return aa == bb

            before = list(schedules)

            if days is None:
                # 시간만 입력 → 해당 시간 전체 삭제
                after = [s for s in before if s.get("time") != ts]
            else:
                # 요일+시간 입력 → 해당 (time+days)만 삭제
                after = [s for s in before if not (s.get("time") == ts and _same_days(s.get("days"), days))]

            nav["schedules"] = after
            nav["last_applied"] = ""
            save_state()
            clear_pending(cid)

            if len(after) == len(before):
                reply(update, "삭제된 항목이 없습니다. (시간/요일 조합을 확인해 주세요.)")
                return

            lines = ["✅ 시간표 삭제 완료.", "", "현재 시간표:"]
            if after:
                for s in after:
                    lines.append(
                        f"· [{_days_to_label(s.get('days'))}] {s.get('time')} → {s.get('bid')}원"
                    )
            else:
                lines.append("· 없음")
            reply(update, "\n".join(lines))
            return


        # 전체 초기화 확인 단계
        if step == "clear_confirm":
            if t in ["예", "네", "YES", "Yes", "yes", "y", "Y"]:
                nav["schedules"] = []
                nav["last_applied"] = ""
                save_state()
                clear_pending(cid)
                reply(
                    update,
                    "✅ 모든 광고 시간표를 삭제했습니다.",
                )
                return
            else:
                clear_pending(cid)
                reply(update, "시간표 삭제를 취소했습니다.")
                return

    # ===== 네이버 입찰추정 플로우 =====
    if action == "naver_abtest":
        if step == "keyword":
            data["keyword"] = text.strip()
            set_pending(cid, "naver_abtest", "start_bid", data)
            reply(
                update,
                "입찰 추정을 시작할 '시작 입찰가(원)'를 입력하세요.",
                kb=CANCEL_KB,
            )
            return

        if step == "start_bid":
            v = text.replace(",", "")
            try:
                start_bid = int(v)
            except ValueError:
                reply(
                    update,
                    "숫자만 입력하세요. 취소는 ‘취소’",
                    kb=CANCEL_KB,
                )
                return
            data["start_bid"] = start_bid
            set_pending(cid, "naver_abtest", "marker", data)
            reply(
                update,
                "검색 결과에서 내 매장을 식별할 문구를 입력하세요.\n예: '두젠틀 애견카페 강남'",
                kb=CANCEL_KB,
            )
            return

        if step == "marker":
            data["marker"] = text.strip()
            set_pending(cid, "naver_abtest", "interval", data)
            reply(
                update,
                "노출위치 확인 간격(초)을 입력하세요. (권장 60)",
                kb=CANCEL_KB,
            )
            return

        if step == "interval":
            v = text.strip()
            if v:
                try:
                    interval = max(10, int(v))
                except ValueError:
                    interval = 60
            else:
                interval = 60
            data["interval"] = interval
            set_pending(cid, "naver_abtest", "max_bid", data)
            reply(
                update,
                "최대 입찰가(원)를 입력하세요. (이 금액을 넘기면 추정을 중단합니다.)",
                kb=CANCEL_KB,
            )
            return

        if step == "max_bid":
            v = text.replace(",", "")
            try:
                max_bid = int(v)
            except ValueError:
                start_bid = int(data.get("start_bid", 0))
                max_bid = start_bid + 200
            keyword = data.get("keyword", "")
            marker = data.get("marker", "")
            start_bid = int(data.get("start_bid", 0))
            interval = int(data.get("interval", 60))
            step_bid = 10
            clear_pending(cid)
            start_naver_abtest(
                cid,
                keyword,
                marker,
                start_bid,
                max_bid,
                step_bid,
                interval,
            )
            reply(
                update,
                f"입찰추정을 시작합니다.\n"
                f"- 키워드: {keyword}\n"
                f"- 시작 입찰가: {start_bid}원\n"
                f"- 최대 입찰가: {max_bid}원\n"
                f"- 확인 간격: {interval}초\n"
                f"- 상승 단위: {step_bid}원",
            )
            return

    # ===== 네이버 노출감시 설정 플로우 =====
    if action == "naver_rank_watch":
        nav = state.setdefault("naver", {})
        cfg = nav.setdefault("rank_watch", {})
        if step == "keyword":
            cfg["keyword"] = text.strip()
            set_pending(cid, "naver_rank_watch", "marker", {})
            save_state()
            reply(
                update,
                "플레이스 리스트에서 내 매장을 식별할 문구를 입력하세요.\n예: '두젠틀 애견카페 강남'",
                kb=CANCEL_KB,
            )
            return
        if step == "marker":
            cfg["marker"] = text.strip()
            set_pending(cid, "naver_rank_watch", "interval", {})
            save_state()
            reply(
                update,
                "확인 간격(초)을 입력하세요. (권장 300)",
                kb=CANCEL_KB,
            )
            return
        if step == "interval":
            try:
                sec = max(30, int(text.strip()))
            except ValueError:
                sec = 300
            cfg["interval"] = sec
            cfg["enabled"] = True
            cfg["last_rank"] = None
            cfg["last_check"] = 0.0
            save_state()
            clear_pending(cid)
            reply(
                update,
                f"노출감시를 시작합니다. (간격 {sec}초, 광고/기본 순위 모두 확인)",
            )
            return

    # ===== 기본 명령 처리 =====
    head = text.split()[0].lstrip("/")
    body = text[len(head):].strip()


    if head in ["도움말", "help"]:
        reply(update, get_help_by_mode(cid))
        return

    if head == "메뉴":
        update.message.reply_text(
            "모드를 선택하세요.", reply_markup=mode_inline_kb()
        )
        return

    if head in ["보기", "view"]:
        if not state["coins"]:
            reply(
                update,
                "📊 보기 (포트폴리오)\n등록된 코인이 없습니다.",
            )
            return

        items = sorted_coin_items()

        total_buy = 0.0
        total_eval = 0.0

        # 총 투자금액 / 평가금액 계산
        for _, _, m, info, cur in items:
            avg = float(info.get("avg_price", 0.0))
            qty = float(info.get("qty", 0.0))
            total_buy += avg * qty
            total_eval += cur * qty

        pnl_w = total_eval - total_buy
        pnl_p = (
            0.0
            if total_buy == 0
            else (total_eval / total_buy - 1) * 100
        )

        # 손익 이모지 선택
        if pnl_w > 0:
            emo_rate = "📈"
            emo_money = "💖"
        elif pnl_w < 0:
            emo_rate = "📉"
            emo_money = "💔"
        else:
            emo_rate = "⚖️"
            emo_money = "⚪️"

        header = (
            "📊 보기 (포트폴리오)\n\n"
            f"💵 총 투자금액: {fmt(total_buy)} 원\n"
            f"📊 현재 평가금액: {fmt(total_eval)} 원\n"
            f"{emo_rate} 손익: {pnl_p:+.2f}%\n"
            f"{emo_money} {fmt(pnl_w)} 원\n\n"
        )

        blocks = []
        for _, _, m, info, cur in items:
            blocks.append(view_block(m, info, cur))

        reply(update, (header + "---\n".join(blocks))[:4000])
        return

    if head in ["상태", "status"]:
        g = norm_threshold(
            state.get("default_threshold_pct", DEFAULT_THRESHOLD)
        )
        header = (
            f"⚙️ 상태 요약\n"
            f"- 기본 임계값: {g}%\n"
            f"- 등록 코인: {len(state['coins'])}개\n\n"
        )

        if not state["coins"]:
            reply(update, header + "- 코인 없음")
        else:
            rows = []
            for _, _, m, info, cur in sorted_coin_items():
                rows.append(status_line(m, info, cur))
            reply(update, (header + "\n".join(rows))[:4000])
        return

    # 네이버 광고 명령
    if head == "광고상태":
        send_naver_status(update)
        return

    if head == "광고설정":
        parts = text.split()
        if len(parts) >= 2:
            v = parts[1].replace(",", "")
            try:
                bid = int(v)
                success, msg = naver_set_bid(bid)
                reply(
                    update,
                    f"✅ {msg}" if success else f"⚠️ {msg}",
                )
                return
            except ValueError:
                pass
        set_pending(cid, "naver_manual", "value", {})
        reply(
            update,
            "변경할 입찰가(원)를 숫자로 입력하세요.",
            kb=CANCEL_KB,
        )
        return

    if head == "광고시간":
        nav = state.setdefault("naver", {})
        schedules = nav.get("schedules") or []

        lines = ["📅 네이버 광고 시간표 관리", ""]
        if schedules:
            lines.append("현재 시간표:")
            for s in schedules:
                lines.append(
                    f"· [{_days_to_label(s.get('days'))}] {s.get('time')} → {s.get('bid')}원"
                )
        else:
            lines.append("현재 시간표: 없음")
        lines.append("")
        lines.append("원하는 작업을 선택하세요.")

        reply(
            update,
            "\n".join(lines),
            kb=NAVER_SCHEDULE_MENU_KB,
        )
        set_pending(cid, "naver_schedule", "menu", {})
        return

    if head == "광고자동":
        nav = state.setdefault("naver", {})
        nav["auto_enabled"] = not bool(nav.get("auto_enabled"))
        save_state()
        status = "켜짐" if nav["auto_enabled"] else "꺼짐"
        reply(
            update,
            f"네이버 광고 자동 변경이 '{status}' 상태입니다.",
        )
        return

    if head in ["입찰추정", "자동입찰"]:
        set_pending(cid, "naver_abtest", "keyword", {})
        reply(
            update,
            "입찰 추정을 위한 검색어를 입력하세요.",
            kb=CANCEL_KB,
        )
        return

    if head == "노출감시":
        nav = state.setdefault("naver", {})
        cfg = nav.setdefault("rank_watch", {})
        if cfg.get("enabled"):
            cfg["enabled"] = False
            save_state()
            reply(update, "노출감시를 중지했습니다.")
        else:
            set_pending(cid, "naver_rank_watch", "keyword", {})
            reply(
                update,
                "노출감시용 키워드를 입력하세요. (예: 강남 애견카페)",
                kb=CANCEL_KB,
            )
        return

    if head in ["노출현황", "노출조회", "노출상태"]:
        naver_rank_check_once(update)
        return

    # 리뷰감시 중지
    if head in ["리뷰감시중지", "리뷰중지", "리뷰감시끄기"]:
        nav = state.setdefault("naver", {})
        cfg = nav.setdefault("review_watch", {})
        cfg["enabled"] = False
        save_state()
        reply(update, "리뷰감시를 중지했습니다.")
        return

    # 리뷰감시 시작: 리뷰감시 [분]
    if head.startswith("리뷰감시"):
        nav = state.setdefault("naver", {})
        cfg = nav.setdefault("review_watch", {})
        parts = text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            minutes = int(parts[1])
            sec = max(60, minutes * 60)
            cfg["interval"] = sec
        if not NAVER_PLACE_ID:
            reply(
                update,
                "NAVER_PLACE_ID가 설정되어 있지 않습니다. .env에 플레이스 ID를 입력하세요.",
            )
            return
        cfg["enabled"] = True
        cfg["last_check"] = 0.0
        save_state()
        iv = int(cfg.get("interval", 180))
        reply(
            update,
            f"리뷰감시를 시작합니다. {iv//60}분 간격으로 확인합니다.",
        )
        return

    if head in ["리뷰현황", "리뷰조회", "리뷰상태"]:
        naver_review_check_once(update)
        return

    # 코인 기본 명령
    if head == "코인":
        set_pending(cid, "coin", "mode", {})
        reply(
            update,
            "코인 관리 방식을 선택하세요.",
            kb=COIN_MODE_KB,
        )
        return

    if head == "가격":
        set_pending(cid, "price", "symbol", {})
        reply(
            update,
            "조회할 코인을 선택하거나 직접 입력하세요.",
            kb=coin_kb(),
        )
        return

    if head == "평단":
        set_pending(cid, "setavg", "symbol", {})
        reply(
            update,
            "코인을 선택하거나 직접 입력하세요.",
            kb=coin_kb(),
        )
        return

    if head == "수량":
        set_pending(cid, "setqty", "symbol", {})
        reply(
            update,
            "코인을 선택하거나 직접 입력하세요.",
            kb=coin_kb(),
        )
        return

    if head == "임계값":
        parts = text.split()
        if len(parts) == 2:
            v = parts[1].replace(",", "")
            try:
                act_setrate_default(update, float(v))
                return
            except Exception:
                pass
        set_pending(cid, "setrate_coin", "symbol", {})
        reply(
            update,
            "개별 임계값 설정할 코인을 선택하거나 직접 입력하세요.",
            kb=coin_kb(),
        )
        return

    if head == "지정가":
        set_pending(cid, "trigger", "symbol", {})
        reply(
            update,
            "코인을 선택하거나 직접 입력하세요.",
            kb=coin_kb(),
        )
        return

    # 🔹 차트 자동 첨부 ON/OFF 토글
    if head == "차트알림":
        current = bool(state.get("chart_auto", True))
        state["chart_auto"] = not current
        save_state()
        if state["chart_auto"]:
            reply(update, "🖼️ 차트 알림을 켰어요. (자동 알림에 차트 사진이 함께 옵니다.)")
        else:
            reply(update, "🖼️ 차트 알림을 껐어요. (자동 알림은 텍스트만 옵니다.)")
        return

    # 🔹 수동 차트 요청 (차트)
    if head == "차트":
        # "차트 BTC" 처럼 심볼까지 함께 온 경우
        if body.strip():
            sym = body.strip().upper()
            market = krw_symbol(sym)
            title10 = f"{sym} 10분봉"
            title60 = f"{sym} 60분봉"
            send_chart_with_text(context, market, title10, title60, f"🖼️ {sym} 차트입니다.")
            return

        # 심볼이 없으면, 코인 선택/입력 플로우로
        set_pending(cid, "chart", "symbol", {})
        reply(
            update,
            "차트를 볼 코인을 선택하거나 직접 입력하세요.",
            kb=coin_kb(),
        )
        return

    # 어떤 것도 매칭 안 되면 현재 모드 도움말
    reply(update, get_help_by_mode(cid))

# ========= COIN ALERT LOOP =========
def check_loop(context):
    if not state["coins"]:
        return
    for m, info in list(state["coins"].items()):
        try:
            cur = get_price(m)
        except:
            continue

        if info.get("last_notified_price") is None:
            info["last_notified_price"] = cur

        base = info.get("last_notified_price", cur)
        th   = norm_threshold(info.get("threshold_pct", None))

        try:
            delta = abs(cur/base - 1) * 100
        except:
            delta = 0

        if base > 0 and delta >= th:
            up = cur > base
            arrow = "🔴" if up else "🔵"
            sym = m.split("-")[1]
            avg = float(info.get("avg_price", 0.0))
            qty = float(info.get("qty", 0.0))
            pnl_w = (cur - avg) * qty
            pnl_p = 0.0 if avg == 0 else (cur / avg - 1) * 100
            change_pct = (cur / base - 1) * 100

            # 이번 알림 기준 시각
            now_ts = time.time()
            # 직전 알림 기준으로 몇 분 지났는지
            recent_line = recent_alert_line(info.get("last_alert_ts"), now_ts)

            msg = (
                f"{arrow} {sym} {arrow}\n"
                f"📍 가격: {fmt(base)} → {fmt(cur)} 원 ({change_pct:+.2f}%)\n"
                f"💰 평가손익: {pnl_p:+.2f}%\n"
                f"📊 평가금액: {fmt(pnl_w)} 원\n"
                f"{recent_line}"
                f"⚙️ 임계 변동: {th}%"
            )

            title10 = f"{sym} 10분봉"
            title60 = f"{sym} 60분봉"
            send_alert_with_optional_chart(context, m, title10, title60, msg)


            # 다음 임계 계산 기준 가격 & 마지막 알림 시각 갱신
            info["last_notified_price"] = cur
            info["last_alert_ts"] = now_ts




        prev = info.get("prev_price")
        if prev is None:
            info["prev_price"] = cur
            continue

        trigs = list(info.get("triggers", []))
        fired = []
        for t in trigs:
            try:
                t = float(t)
                up_cross   = (prev < t <= cur)
                down_cross = (prev > t >= cur)
                if up_cross or down_cross:
                    sym = m.split("-")[1]
                    direction = "🔴 상향" if up_cross else "🔵 하향"
                    now_ts = time.time()
                    recent_line = recent_alert_line(info.get("last_alert_ts"), now_ts)

                    msg = (
                        f"🎯 트리거 도달\n"
                        f"{direction} {sym}: 현재 {fmt(cur)}원 | 트리거 {fmt(t)}원\n"
                        f"{recent_line}"
                    )

                    title10 = f"{sym} 10분봉"
                    title60 = f"{sym} 60분봉"
                    send_alert_with_optional_chart(context, m, title10, title60, msg)


                    # 마지막 알림 시각 갱신 (트리거도 알림으로 취급)
                    info["last_alert_ts"] = now_ts

                    fired.append(t)


            except:
                pass

        if fired:
            info["triggers"] = [x for x in info.get("triggers", []) if x not in fired]

        info["prev_price"] = cur

    save_state()



# ========= MAIN =========
def main():
    _start_keepalive()

    if not BOT_TOKEN:
        print("BOT_TOKEN 누락")
        return

    up = Updater(BOT_TOKEN, use_context=True)

    try:
        up.bot.delete_webhook(drop_pending_updates=True)
    except:
        pass

    dp = up.dispatcher
    dp.add_handler(CallbackQueryHandler(on_mode_select, pattern=r"^mode_"))
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), on_text))
    dp.add_handler(MessageHandler(Filters.command, on_text))

    # Job queues
    up.job_queue.run_repeating(check_loop, interval=8, first=5)
    up.job_queue.run_repeating(naver_schedule_loop, interval=30, first=10)
    up.job_queue.run_repeating(naver_abtest_loop, interval=15, first=15)
    up.job_queue.run_repeating(naver_rank_watch_loop, interval=60, first=20)
    up.job_queue.run_repeating(naver_review_watch_loop, interval=60, first=40)
    up.job_queue.run_repeating(gcal_watch_loop, interval=60, first=30)
    up.job_queue.run_repeating(reminder_watch_loop, interval=60, first=35)  # ⬅️ 추가

    # 📊 킴스 통계: 클릭 급증 감시(기본 OFF, 켜면 동작)
    up.job_queue.run_repeating(_kims_spike_watch_loop, interval=30, first=20)

    # 📡 두젠틀보드 대시보드 push (60초마다 코인 스냅샷)
    up.job_queue.run_repeating(_dashboard_crypto_push_job, interval=60, first=15)

    # 🌅 킴스 통계: 매일 아침 브리핑(어제 + 전날 대비 UV)
    ks = state.setdefault("kims_stats", {})
    dbf = ks.setdefault("daily_brief", {})
    if dbf.get("enabled", True):
        hh = int(dbf.get("hour", 9) or 9)
        mm = int(dbf.get("minute", 0) or 0)
        up.job_queue.run_daily(
            _kims_daily_brief_job,
            time=dtime(hh, mm, tzinfo=SEOUL_TZ),
        )

    def hi(ctx):
        try:
            if CHAT_ID:
                send_ctx(
                    ctx,
                    "김비서 출근했어요 💖"
                )
        except:
            pass

    up.job_queue.run_once(lambda c: hi(c), when=2)

    print("////////////////////////////////////////")
    print(">>> Upbit + Naver Ads + Place Watch Bot is running")
    print("////////////////////////////////////////")

    up.start_polling(drop_pending_updates=True)
    up.idle()

if __name__ == "__main__":
    try:
        main()
    finally:
        _release_lock()
    # threading.Thread(target=start_mac_push_server, daemon=True).start()
