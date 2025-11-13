# app.py
import os, json, requests, atexit, signal, threading, random, re, time, base64, hmac, hashlib, urllib.parse
from datetime import datetime, timezone, timedelta
KST = timezone(timedelta(hours=9))
<<<<<<< HEAD

# === Google Calendar ===
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz

=======
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
from http.server import BaseHTTPRequestHandler, HTTPServer
from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Updater, MessageHandler, Filters, CallbackQueryHandler

import io
import matplotlib
matplotlib.use("Agg")  # ✅ GUI 안 쓰고 이미지용 백엔드 사용

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import platform
from matplotlib import font_manager


# (선택) 한글 폰트 설정 - OS에 따라 자동 선택
def set_korean_font():
    try:
        system = platform.system()
        # OS별 후보 폰트 목록
        if system == "Windows":
            candidates = ["Malgun Gothic", "맑은 고딕", "Gulim", "굴림"]
        elif system == "Darwin":  # macOS
            candidates = ["AppleGothic", "NanumGothic", "NanumSquare", "맑은 고딕"]
        else:  # Linux 등 기타
            candidates = ["NanumGothic", "NanumSquare", "DejaVu Sans"]

        available = {f.name for f in font_manager.fontManager.ttflist}

        # 설치되어 있는 폰트 중 첫 번째를 사용
        for name in candidates:
            if name in available:
                plt.rcParams["font.family"] = name
                break

        # 마이너스 깨짐 방지
        plt.rcParams["axes.unicode_minus"] = False

    except Exception:
        # 문제 생겨도 최소한 마이너스 깨짐만 방지
        plt.rcParams["axes.unicode_minus"] = False

set_korean_font()




# ========= ENV =========
load_dotenv()

BOT_TOKEN   = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID     = str(os.getenv("CHAT_ID", "")).strip()
DEFAULT_THRESHOLD = float(os.getenv("THRESHOLD_PCT", "1.0"))
PORT        = int(os.getenv("PORT", "0"))

# Persistent state dir (Render: DATA_DIR=/data)
DATA_DIR    = os.getenv("DATA_DIR", "").strip() or "."
os.makedirs(DATA_DIR, exist_ok=True)

# Naver Searchad API
NAVER_BASE_URL      = "https://api.naver.com"
NAVER_API_KEY       = os.getenv("NAVER_API_KEY", "").strip()
NAVER_API_SECRET    = os.getenv("NAVER_API_SECRET", "").strip()
NAVER_CUSTOMER_ID   = os.getenv("NAVER_CUSTOMER_ID", "").strip()
NAVER_CAMPAIGN_ID   = os.getenv("NAVER_CAMPAIGN_ID", "").strip()
NAVER_ADGROUP_ID    = os.getenv("NAVER_ADGROUP_ID", "").strip()
NAVER_ADGROUP_NAME  = os.getenv("NAVER_ADGROUP_NAME", "").strip()

# Naver Place (리뷰/노출 감시용)
NAVER_PLACE_ID      = os.getenv("NAVER_PLACE_ID", "").strip()

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
            self.send_header("Content-Type","text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")
        except:
            pass

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

        "modes": {},
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

    d.setdefault("modes", {})

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

def save_state():
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DATA_FILE)

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
                ["📅 캘린더 알림", "도움말", "메뉴"],
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
                ["메뉴"],
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

    plt.tight_layout()

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
<<<<<<< HEAD

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

=======
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b

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

<<<<<<< HEAD
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


=======
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
<<<<<<< HEAD
    "각 모드를 누르면 아래 키보드가 바뀌고,\n"
    "그 모드 전용 명령만 사용할 수 있습니다.\n"
=======
    "📢 네이버 광고 기능\n"
    "• 광고상태 : 현재 설정/감시 요약\n"
    "• 광고설정 X : 입찰가를 X원으로 즉시 변경\n"
    "• 광고시간 : 'HH:MM/입찰가' 형식 시간표 설정\n"
    "• 광고자동 : 시간표 자동 적용 켜기/끄기\n"
    "• 입찰추정 : 1순위 추정 입찰가 자동 탐색\n"
    "• 노출감시 : 플레이스 순위 변동 실시간 감시 (광고/기본 순위 함께 표시)\n"
    "• 노출현황 : 현재 플레이스 순위를 즉시 1회 조회 (광고/기본 순위 함께 표시)\n"
    "• 리뷰감시 : NAVER_PLACE_ID 기준 신규 리뷰 감시\n"
    "• 리뷰현황 : 현재 리뷰 개수를 즉시 1회 조회\n"
    "\n"
    "🏨 호텔 : 랜덤 후기 3줄 생성\n"
    "🔧 메뉴 : '네이버 광고 / 코인 가격알림' 모드 전환"
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
)

HELP_PA = (
    "🤖 [개인비서 모드 안내]\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "이 모드는 일상 질문, 일정, 메모 등을 도와주는 공간입니다.\n"
    "지금은 준비 단계이며 아래 기능만 사용할 수 있어요.\n"
    "\n"
    "📝 도움말 : 개인비서 사용법을 다시 보여줍니다.\n"
    "🏠 메뉴   : 모드 선택 화면으로 돌아갑니다.\n"
    "\n"
    "추후에\n"
    "• 일정 확인\n"
    "• 메모 기록\n"
    "• 자주 쓰는 답변 자동화\n"
    "등이 추가될 예정입니다."
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
    "\n"
    "📌 평단 : 해당 코인의 평단가를 등록/수정합니다.\n"
    "📦 수량 : 보유 수량을 등록/수정합니다.\n"
    "📍 지정가 : 특정 가격(트리거)을 등록해서, 도달 시 알림을 받습니다.\n"
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
<<<<<<< HEAD

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


=======
    nn = _normalize(name)
    mm = _normalize(marker)
    if mm and mm in nn:
        return True
    tokens = [t for t in re.split(r"\s+", marker.strip()) if t]
    if tokens and all(t in name for t in tokens):
        return True
    return False

>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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

<<<<<<< HEAD
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


=======
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
<<<<<<< HEAD

                name, bid = _get_name_id(apollo, ref)
                if not name:
                    continue

                idx += 1
                # 우리 매장(PLACE ID 또는 marker 기준)에만 ad_rank 부여
                if ad_rank is None and _is_target_place(bid, name, marker):
=======
                name, _ = _get_name_id(apollo, ref)
                if not name:
                    continue
                idx += 1
                if ad_rank is None and _match_name(name, marker):
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
<<<<<<< HEAD

                name, bid = _get_name_id(apollo, ref)
                if not name:
                    continue

                idx += 1
                # 우리 매장(PLACE ID 또는 marker 기준)에만 organic 순위 부여
                if org_rank is None and _is_target_place(bid, name, marker):
                    org_rank = idx


=======
                name, _ = _get_name_id(apollo, ref)
                if not name:
                    continue
                idx += 1
                if org_rank is None and _match_name(name, marker):
                    org_rank = idx

>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
            lines.append(f"· {t} → {bid}원")
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
<<<<<<< HEAD
        kw = rw.get("keyword") or "미설정"
        iv_sec = float(rw.get("interval", 300))
        iv_min = iv_sec / 60.0
        if abs(iv_min - round(iv_min)) < 1e-6:
            iv_str = f"{int(round(iv_min))}분"
        else:
            iv_str = f"{iv_min:.1f}분"
=======
        lines.append(
            f"- 노출감시: ON (키워드 '{rw.get('keyword','')}', "
            f"간격 {rw.get('interval',300)}초, 최근 기본 순위 {_fmt_rank(rw.get('last_rank'))})"
        )
    else:
        lines.append("- 노출감시: OFF")
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b

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

    for s in schedules:
        t = s.get("time")
        bid = s.get("bid")
        if not t:
            continue
        if current_hm == t:
            key = f"{today} {t} {bid}"
            if nav.get("last_applied") == key:
                continue
            success, msg = naver_set_bid(int(bid))
            nav["last_applied"] = key
            save_state()
            try:
                if success:
                    send_ctx(context, f"✅ [네이버 광고 자동 변경]\n{msg}")
                else:
                    send_ctx(context, f"⚠️ [네이버 광고 자동 변경 실패]\n{msg}")
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
        if prev_org is None:
            try:
                send_ctx(
                    context,
                    f"📡 [노출감시 시작]\n"
                    f"키워드 '{keyword}'\n"
                    f"광고 : {_fmt_rank(ad_rank)}\n"
                    f"기본 : {_fmt_rank(org_rank)} (광고 제외)"
                )
            except:
                pass
        elif org_rank != prev_org:
            try:
                send_ctx(
                    context,
                    f"📡 [노출감시] 순위 변경\n"
                    f"키워드 '{keyword}'\n"
                    f"이전 기본 : {_fmt_rank(prev_org)} → 현재 기본 : {_fmt_rank(org_rank)}\n"
                    f"광고 : {_fmt_rank(ad_rank)}"
                )
            except:
                pass
        cfg["last_rank"] = org_rank

    save_state()

# ========= NAVER 리뷰감시 =========
def _parse_review_count_from_html(html: str):
    """
<<<<<<< HEAD
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





=======
    네이버 플레이스 최신 구조 기준 리뷰 수 파싱.
    1순위: __APOLLO_STATE__ 내 VisitorReviewStatsResult / PlaceDetailBase에서 추출
    2순위: 예전 JSON/텍스트 패턴 정규식 (하위 호환)
    """

    # 1) __APOLLO_STATE__ 기반 파싱 (최신 구조)
    apollo = _extract_apollo_state(html)
    if apollo:
        candidates = []

        for v in apollo.values():
            if not isinstance(v, dict):
                continue
            typ = v.get("__typename")

            # VisitorReviewStatsResult 노드
            if typ == "VisitorReviewStatsResult":
                review = v.get("review") or {}
                if isinstance(review, dict):
                    c = review.get("totalCount") or review.get("allCount")
                    if isinstance(c, (int, float)):
                        candidates.append(int(c))

                for field in ["visitorReviewsTotal", "ratingReviewsTotal"]:
                    c = v.get(field)
                    if isinstance(c, (int, float)):
                        candidates.append(int(c))

            # PlaceDetailBase 노드
            if typ == "PlaceDetailBase":
                for field in [
                    "visitorReviewsTotal",
                    "visitorReviewsTextReviewTotal",
                    "reviewCount",
                    "totalReviewCount",
                ]:
                    c = v.get(field)
                    if isinstance(c, (int, float)):
                        candidates.append(int(c))

        # 후보 값들 중 최대값을 리뷰 총합으로 사용
        if candidates:
            return max(candidates)

    # 2) 예전/예비 패턴 (하위 호환용)
    mv = re.search(r'"visitorReviewCount"\s*:\s*(\d+)', html)
    mb = re.search(r'"blogReviewCount"\s*:\s*(\d+)', html)
    if mv or mb:
        v = int(mv.group(1)) if mv else 0
        b = int(mb.group(1)) if mb else 0
        if v or b:
            return v + b

    mv = re.search(r"방문자\s*리뷰\s*([0-9,]+)", html)
    mb = re.search(r"블로그\s*리뷰\s*([0-9,]+)", html)
    if mv or mb:
        v = int(mv.group(1).replace(",", "")) if mv else 0
        b = int(mb.group(1).replace(",", "")) if mb else 0
        if v or b:
            return v + b

    mt = re.search(r'"totalReviewCount"\s*:\s*(\d+)', html)
    if mt:
        return int(mt.group(1))

    # "리뷰 123건" 같은 일반 패턴 (최후 보정)
    ml = re.search(r"리뷰\s*([0-9,]+)\s*건", html)
    if ml:
        return int(ml.group(1).replace(",", ""))

    return None

def get_place_review_count():
    if not NAVER_PLACE_ID:
        return None

    urls = [
        f"https://m.place.naver.com/place/{NAVER_PLACE_ID}",
        f"https://map.naver.com/p/entry/place/{NAVER_PLACE_ID}",
        f"https://pcmap.place.naver.com/restaurant/{NAVER_PLACE_ID}/home",
    ]

    for url in urls:
        try:
            r = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        except Exception as e:
            print(f"[NAVER] 리뷰 URL 요청 실패: {url} :: {e}")
            continue

        try:
            cnt = _parse_review_count_from_html(r.text)
            if cnt is not None:
                return cnt
        except Exception as e:
            print(f"[NAVER] 리뷰 파싱 실패: {url} :: {e}")

    return None
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b


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

<<<<<<< HEAD
    if not stats:
=======
    if cnt is None:
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
        print("[NAVER] 리뷰감시: 리뷰 수 파싱 실패")
        save_state()
        return

    v = int(stats.get("visit", 0))
    b = int(stats.get("blog", 0))
    t = int(stats.get("total", 0)) or (v + b)

    # 이전 값 (하위호환: 예전에는 숫자만 저장했으므로 처리)
    last = cfg.get("last_count")
    if not isinstance(last, dict):
        cfg["last_count"] = {"visit": v, "blog": b, "total": t}
        save_state()
        try:
            send_ctx(
                context,
<<<<<<< HEAD
                "⭐️ [리뷰감시 시작]\n"
                f"🧍 방문자 리뷰: {v}\n"
                f"📝 블로그 리뷰: {b}\n"
                f"💯 총합: {t}"
=======
                f"⭐️ [리뷰감시 시작]\n현재 리뷰 {cnt}건 기준으로 감시합니다."
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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

        msg = "🆕 [리뷰감시] 신규 리뷰 감지!\n"
        if dv > 0:
            msg += f"🧍 방문자 리뷰 +{dv} → {v}\n"
        if db > 0:
            msg += f"📝 블로그 리뷰 +{db} → {b}\n"
        msg += f"💯 총합: {t}"

        try:
<<<<<<< HEAD
            send_ctx(context, msg)
=======
            send_ctx(
                context,
                f"⭐️ [리뷰감시]\n신규 리뷰 {diff}건 추가 (총 {cnt}건)"
            )
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
        except:
            pass
    else:
        save_state()

<<<<<<< HEAD
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



=======
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
def naver_review_check_once(update):
    if not NAVER_PLACE_ID:
        reply(update, "NAVER_PLACE_ID가 설정되어 있지 않습니다. .env에 플레이스 ID를 입력하세요.")
        return

<<<<<<< HEAD
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

=======
    cnt = get_place_review_count()
    if cnt is None:
        reply(update, "리뷰현황 조회 중 오류가 발생했습니다.")
        return

    nav = state.setdefault("naver", {})
    cfg = nav.setdefault("review_watch", {})
    cfg["last_count"] = cnt
    save_state()
    reply(update, f"리뷰현황: 현재 네이버 플레이스 리뷰는 총 {cnt}건입니다.")
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b

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

    if not res:
        reply(
            update,
            "📡 노출현황 알림\n"
            f"🔍 키워드: '{keyword}'\n"
            "⚠️ 검색 결과에서 지정한 매장을 찾지 못했습니다.\n"
            "설정하신 키워드/문구를 다시 한 번 확인해 주세요."
        )
    else:
        ad_rank = res.get("ad")
        org_rank = res.get("organic")
        if org_rank is not None:
            cfg["last_rank"] = org_rank
            save_state()
        reply(
            update,
           "📡 노출현황 알림\n"
        f"🔍 키워드: '{keyword}'\n"
        f"💚 광고 노출: {_fmt_rank(ad_rank)}\n"
        f"📍 기본 노출: {_fmt_rank(org_rank)} (광고 제외)"
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

    # ===== 개인비서 모드: 구글 캘린더 알림 토글 =====
    mode = get_mode(cid)
    if mode in ("pa", "mode_pa"):
        # 토글 버튼
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
<<<<<<< HEAD
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
=======

        if step == "symbol" and action in ["price","setavg","setqty","setrate_coin"]:
            symbol = text.upper()
            data["symbol"] = symbol
            if action == "price":
                act_price(update, symbol)
                clear_pending(cid)
            else:
                set_pending(cid, action, "value", data)
                label = {
                    "setavg":"평단가(원)",
                    "setqty":"수량",
                    "setrate_coin":"임계값(%)"
                }[action]
            reply(update, f"{symbol} {label} 값을 숫자로 입력하세요.", kb=CANCEL_KB)
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
                    "추가할 시간/입찰가를 입력하세요.\n예: 13:00/200",
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
                    "삭제할 시간을 입력하세요. 예: 11:00",
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

        # 추가 단계: "HH:MM/가격"
        if step == "add":
            raw = t.replace(" ", "")
            try:
                ts, vs = raw.split("/", 1)
                datetime.strptime(ts, "%H:%M")
                bid = int(vs.replace(",", ""))
            except Exception:
                reply(
                    update,
                    "형식이 올바르지 않습니다. 예: 13:00/200",
                    kb=CANCEL_KB,
                )
                return

            new = [s for s in schedules if s.get("time") != ts]
            new.append({"time": ts, "bid": bid})
            new.sort(key=lambda x: x.get("time") or "")

            nav["schedules"] = new
            nav.setdefault("auto_enabled", False)
            nav["last_applied"] = ""
            save_state()
            clear_pending(cid)

            lines = ["✅ 시간표 추가/수정 완료.", "", "현재 시간표:"]
            for s in new:
                lines.append(
                    f"· {s.get('time')} → {s.get('bid')}원"
                )
            reply(update, "\n".join(lines))
            return

        # 삭제 단계: "HH:MM"
        if step == "del":
            try:
                datetime.strptime(t, "%H:%M")
            except Exception:
                reply(
                    update,
                    "형식이 올바르지 않습니다. 예: 11:00",
                    kb=CANCEL_KB,
                )
                return

            before = len(schedules)
            new = [s for s in schedules if s.get("time") != t]

            nav["schedules"] = new
            nav["last_applied"] = ""
            save_state()
            clear_pending(cid)

            if len(new) < before:
                lines = [f"✅ {t} 시간표를 삭제했습니다.", ""]
            else:
                lines = [f"⚠️ {t} 시간표를 찾지 못했습니다.", ""]
            lines.append("현재 시간표:")
            if new:
                for s in new:
                    lines.append(
                        f"· {s.get('time')} → {s.get('bid')}원"
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
<<<<<<< HEAD
                reply(
                    update,
                    "✅ 모든 광고 시간표를 삭제했습니다.",
                )
=======
                reply(update, f"노출감시를 시작합니다. (간격 {sec}초, 광고/기본 순위 모두 확인)")
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
                    f"· {s.get('time')} → {s.get('bid')}원"
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

<<<<<<< HEAD
    # 리뷰감시 중지
=======
    # 리뷰감시: 리뷰감시 [분], 리뷰감시중지
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
    if head in ["리뷰감시중지", "리뷰중지", "리뷰감시끄기"]:
        nav = state.setdefault("naver", {})
        cfg = nav.setdefault("review_watch", {})
        cfg["enabled"] = False
        save_state()
        reply(update, "리뷰감시를 중지했습니다.")
        return

<<<<<<< HEAD
    # 리뷰감시 시작: 리뷰감시 [분]
=======
    # 리뷰감시: 리뷰감시 [분]
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
<<<<<<< HEAD
        reply(
            update,
            f"리뷰감시를 시작합니다. {iv//60}분 간격으로 확인합니다.",
        )
        return

    if head in ["리뷰현황", "리뷰조회", "리뷰상태"]:
=======
        reply(update, f"리뷰감시를 시작합니다. {iv//60}분 간격으로 확인합니다.")
        return

    if head in ["리뷰현황","리뷰조회","리뷰상태"]:
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
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
            send_chart_with_text(context, m, title10, title60, msg)

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
                    send_chart_with_text(context, m, title10, title60, msg)

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
<<<<<<< HEAD
    up.job_queue.run_repeating(check_loop, interval=8, first=5)
=======
    up.job_queue.run_repeating(check_loop, interval=3, first=3)
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b
    up.job_queue.run_repeating(naver_schedule_loop, interval=30, first=10)
    up.job_queue.run_repeating(naver_abtest_loop, interval=15, first=15)
    up.job_queue.run_repeating(naver_rank_watch_loop, interval=60, first=20)
    up.job_queue.run_repeating(naver_review_watch_loop, interval=60, first=40)
<<<<<<< HEAD
    up.job_queue.run_repeating(gcal_watch_loop, interval=60, first=30)
=======
>>>>>>> d87eb91a20944608b9ab65b28a4b6e47e8ce4b2b

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

