from __future__ import annotations
import time
import os
from pathlib import Path
from dotenv import load_dotenv

# FORCE load .env from the same directory as this file
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)


def verify_env_loaded() -> None:
    REQUIRED = [
        "POLYGON_API_KEY",
        "FINNHUB_API_KEY",
        "GROQ_API_KEY",
        "UNUSUALWHALES_API_KEY",
        "UW_BASE_URL",
        "UW_SHORT_ENDPOINT_TEMPLATE",
        "UW_BORROW_ENDPOINT_TEMPLATE",
        "UW_OPTIONS_ENDPOINT_TEMPLATE",
        "FINRA_SHORTVOL_BASE",
        "EDGAR_USER_AGENT",
        "OUT_DIR",
        "CACHE_DB",
    ]

    print("\n==== .ENV LOAD VERIFICATION ====")
    for k in REQUIRED:
        v = os.getenv(k)
        if v is None or not str(v).strip():
            print(f"{k:35} ❌ MISSING")
        else:
            print(f"{k:35} ✅ LOADED")
    print("================================\n")


# 🔎 TEMPORARY: call once to verify .env loading




import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


# -------------------------
# Small helpers
# -------------------------

def iso_date(d: date) -> str:
    return d.isoformat()

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def safe_float(x: Any, default: float = float("nan")) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return default
        return float(x)
    except Exception:
        return default

def human_int(n: float) -> str:
    try:
        n = float(n)
    except Exception:
        return "NA"
    if math.isnan(n):
        return "NA"
    if abs(n) >= 1e9:
        return f"{n/1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"{n/1e6:.2f}M"
    if abs(n) >= 1e3:
        return f"{n/1e3:.2f}K"
    return f"{n:.0f}"

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def percentile_rank(value: float, lo: float, hi: float) -> float:
    if math.isnan(value):
        return 0.0
    if hi <= lo:
        return 0.0
    return clamp((value - lo) / (hi - lo), 0.0, 1.0)

def to_json_safe(obj: Any) -> Any:
    """Recursively convert NaN/Inf to None so json is valid and portable."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_json_safe(v) for v in obj]
    return obj


# -------------------------
# Config
# -------------------------

@dataclass
class Cfg:
    polygon_api_key: str
    finnhub_api_key: str
    groq_api_key: str
    groq_model: str
    groq_base_url: str
    uw_api_key: str
    uw_base_url: str
    uw_short_tpl: str
    uw_borrow_tpl: str
    uw_options_tpl: str
    out_dir: str
    edgar_user_agent: str
    finra_shortvol_base: str
    concurrency: int
    uw_concurrency: int
    cache_db: str

    # universe filters
    min_price: float = 0.5
    max_price: float = 25.0
    min_vol: float = 300_000.0
    min_dollar_vol: float = 1_000_000.0
    universe_limit: int = 2500
    top_n: int = 20

    # UW plan safe limits (your plan says 120 rpm; use conservative)
    uw_rpm: int = int(os.getenv("UW_RPM", "60"))
    uw_max_retries: int = 5


def getenv_required(k: str) -> str:
    v = os.getenv(k, "").strip()
    if not v:
        raise SystemExit(f"Missing required env var: {k}")
    return v

def load_cfg(args: argparse.Namespace) -> Cfg:
    return Cfg(
        polygon_api_key=getenv_required("POLYGON_API_KEY"),
        finnhub_api_key=os.getenv("FINNHUB_API_KEY", "").strip(),
        groq_api_key=os.getenv("GROQ_API_KEY", "").strip(),
        groq_model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant").strip(),
        groq_base_url=os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1").strip(),
        uw_api_key=os.getenv("UNUSUALWHALES_API_KEY", "").strip(),
        uw_base_url=os.getenv("UW_BASE_URL", "https://api.unusualwhales.com").strip(),
        uw_short_tpl=os.getenv("UW_SHORT_ENDPOINT_TEMPLATE", "/api/shorts/{ticker}/interest-float/v2").strip(),
        uw_borrow_tpl=os.getenv("UW_BORROW_ENDPOINT_TEMPLATE", "/api/shorts/{ticker}/data/v2").strip(),
        uw_options_tpl=os.getenv("UW_OPTIONS_ENDPOINT_TEMPLATE", "/api/stock/{ticker}/flow-recent").strip(),
        out_dir=os.getenv("OUT_DIR", "out").strip(),
        edgar_user_agent=os.getenv("EDGAR_USER_AGENT", "Finance Corp financecorp@email.com").strip(),
        finra_shortvol_base=os.getenv("FINRA_SHORTVOL_BASE", "https://cdn.finra.org/equity/regsho/daily").strip(),
        concurrency=int(getattr(args, "concurrency", 8)),
        uw_concurrency=int(getattr(args, "uw_concurrency", 3)),
        cache_db=os.getenv("CACHE_DB", "cache.sqlite").strip(),
    )


# -------------------------
# SQLite cache
# -------------------------

class Cache:
    def __init__(self, path: str):
        self.path = path
        self._init()

    def _init(self) -> None:
        con = sqlite3.connect(self.path)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    k TEXT PRIMARY KEY,
                    ts INTEGER NOT NULL,
                    ttl INTEGER NOT NULL,
                    j TEXT NOT NULL
                )
            """)
            con.commit()
        finally:
            con.close()

    def get(self, k: str) -> Optional[Dict[str, Any]]:
        con = sqlite3.connect(self.path)
        try:
            cur = con.execute("SELECT ts, ttl, j FROM cache WHERE k=?", (k,))
            row = cur.fetchone()
            if not row:
                return None
            ts, ttl, j = row
            if int(time.time()) > int(ts) + int(ttl):
                return None
            return json.loads(j)
        except Exception:
            return None
        finally:
            con.close()

    def set(self, k: str, obj: Dict[str, Any], ttl: int) -> None:
        con = sqlite3.connect(self.path)
        try:
            con.execute(
                "INSERT OR REPLACE INTO cache(k, ts, ttl, j) VALUES(?,?,?,?)",
                (k, int(time.time()), int(ttl), json.dumps(obj, ensure_ascii=False)),
            )
            con.commit()
        finally:
            con.close()

def cache_key(prefix: str, url: str, params: Optional[Dict[str, Any]] = None) -> str:
    raw = prefix + "|" + url + "|" + json.dumps(params or {}, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# -------------------------
# HTTP client
# -------------------------

class Http:
    def __init__(self, session: aiohttp.ClientSession, cache: Cache):
        self.s = session
        self.cache = cache

    async def get_bytes(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        ttl: int = 3600,
        prefix: str = "GETB",
        timeout: int = 30,
    ) -> bytes:
        k = cache_key(prefix, url, params)
        hit = self.cache.get(k)
        if hit is not None and "b64" in hit:
            return bytes.fromhex(hit["b64"])

        async with self.s.get(url, headers=headers, params=params, timeout=timeout) as r:
            data = await r.read()
            if r.status >= 400:
                txt = data[:300].decode("utf-8", errors="ignore")
                raise RuntimeError(f"HTTP {r.status} for {url}: {txt}")
        self.cache.set(k, {"b64": data.hex()}, ttl=ttl)
        return data

    async def get_text(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        ttl: int = 3600,
        prefix: str = "GETT",
        timeout: int = 30,
    ) -> str:
        b = await self.get_bytes(url, headers=headers, params=params, ttl=ttl, prefix=prefix, timeout=timeout)
        return b.decode("utf-8", errors="ignore")

    async def get_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        ttl: int = 3600,
        prefix: str = "GETJ",
        timeout: int = 30,
    ) -> Dict[str, Any]:
        k = cache_key(prefix, url, params)
        hit = self.cache.get(k)
        if hit is not None:
            return hit

        async with self.s.get(url, headers=headers, params=params, timeout=timeout) as r:
            txt = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"HTTP {r.status} for {url}: {txt[:250]}")
            j = json.loads(txt)
        self.cache.set(k, j, ttl=ttl)
        return j


# -------------------------
# Rate limiter (simple token bucket for UW)
# -------------------------

class TokenBucket:
    def __init__(self, rate_per_minute: int):
        self.rate = max(1, rate_per_minute)
        self.capacity = float(self.rate)
        self.tokens = float(self.rate)
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            # refill tokens per second
            refill = elapsed * (self.rate / 60.0)
            self.tokens = min(self.capacity, self.tokens + refill)
            self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # need to wait
            needed = 1.0 - self.tokens
            wait_s = needed / (self.rate / 60.0)
        await asyncio.sleep(max(0.05, wait_s))
        # try again
        await self.acquire()


# -------------------------
# Polygon
# -------------------------

class Polygon:
    def __init__(self, http: Http, api_key: str):
        self.http = http
        self.key = api_key
        self.base = "https://api.polygon.io"

    async def grouped_daily(self, asof: str) -> List[Dict[str, Any]]:
        url = f"{self.base}/v2/aggs/grouped/locale/us/market/stocks/{asof}"
        j = await self.http.get_json(url, params={"adjusted": "true", "apiKey": self.key}, ttl=6 * 3600, prefix="POLY_GROUPED")
        return j.get("results", []) or []

    async def daily_agg(self, ticker: str, start: str, end: str) -> List[Dict[str, Any]]:
        url = f"{self.base}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        j = await self.http.get_json(
            url,
            params={"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": self.key},
            ttl=6 * 3600,
            prefix="POLY_DAILY",
        )
        return j.get("results", []) or []

    async def ticker_details(self, ticker: str) -> Dict[str, Any]:
        # Used to exclude ETFs, ADRs, funds, etc.
        url = f"{self.base}/v3/reference/tickers/{ticker}"
        j = await self.http.get_json(url, params={"apiKey": self.key}, ttl=7 * 24 * 3600, prefix="POLY_TDETAIL")
        return j.get("results", {}) or {}


def is_excluded_asset(details: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Exclude ETFs/ETNs/funds and other non-common-equity instruments for squeeze logic.
    Polygon details often contain:
      type, market, locale, name, currency_name, primary_exchange, etc.
    """
    t = (details.get("type") or "").upper()
    name = (details.get("name") or "").upper()

    # Common Polygon 'type' values vary; we exclude obvious non-stocks
    if t in {"ETF", "ETN", "FUND", "MUTUALFUND", "INDEX", "CRYPTO", "FX"}:
        return True, f"type={t}"
    # Heuristic in case type missing:
    if "ETF" in name or "TRUST" in name or "ETN" in name:
        return True, "name_looks_like_etf"
    return False, ""


# -------------------------
# Unusual Whales (with retry/backoff + fallback endpoints)
# -------------------------

class UnusualWhales:
    def __init__(self, http: Http, cfg: Cfg, limiter: TokenBucket):
        self.http = http
        self.cfg = cfg
        self.limiter = limiter
        self.base = cfg.uw_base_url.rstrip("/")
        token = cfg.uw_api_key.strip().replace("Bearer ", "")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "pre-squeeze-scanner/1.0",
        }

    def _url(self, tpl: str, ticker: str) -> str:
        path = tpl.format(ticker=ticker.upper())
        if not path.startswith("/"):
            path = "/" + path
        return self.base + path

    async def _get_json_with_retry(self, url: str, ttl: int, prefix: str) -> Dict[str, Any]:
        # cached? then no limiter needed
        k = cache_key(prefix, url, None)
        hit = self.http.cache.get(k)
        if hit is not None:
            return hit

        last_err = None
        for attempt in range(self.cfg.uw_max_retries):
            await self.limiter.acquire()
            try:
                async with self.http.s.get(url, headers=self.headers, timeout=30) as r:
                    txt = await r.text()
                    if r.status == 429:
                        retry_after = r.headers.get("Retry-After")
                        wait_s = float(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
                        await asyncio.sleep(min(30.0, max(1.0, wait_s)))
                        last_err = RuntimeError(f"HTTP 429 for {url}: rate limited")
                        continue
                    if r.status >= 400:
                        raise RuntimeError(f"HTTP {r.status} for {url}: {txt[:250]}")
                    j = json.loads(txt)
                    self.http.cache.set(k, j, ttl=ttl)
                    return j
            except Exception as e:
                last_err = e
                await asyncio.sleep(min(10.0, 1.5 * (2 ** attempt)))

        raise RuntimeError(str(last_err)[:200] if last_err else f"UW request failed: {url}")

    async def interest_float(self, ticker: str) -> Dict[str, Any]:
        # try v2; fallback to v1 if 404
        url_v2 = self._url(self.cfg.uw_short_tpl, ticker)
        try:
            return await self._get_json_with_retry(url_v2, ttl=6 * 3600, prefix="UW_IF")
        except Exception as e:
            if "HTTP 404" in str(e) and url_v2.endswith("/v2"):
                url_v1 = url_v2[:-3]  # drop /v2
                return await self._get_json_with_retry(url_v1, ttl=6 * 3600, prefix="UW_IF1")
            raise

    async def borrow_data(self, ticker: str) -> Dict[str, Any]:
        url_v2 = self._url(self.cfg.uw_borrow_tpl, ticker)
        try:
            return await self._get_json_with_retry(url_v2, ttl=3 * 3600, prefix="UW_BORROW")
        except Exception as e:
            if "HTTP 404" in str(e) and url_v2.endswith("/v2"):
                url_v1 = url_v2[:-3]
                return await self._get_json_with_retry(url_v1, ttl=3 * 3600, prefix="UW_BORROW1")
            raise

    async def flow_recent(self, ticker: str) -> Dict[str, Any]:
        url = self._url(self.cfg.uw_options_tpl, ticker)
        return await self._get_json_with_retry(url, ttl=2 * 3600, prefix="UW_FLOW")


def parse_uw_interest_float(j: Dict[str, Any]) -> Dict[str, Any]:
    data = j.get("data", j)
    if isinstance(data, list) and data:
        rec = data[0]
    elif isinstance(data, dict):
        rec = data
        if "data" in rec and isinstance(rec["data"], dict):
            rec = rec["data"]
    else:
        rec = {}
    return {
        "days_to_cover": safe_float(rec.get("days_to_cover")),
        "si_float": safe_float(rec.get("si_float")),
        "short_interest": safe_float(rec.get("short_interest")),
        "total_float": safe_float(rec.get("total_float")),
        "asof": rec.get("market_date") or rec.get("asof") or rec.get("date"),
    }

def parse_uw_borrow(j: Dict[str, Any]) -> Dict[str, Any]:
    data = j.get("data", j)
    if isinstance(data, list) and data:
        rec = data[0]
    elif isinstance(data, dict):
        rec = data
    else:
        rec = {}

    fee = safe_float(rec.get("fee_rate"))
    avail = safe_float(rec.get("short_shares_available"))
    if math.isnan(fee):
        fee = safe_float(rec.get("borrow_fee")) or safe_float(rec.get("borrow_fee_apr"))
    if math.isnan(avail):
        avail = safe_float(rec.get("shares_available"))
    return {
        "fee_rate": fee,
        "shares_available": avail,
        "asof": rec.get("market_date") or rec.get("date"),
    }


# -------------------------
# FINRA RegSHO
# -------------------------

async def finra_shortvol_ratio(http: Http, base_url: str, asof: str, ticker: str) -> float:
    """
    FINRA RegSHO daily: CNMSshvolYYYYMMDD.txt or .txt.gz
    """
    ymd = asof.replace("-", "")
    candidates = [
        f"{base_url.rstrip('/')}/CNMSshvol{ymd}.txt",
        f"{base_url.rstrip('/')}/CNMSshvol{ymd}.txt.gz",
    ]

    raw = None
    for u in candidates:
        try:
            b = await http.get_bytes(u, ttl=24 * 3600, prefix="FINRA_B")
            # If gz:
            if u.endswith(".gz"):
                try:
                    b = gzip.decompress(b)
                except Exception:
                    continue
            raw = b.decode("utf-8", errors="ignore")
            if "|" in raw:
                break
        except Exception:
            continue

    if not raw:
        return float("nan")

    t = ticker.upper()
    for line in raw.splitlines():
        if not line or "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) < 6:
            continue
        if parts[0].lower() == "date":
            continue
        sym = parts[1].strip().upper()
        if sym != t:
            continue
        short_vol = safe_float(parts[2])
        total_vol = safe_float(parts[4])
        if total_vol and not math.isnan(total_vol) and total_vol > 0:
            return short_vol / total_vol
    return float("nan")


# -------------------------
# Finnhub + Groq
# -------------------------

class Finnhub:
    def __init__(self, http: Http, api_key: str):
        self.http = http
        self.key = api_key
        self.base = "https://finnhub.io/api/v1"

    async def company_news(self, ticker: str, frm: str, to: str) -> List[Dict[str, Any]]:
        url = f"{self.base}/company-news"
        j = await self.http.get_json(
            url,
            params={"symbol": ticker.upper(), "from": frm, "to": to, "token": self.key},
            ttl=2 * 3600,
            prefix="FINNHUB_NEWS",
        )
        return j if isinstance(j, list) else (j.get("news", []) or [])


class Groq:
    def __init__(self, session: aiohttp.ClientSession, cache: Cache, api_key: str, base_url: str, model: str):
        self.s = session
        self.cache = cache
        self.key = api_key.strip()
        self.base = base_url.rstrip("/")
        self.model = model

    async def score_news(self, ticker: str, headlines: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self.key or not headlines:
            return {"sentiment": 0.0, "catalyst_strength": 0.0, "is_real_catalyst": False, "risk_flags": [], "summary": ""}

        items = []
        for h in headlines[:10]:
            title = (h.get("headline") or h.get("title") or "").strip()
            src = (h.get("source") or "").strip()
            dt = h.get("datetime")
            if isinstance(dt, (int, float)):
                # timezone-aware fix
                dt = datetime.fromtimestamp(int(dt), tz=timezone.utc).strftime("%Y-%m-%d")
            items.append({"title": title[:180], "source": src[:40], "date": str(dt)[:10]})

        prompt = f"""
Return STRICT JSON only:
sentiment (-1..1),
catalyst_strength (0..10),
is_real_catalyst (boolean),
risk_flags (array),
summary (<=240 chars).
Evaluate whether {ticker.upper()} has a REAL near-term catalyst that could sustain buying (pre-squeeze ignition).
Headlines:
{json.dumps(items, ensure_ascii=False)}
""".strip()

        url = f"{self.base}/chat/completions"
        headers = {"Authorization": f"Bearer {self.key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": "Return valid JSON only. No markdown."},
                {"role": "user", "content": prompt},
            ],
        }

        ck = hashlib.sha256((ticker.upper() + "|" + json.dumps(items, sort_keys=True)).encode("utf-8")).hexdigest()
        hit = self.cache.get("GROQ|" + ck)
        if hit is not None:
            return hit

        async with self.s.post(url, headers=headers, json=payload, timeout=45) as r:
            txt = await r.text()
            if r.status >= 400:
                out = {"sentiment": 0.0, "catalyst_strength": 0.0, "is_real_catalyst": False, "risk_flags": ["groq_error"], "summary": txt[:160]}
                self.cache.set("GROQ|" + ck, out, ttl=6 * 3600)
                return out
            j = json.loads(txt)
            content = j["choices"][0]["message"]["content"]
            out = json.loads(content)

        out = {
            "sentiment": float(out.get("sentiment", 0.0)),
            "catalyst_strength": float(out.get("catalyst_strength", 0.0)),
            "is_real_catalyst": bool(out.get("is_real_catalyst", False)),
            "risk_flags": out.get("risk_flags", []) or [],
            "summary": (out.get("summary", "") or "")[:240],
        }
        self.cache.set("GROQ|" + ck, out, ttl=6 * 3600)
        return out


# -------------------------
# SEC EDGAR (lightweight risk)
# -------------------------

SEC_BASE = "https://data.sec.gov"
SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

async def sec_get_ticker_to_cik(http: Http, user_agent: str) -> Dict[str, str]:
    headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate", "Accept": "application/json"}
    j = await http.get_json(SEC_TICKER_MAP_URL, headers=headers, ttl=7 * 24 * 3600, prefix="SEC_TMAP")
    out: Dict[str, str] = {}
    for _, rec in j.items():
        t = str(rec.get("ticker", "")).upper()
        cik = str(rec.get("cik_str", "")).strip()
        if t and cik:
            out[t] = cik.zfill(10)
    return out

async def sec_recent_filings_risk(http: Http, user_agent: str, cik10: str) -> Dict[str, Any]:
    headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate", "Accept": "application/json"}
    url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
    j = await http.get_json(url, headers=headers, ttl=12 * 3600, prefix="SEC_SUB")

    filings = (((j.get("filings") or {}).get("recent")) or {})
    forms = filings.get("form", []) or []
    dates = filings.get("filingDate", []) or []

    risk = 0.0
    flags: List[str] = []

    for i in range(min(len(forms), 30)):
        form = str(forms[i])
        if form in ("S-3", "S-1"):
            risk += 20; flags.append(f"recent_{form}")
        elif form.startswith("424"):
            risk += 15; flags.append("prospectus_424")
        elif form == "S-8":
            risk += 8; flags.append("s8_comp_shares")

    return {"risk": clamp(risk, 0, 40), "flags": sorted(list(set(flags)))}


# -------------------------
# Features
# -------------------------

async def resolve_asof(poly: Polygon, preferred: Optional[str] = None) -> str:
    if preferred:
        return preferred
    d = date.today()
    for _ in range(10):
        asof = iso_date(d)
        try:
            rows = await poly.grouped_daily(asof)
            if rows:
                return asof
        except Exception:
            pass
        d -= timedelta(days=1)
    return iso_date(date.today() - timedelta(days=1))


async def build_universe(poly: Polygon, asof: str, cfg: Cfg) -> List[Dict[str, Any]]:
    rows = await poly.grouped_daily(asof)
    out = []
    for r in rows:
        t = (r.get("T") or "").upper().strip()
        if not t or not re.match(r"^[A-Z.\-]{1,10}$", t):
            continue
        c = safe_float(r.get("c"))
        v = safe_float(r.get("v"))
        if math.isnan(c) or math.isnan(v):
            continue
        if c < cfg.min_price or c > cfg.max_price:
            continue
        if v < cfg.min_vol:
            continue
        dollar_vol = c * v
        if dollar_vol < cfg.min_dollar_vol:
            continue
        out.append({"ticker": t, "close": c, "volume": v, "dollar_vol": dollar_vol})
    out.sort(key=lambda x: x["dollar_vol"], reverse=True)
    return out[: cfg.universe_limit]


async def price_volume_features(poly: Polygon, ticker: str, asof: str) -> Dict[str, Any]:
    end = date.fromisoformat(asof)
    start = end - timedelta(days=45)
    bars = await poly.daily_agg(ticker, iso_date(start), iso_date(end))
    if not bars:
        return {}

    closes = [safe_float(b.get("c")) for b in bars if b.get("c") is not None]
    vols = [safe_float(b.get("v")) for b in bars if b.get("v") is not None]
    highs = [safe_float(b.get("h")) for b in bars if b.get("h") is not None]
    lows = [safe_float(b.get("l")) for b in bars if b.get("l") is not None]

    if len(closes) < 10:
        return {}

    c0 = closes[-1]
    v0 = vols[-1] if vols else float("nan")
    avg20 = sum(vols[-20:]) / max(1, len(vols[-20:]))
    vol_spike = (v0 / avg20) if avg20 and not math.isnan(v0) else float("nan")
    range_pct = ((highs[-1] - lows[-1]) / c0) * 100 if c0 else float("nan")

    prev20 = closes[-21:-1] if len(closes) >= 21 else closes[:-1]
    prior_high = max(prev20) if prev20 else float("nan")
    breakout = 1.0 if (not math.isnan(prior_high) and c0 > prior_high) else 0.0

    last10 = closes[-10:]
    x = list(range(len(last10)))
    x_mean = sum(x) / len(x)
    y_mean = sum(last10) / len(last10)
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, last10))
    den = sum((xi - x_mean) ** 2 for xi in x) or 1.0
    slope = num / den
    slope_pct = (slope / c0) * 100 if c0 else 0.0

    return {
        "price": c0,
        "volume": v0,
        "avg20_volume": avg20,
        "vol_spike": vol_spike,
        "range_pct": range_pct,
        "breakout": breakout,
        "trend_slope_pct": slope_pct,
    }


def presqueeze_score(feat: Dict[str, Any]) -> Tuple[float, Dict[str, float], str]:
    """
    Returns (score 0..100, score_parts, bucket)
    Buckets:
      TRUE_PRE_SQUEEZE: UW short info present (si + dtc) and at least one borrow stress signal
      WATCHLIST: partial UW OR strong FINRA SVR + decent price ignition
      MOMENTUM_ONLY: no UW short info (treat as momentum, not squeeze)
    """

    si = safe_float(feat.get("uw_si_float"))
    dtc = safe_float(feat.get("uw_dtc"))
    fee = safe_float(feat.get("uw_fee_rate"))
    avail = safe_float(feat.get("uw_shares_available"))
    finra = safe_float(feat.get("finra_svr"))

    has_uw_short = (not math.isnan(si)) and (not math.isnan(dtc))
    has_borrow = (not math.isnan(fee)) or (not math.isnan(avail))

    # Price ignition
    vol_spike = safe_float(feat.get("vol_spike"))
    breakout = safe_float(feat.get("breakout"))
    trend = safe_float(feat.get("trend_slope_pct"))

    # News
    cat = safe_float(feat.get("news_catalyst_strength"))
    sent = safe_float(feat.get("news_sentiment"))

    # Dilution penalty
    edgar = safe_float(feat.get("edgar_risk"))

    # A) short crowding (0-30)
    a_si = percentile_rank(si, 10, 45) * 18
    a_dtc = percentile_rank(dtc, 2, 10) * 12
    A = a_si + a_dtc

    # B) borrow stress (0-25)
    b_fee = percentile_rank(fee, 10, 150) * 16
    b_avail = (1.0 - percentile_rank(avail, 0, 2_000_000)) * 9 if not math.isnan(avail) else 0.0
    B = b_fee + b_avail

    # C) price/volume ignition (0-25)
    c_vol = percentile_rank(vol_spike, 1.0, 8.0) * 12
    c_brk = clamp(breakout, 0, 1) * 6
    c_trend = percentile_rank(trend, -0.2, 0.8) * 7
    C = c_vol + c_brk + c_trend

    # D) catalyst (0-15)
    d_cat = percentile_rank(cat, 0, 8) * 10
    d_sent = percentile_rank(sent, -0.2, 0.6) * 5
    D = d_cat + d_sent

    # E) dilution penalty (-0..-30)
    E = -percentile_rank(edgar, 5, 35) * 30

    total = A + B + C + D + E

    # Hard plan-aware penalties:
    # If no UW short data, you can't call it pre-squeeze. Penalize score so momentum doesn't pollute top list.
    missing_pen = 0.0
    if not has_uw_short:
        missing_pen -= 22.0  # strong penalty
    # If UW short exists but borrow missing, still possible but weaker
    if has_uw_short and not has_borrow:
        missing_pen -= 6.0

    # FINRA SVR bonus (pressure confirmation)
    finra_bonus = percentile_rank(finra, 0.45, 0.70) * 6 if not math.isnan(finra) else 0.0

    total = clamp(total + missing_pen + finra_bonus, 0, 100)

    # Bucket classification
    if has_uw_short and has_borrow:
        bucket = "TRUE_PRE_SQUEEZE"
    elif has_uw_short or (not math.isnan(finra) and finra >= 0.55 and (vol_spike >= 1.5 or breakout >= 1.0)):
        bucket = "WATCHLIST"
    else:
        bucket = "MOMENTUM_ONLY"

    parts = {"A_short": A, "B_borrow": B, "C_price": C, "D_news": D, "E_dilution": E, "pen_missing": missing_pen, "bonus_finra": finra_bonus}
    return total, parts, bucket


# -------------------------
# Build one ticker row
# -------------------------

async def build_row(
    ticker: str,
    asof: str,
    poly: Polygon,
    uw: Optional[UnusualWhales],
    finnhub: Optional[Finnhub],
    groq: Optional[Groq],
    http: Http,
    cfg: Cfg,
    sec_tmap: Optional[Dict[str, str]],
    uw_sem: asyncio.Semaphore,
) -> Optional[Dict[str, Any]]:
    out: Dict[str, Any] = {"ticker": ticker, "asof": asof}

    # Exclude ETFs/funds/etc using Polygon reference details
    try:
        details = await poly.ticker_details(ticker)
        excluded, reason = is_excluded_asset(details)
        if excluded:
            return None
        out["asset_type"] = (details.get("type") or "")
    except Exception:
        # If details fail, continue (but risk ETFs leaking). You can choose to skip instead.
        out["asset_type"] = ""

    # Polygon price/volume features
    pv = await price_volume_features(poly, ticker, asof)
    out.update(pv)

    # UW (rate-limited)
    if uw and cfg.uw_api_key:
        async with uw_sem:
            try:
                j_if = await uw.interest_float(ticker)
                p_if = parse_uw_interest_float(j_if)
                out["uw_dtc"] = p_if.get("days_to_cover")
                out["uw_si_float"] = p_if.get("si_float")
                out["uw_short_interest"] = p_if.get("short_interest")
                out["uw_total_float"] = p_if.get("total_float")
            except Exception as e:
                out["uw_err"] = str(e)[:200]

        async with uw_sem:
            try:
                j_b = await uw.borrow_data(ticker)
                p_b = parse_uw_borrow(j_b)
                out["uw_fee_rate"] = p_b.get("fee_rate")
                out["uw_shares_available"] = p_b.get("shares_available")
            except Exception as e:
                out["uw_borrow_err"] = str(e)[:200]

        # Options flow is optional; keep lightweight
        async with uw_sem:
            try:
                j_flow = await uw.flow_recent(ticker)
                data = j_flow.get("data", j_flow)
                prem = 0.0
                if isinstance(data, list) and data:
                    for rec in data[:50]:
                        prem += safe_float(rec.get("premium"), 0.0)
                out["uw_flow_premium"] = prem
            except Exception:
                pass

    # FINRA short volume ratio
    try:
        out["finra_svr"] = await finra_shortvol_ratio(http, cfg.finra_shortvol_base, asof, ticker)
    except Exception:
        out["finra_svr"] = float("nan")

    # News + Groq
    if finnhub and cfg.finnhub_api_key:
        try:
            end = date.fromisoformat(asof)
            frm = iso_date(end - timedelta(days=7))
            news = await finnhub.company_news(ticker, frm, asof)
            out["news_count_7d"] = len(news)
            if groq and cfg.groq_api_key and news:
                g = await groq.score_news(ticker, news)
                out["news_sentiment"] = safe_float(g.get("sentiment"), 0.0)
                out["news_catalyst_strength"] = safe_float(g.get("catalyst_strength"), 0.0)
                out["news_is_real_catalyst"] = bool(g.get("is_real_catalyst", False))
                out["news_summary"] = (g.get("summary", "") or "")[:240]
                out["news_risk_flags"] = ",".join(g.get("risk_flags", []) or [])
        except Exception as e:
            out["news_err"] = str(e)[:180]

    # EDGAR dilution risk
    if sec_tmap and cfg.edgar_user_agent:
        cik10 = sec_tmap.get(ticker.upper())
        if cik10:
            try:
                risk = await sec_recent_filings_risk(http, cfg.edgar_user_agent, cik10)
                out["edgar_risk"] = safe_float(risk.get("risk"), 0.0)
                out["edgar_flags"] = ",".join(risk.get("flags", []) or [])
            except Exception as e:
                out["edgar_err"] = str(e)[:180]
        else:
            out["edgar_risk"] = 0.0

    # Score + bucket
    total, parts, bucket = presqueeze_score(out)
    out["presqueeze_score"] = total
    out["bucket"] = bucket
    for k, v in parts.items():
        out[f"score_{k}"] = v

    return out


# -------------------------
# Printing + saving
# -------------------------

def print_table(title: str, rows: List[Dict[str, Any]], top_n: int) -> None:
    if not rows:
        print(f"\n{title}: (no results)\n")
        return

    cols = [
        "ticker", "bucket", "presqueeze_score", "price",
        "vol_spike", "range_pct", "breakout", "trend_slope_pct",
        "uw_si_float", "uw_dtc", "uw_fee_rate", "uw_shares_available",
        "finra_svr", "news_catalyst_strength", "news_sentiment", "edgar_risk"
    ]
    rows = rows[:top_n]

    print("\n" + "=" * 118)
    print(title)
    print("=" * 118)
    print(" | ".join([c.ljust(18) for c in cols]))
    print("-" * 118)

    for r in rows:
        line = []
        for c in cols:
            v = r.get(c)
            if c in ("ticker", "bucket"):
                s = str(v or "").ljust(18)
            elif isinstance(v, (int, float)):
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    s = "NA".ljust(18)
                else:
                    if c in ("price",):
                        s = f"{fv:.2f}".ljust(18)
                    elif c in ("presqueeze_score",):
                        s = f"{fv:.1f}".ljust(18)
                    elif c in ("uw_shares_available", "volume", "avg20_volume"):
                        s = human_int(fv).ljust(18)
                    else:
                        s = f"{fv:.3f}".ljust(18)
            else:
                s = (str(v) if v is not None else "NA")[:18].ljust(18)
            line.append(s)
        print(" | ".join(line))
    print("-" * 118)
    print("Note: TRUE_PRE_SQUEEZE requires UW short+borrow data. WATCHLIST = partial confirmation. MOMENTUM_ONLY is not squeeze-confirmed.\n")


def save_outputs(rows: List[Dict[str, Any]], out_dir: str, asof: str) -> Tuple[str, str]:
    ensure_dir(out_dir)
    stamp = asof.replace("-", "")
    json_path = os.path.join(out_dir, f"presqueeze_{stamp}.json")
    csv_path = os.path.join(out_dir, f"presqueeze_{stamp}.csv")

    safe_rows = to_json_safe(rows)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(safe_rows, f, ensure_ascii=False, indent=2, allow_nan=False)

    keys = sorted({k for r in rows for k in r.keys()})
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            rr = {k: r.get(k) for k in keys}
            rr = to_json_safe(rr)
            w.writerow(rr)

    return json_path, csv_path


# -------------------------
# Main
# -------------------------

async def main_async(args: argparse.Namespace) -> int:
    cfg = load_cfg(args)
    ensure_dir(cfg.out_dir)
    cache = Cache(cfg.cache_db)

    timeout = aiohttp.ClientTimeout(total=70)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        http = Http(session, cache)
        poly = Polygon(http, cfg.polygon_api_key)

        asof = await resolve_asof(poly, args.asof)

        # SEC mapping
        sec_tmap = None
        try:
            sec_tmap = await sec_get_ticker_to_cik(http, cfg.edgar_user_agent)
        except Exception:
            sec_tmap = None

        # Universe
        universe = await build_universe(poly, asof, cfg)
        tickers = [u["ticker"] for u in universe]
        print(f"Universe size: {len(tickers)} (asof={asof})", flush=True)

        # Clients
        finnhub = Finnhub(http, cfg.finnhub_api_key) if cfg.finnhub_api_key else None
        groq = Groq(session, cache, cfg.groq_api_key, cfg.groq_base_url, cfg.groq_model) if cfg.groq_api_key else None

        uw = None
        uw_sem = asyncio.Semaphore(max(1, cfg.uw_concurrency))
        if cfg.uw_api_key:
            limiter = TokenBucket(cfg.uw_rpm)
            uw = UnusualWhales(http, cfg, limiter)

        # Global concurrency for total tasks
        sem = asyncio.Semaphore(max(1, cfg.concurrency))

        async def run_one(t: str) -> Optional[Dict[str, Any]]:
            async with sem:
                return await build_row(t, asof, poly, uw, finnhub, groq, http, cfg, sec_tmap, uw_sem)

        tasks = [asyncio.create_task(run_one(t)) for t in tickers]

        rows: List[Dict[str, Any]] = []
        total = len(tasks)
        done = 0

        # ✅ heartbeat progress (prints every 5 seconds even if nothing finishes yet)
        start_t = time.time()

        async def progress_heartbeat() -> None:
            while done < total:
                await asyncio.sleep(5)
                elapsed = time.time() - start_t
                print(f"[progress] done {done}/{total} | elapsed {elapsed:.0f}s", flush=True)

        hb_task = asyncio.create_task(progress_heartbeat())

        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    r = await fut
                    done += 1

                    # ✅ completion counter (prints every 50 finished)
                    if done % 50 == 0 or done == total:
                        print(f"Processed {done}/{total} tickers", flush=True)

                    if r is not None:
                        rows.append(r)

                except Exception:
                    done += 1
                    if done % 50 == 0 or done == total:
                        print(f"Processed {done}/{total} tickers", flush=True)

        finally:
            hb_task.cancel()

        rows.sort(key=lambda r: safe_float(r.get("presqueeze_score"), 0.0), reverse=True)

        # Split buckets
        true_ps = [r for r in rows if r.get("bucket") == "TRUE_PRE_SQUEEZE"]
        watch = [r for r in rows if r.get("bucket") == "WATCHLIST"]
        momo = [r for r in rows if r.get("bucket") == "MOMENTUM_ONLY"]

        top = args.top or cfg.top_n
        print_table(f"🔥 TRUE PRE-SQUEEZE CANDIDATES (asof={asof})", true_ps, top)
        print_table(f"🟨 WATCHLIST (partial confirmation) (asof={asof})", watch, top)
        print_table(f"🟦 MOMENTUM ONLY (not squeeze-confirmed) (asof={asof})", momo, top)

        json_path, csv_path = save_outputs(rows, cfg.out_dir, asof)
        print(f"Saved:\n  {json_path}\n  {csv_path}\n", flush=True)
        return 0


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pre-squeeze daily scanner (plan-aware)")
    p.add_argument("--asof", default=None, help="YYYY-MM-DD (auto: latest Polygon grouped day)")
    p.add_argument("--concurrency", type=int, default=8, help="Total concurrency (default 8)")
    p.add_argument("--uw-concurrency", type=int, default=3, help="UW concurrency (default 3)")
    p.add_argument("--top", type=int, default=20, help="Top N to show per bucket (default 20)")
    return p


def main() -> None:
    args = build_argparser().parse_args()
    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
