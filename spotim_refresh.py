"""Spot.IM JWT 自动续期脚本（CDP 提取）

原理：WSJ 网页的 Spot.IM 评论 widget 加载时会发出带 x-access-token 请求头的
JWT（有效期约 1 小时）。本脚本通过 CDP 在调试 Chrome 里新开一个 tab，打开一篇
有评论的文章、触发评论 widget、从 Network 事件里抓出该 token，验证后写入
spotim_jwt.txt。

用法:
  python spotim_refresh.py            # 单次提取并写文件
  python spotim_refresh.py --loop     # 常驻：每 40 分钟自动续一次

comment_collector 已内嵌调用本模块，通常无需手动运行。
前置: Chrome 以 --remote-debugging-port=9222 启动（独立 profile）且已登录 WSJ
"""

import base64
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
import psycopg2

from config import CDP_HOST, PG_CONFIG
from archive_collector import CDPClient

TOKEN_FILE = Path(__file__).parent / "spotim_jwt.txt"
REFRESH_MARGIN_SEC = 600  # 剩余不足 10 分钟即续期
LOOP_INTERVAL_SEC = 40 * 60  # 常驻模式轮询间隔（token 约 56 分钟有效）

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spotim_refresh")

# 点击/触发评论 widget 的 JS（WSJ 网页按钮文案为 Show Conversation / Conversation (N)）
CLICK_JS = """
(() => {
  window.scrollTo(0, Math.floor(document.body.scrollHeight / 2));
  const norm = s => (s || '').toLowerCase();
  const cands = [...document.querySelectorAll('button, a, div[role="button"], span')];
  const btn = cands.find(e => {
    const t = norm(e.textContent);
    return t.includes('show conversation') || /conversation\\s*\\(\\d+\\)/.test(t);
  });
  if (btn) { btn.click(); return 'clicked'; }
  return 'not-found';
})()
"""


def jwt_remaining_seconds(jwt: Optional[str]) -> float:
    """返回 JWT 剩余有效秒数；解析失败/缺失返回 -1"""
    if not jwt:
        return -1
    try:
        pay = jwt.split(".")[1]
        pay += "=" * ((4 - len(pay) % 4) % 4)
        exp = json.loads(base64.urlsafe_b64decode(pay)).get("exp", 0)
        return exp - time.time()
    except Exception:
        return -1


def save_spotim_jwt(jwt: str):
    TOKEN_FILE.write_text(jwt, encoding="utf-8")


def _http(url: str) -> httpx.Response:
    # CDP 为纯本地通信，禁止走系统代理
    return httpx.get(url, timeout=10, trust_env=False)


def _pick_articles(limit: int = 5) -> list[tuple]:
    """挑几篇有评论的文章用于触发 widget（最近的优先）"""
    db = psycopg2.connect(**PG_CONFIG)
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT Art_URL, Art_ID FROM Article_Info
            WHERE Comments_Count > 0
              AND Art_Text NOT LIKE 'FAILED:%%'
              AND Art_URL LIKE 'https://www.wsj.com/articles/%%'
            ORDER BY scrape_time DESC LIMIT %s
        """, (limit,))
        return cur.fetchall()
    finally:
        db.close()


def _verify(jwt: str, post_id: str) -> bool:
    """用 mobile-gw conversation/read 验证 token；Bearer 与 x-access-token 两种头都试"""
    body = {"offset": 0, "count": 1, "sort_by": "best", "extract_data": False,
            "depth": 1, "tab_id": "all", "with_star_rating": False}
    base = {
        "accept": "application/json",
        "content-type": "application/json; charset=utf-8",
        "x-spot-id": "sp_92LbaOI5",
        "x-post-id": post_id,
        "user-agent": "wsj-version-6.18.1.1-code-61801001-android-32",
    }
    for extra in ({"authorization": f"Bearer {jwt}"}, {"x-access-token": jwt}):
        try:
            r = httpx.post("https://mobile-gw.spot.im/conversation/read",
                           json=body, headers={**base, **extra}, timeout=30)
            if r.status_code == 200 and "error" not in r.json():
                return True
        except Exception as e:
            log.warning(f"verify error: {e}")
    return False


def _pump_token(client: CDPClient, seconds: float) -> Optional[str]:
    """从 Network.requestWillBeSent 事件里抓 spot.im 请求上的 JWT"""
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            client.ws.settimeout(max(1.0, deadline - time.time()))
            msg = json.loads(client.ws.recv())
        except Exception:
            break
        if msg.get("method") != "Network.requestWillBeSent":
            continue
        req = msg.get("params", {}).get("request", {})
        if "spot.im" not in req.get("url", ""):
            continue
        headers = {k.lower(): v for k, v in req.get("headers", {}).items()}
        tok = headers.get("x-access-token")
        auth = headers.get("authorization", "")
        if not tok and auth.startswith("Bearer "):
            tok = auth[7:]
        if tok and tok.count(".") == 2:
            return tok
    return None


def extract_spotim_jwt_via_cdp() -> Optional[str]:
    """CDP 提取 Spot.IM JWT：新 tab → 文章页 → 触发 widget → 抓请求头 → 验证"""
    try:
        # 新版 Chrome 的 /json/new 只接受 PUT（GET 返回 405），旧版两者皆可
        url = f"{CDP_HOST}/json/new?url=about:blank"
        r = httpx.request("PUT", url, timeout=10, trust_env=False)
        if r.status_code in (400, 405):
            r = httpx.get(url, timeout=10, trust_env=False)
        tab = r.json()
    except Exception as e:
        log.error(f"CDP new tab failed: {e}")
        return None

    tab_id = tab.get("id")
    client = CDPClient(tab["webSocketDebuggerUrl"], timeout=30)
    token = None
    try:
        client.send_and_wait("Network.enable", timeout=5)
        client.enable_page_events()
        for art_url, art_id in _pick_articles():
            log.info(f"Trying article: {art_url[:70]}")
            if not client.navigate_and_wait(art_url, timeout=25):
                continue
            time.sleep(3)
            for attempt in range(2):
                client.evaluate(CLICK_JS, timeout=10)
                token = _pump_token(client, 25 if attempt == 0 else 15)
                if token:
                    break
                time.sleep(2)
            if token:
                if _verify(token, art_id):
                    log.info(f"Token verified (expires in {jwt_remaining_seconds(token)/60:.0f} min)")
                    return token
                log.warning("Token captured but verification failed, trying next article")
                token = None
    except Exception as e:
        log.error(f"Extraction error: {e}")
    finally:
        client.close()
        try:
            _http(f"{CDP_HOST}/json/close/{tab_id}")
        except Exception:
            pass
    return None


def refresh_spotim_jwt() -> Optional[str]:
    """提取并落盘，返回新 JWT（失败返回 None）"""
    jwt = extract_spotim_jwt_via_cdp()
    if jwt:
        save_spotim_jwt(jwt)
        log.info(f"Saved to {TOKEN_FILE}")
    return jwt


def main():
    if "--loop" in sys.argv:
        log.info(f"Loop mode: refresh every {LOOP_INTERVAL_SEC // 60} min")
        while True:
            refresh_spotim_jwt()
            time.sleep(LOOP_INTERVAL_SEC)
    else:
        jwt = refresh_spotim_jwt()
        if not jwt:
            log.error("Extraction failed. Is debug Chrome running and logged into WSJ?")
            sys.exit(1)


if __name__ == "__main__":
    main()
