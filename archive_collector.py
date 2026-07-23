"""WSJ 归档采集器 - 通过 CDP 从 __NEXT_DATA__ 提取 1997-2026 全部文章 URL 和正文

Phase 1: 遍历归档日页 → 提取 newsArchiveArticles → 写入 Daily_Articles
Phase 2: 遍历 Daily_Articles → CDP 导航文章页 → 提取正文 → 写入 Article_Info

前置条件: 本地 Chrome 以调试模式运行并已登录 WSJ
    chrome.exe --remote-debugging-port=9222 --remote-allow-origins=*
"""

import json
import sys
import time
import random
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import psycopg2
import psycopg2.extras
from websocket import create_connection, WebSocket

# ============================================================
# 配置
# ============================================================
from config import PG_CONFIG, CDP_HOST

CHROME_WSJ_ARCHIVE = "https://www.wsj.com/news/archive"

START_YEAR = 1997
END_YEAR = 2026

# 采集间隔（加入随机性对抗风控）
DAY_PAGE_DELAY = 2.0    # 日页之间间隔（秒）
ARTICLE_DELAY_MIN = 8.0  # 文章页最小间隔
ARTICLE_DELAY_MAX = 15.0 # 文章页最大间隔
HOMEPAGE_REFRESH_EVERY = 20  # 每N篇文章回首页一次，模拟人类浏览

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("archive_collector.log", encoding="utf-8")]
)
log = logging.getLogger("archive")


# ============================================================
# 数据库
# ============================================================

def get_db():
    return psycopg2.connect(**PG_CONFIG)


def init_db():
    """确保表存在"""
    db = get_db()
    cur = db.cursor()
    # 使用 wsj/schema.sql 中的表结构（如果还没建的话）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Daily_Articles (
            id SERIAL PRIMARY KEY,
            Year INTEGER NOT NULL,
            Month INTEGER NOT NULL,
            Date TEXT NOT NULL,
            Article_Title TEXT NOT NULL,
            Article_URL TEXT NOT NULL,
            scrape_time TEXT NOT NULL,
            UNIQUE(Date, Article_URL)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Article_Info (
            Art_ID TEXT PRIMARY KEY,
            Art_Title TEXT NOT NULL,
            Art_Title_Short TEXT,
            Art_Author TEXT,
            Art_Time TEXT,
            Art_Tag_1 TEXT,
            Art_Tag_2 TEXT,
            Comments_Count INTEGER DEFAULT 0,
            Art_URL TEXT NOT NULL,
            Spot_ID TEXT,
            Post_ID TEXT,
            scrape_time TEXT NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_progress (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    # 为 Article_Info 添加缺失的列
    for col, col_type in [("Art_Text", "TEXT"), ("Art_Text_HTML", "TEXT")]:
        try:
            cur.execute(f"ALTER TABLE Article_Info ADD COLUMN IF NOT EXISTS {col} {col_type}")
        except:
            pass
    db.commit()
    cur.close()
    db.close()
    log.info("Database initialized")


def clear_db():
    """清空数据表（保留结构）"""
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM Daily_Articles")
    cur.execute("DELETE FROM Article_Info")
    cur.execute("DELETE FROM scrape_progress")
    db.commit()
    log.info(f"Cleared Daily_Articles: {cur.rowcount} rows")
    log.info(f"Cleared Article_Info: {cur.rowcount} rows")
    log.info(f"Cleared scrape_progress")
    cur.close()
    db.close()


def ensure_db(db):
    """检查数据库连接是否存活，若断开则重连"""
    try:
        db.cursor().execute("SELECT 1")
        return db
    except Exception:
        log.warning("DB connection lost, reconnecting...")
        try:
            db.close()
        except:
            pass
        new_db = get_db()
        log.info("DB reconnected successfully")
        return new_db


def insert_articles(db, articles: list[dict]) -> int:
    """批量插入文章 URL 到 Daily_Articles（使用 savepoint 隔离每条 insert 失败，支持自动重连）"""
    try:
        cur = db.cursor()
    except Exception:
        log.warning("DB connection lost before insert, reconnecting...")
        raise  # 让调用方处理重连

    now = datetime.now(timezone.utc).isoformat()
    count = 0
    skipped_null = 0
    db_broken = False
    for art in articles:
        url = art.get("articleUrl") or ""
        url = url.strip()
        headline = art.get("headline", "")
        ts = art.get("timestamp", "")

        # 跳过空 URL 的文章（如视频、音频等非文章内容）
        if not url:
            skipped_null += 1
            continue

        # 解析日期
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                year = dt.year
                month = dt.month
                date_str = dt.strftime("%Y-%m-%d")
            except:
                year = 0; month = 0; date_str = ""
        else:
            year = 0; month = 0; date_str = ""

        # 如果连接已断，跳过后续（由调用方重连后重试）
        if db_broken:
            continue

        # 使用 savepoint 隔离每条 insert，防止一条失败导致整个事务中止
        try:
            cur.execute("SAVEPOINT sp_article")
            cur.execute(
                """INSERT INTO Daily_Articles (Year, Month, Date, Article_Title, Article_URL, scrape_time)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (Date, Article_URL) DO NOTHING""",
                (year, month, date_str, headline, url, now)
            )
            inserted = cur.rowcount  # 必须在 RELEASE 前保存，RELEASE 会重置 rowcount
            cur.execute("RELEASE SAVEPOINT sp_article")
            if inserted > 0:
                count += 1
        except psycopg2.OperationalError as e:
            # 连接断开类错误，标记 db_broken，剩余文章跳过
            db_broken = True
            log.warning(f"DB connection broken during insert: {str(e)[:80]}")
            try:
                cur.execute("ROLLBACK TO SAVEPOINT sp_article")
            except:
                pass
        except Exception as e:
            # 回滚到 savepoint，继续处理下一条
            try:
                cur.execute("ROLLBACK TO SAVEPOINT sp_article")
            except:
                pass
            log.warning(f"Insert failed (skipped): {str(e)[:80]} | {url[:80] if url else 'None'}")

    if skipped_null > 0:
        log.info(f"  Skipped {skipped_null} articles with null URL (videos/audio/etc)")
    if db_broken:
        log.warning(f"  DB broken during insert, {count} inserted before failure, remaining skipped")
    try:
        db.commit()
    except Exception:
        pass  # 连接已断，commit 会失败，由调用方处理
    cur.close()
    return count


def set_progress(db, key: str, value: str):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO scrape_progress (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
        (key, value, value)
    )
    db.commit()
    cur.close()


def get_progress(db, key: str) -> Optional[str]:
    cur = db.cursor()
    cur.execute("SELECT value FROM scrape_progress WHERE key = %s", (key,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def get_pending_article_urls(db, limit: int = 5000) -> list[tuple]:
    """获取尚未采集正文的文章 URL"""
    cur = db.cursor()
    cur.execute("""
        SELECT d.Article_URL, d.Article_Title, d.Date
        FROM Daily_Articles d
        LEFT JOIN Article_Info a ON d.Article_URL = a.Art_URL
        WHERE a.Art_ID IS NULL
        ORDER BY d.Date ASC, d.id
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    return rows


def get_pending_count(db) -> int:
    """获取待采集文章总数"""
    cur = db.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM Daily_Articles d
        LEFT JOIN Article_Info a ON d.Article_URL = a.Art_URL
        WHERE a.Art_ID IS NULL
    """)
    cnt = cur.fetchone()[0]
    cur.close()
    return cnt


def mark_article_skipped(db, article_url: str, reason: str = "PAYWALLED"):
    """标记文章为已跳过（付费墙/无内容），避免下次重试"""
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()
    art_id = article_url.split("/")[-1].split("?")[0]
    if len(art_id) > 80:
        art_id = art_id[:80]
    try:
        cur.execute("SAVEPOINT sp_skip")
        cur.execute(
            """INSERT INTO Article_Info (Art_ID, Art_Title, Art_URL, Art_Text, scrape_time)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (Art_ID) DO NOTHING""",
            (art_id, reason, article_url, reason, now)
        )
        cur.execute("RELEASE SAVEPOINT sp_skip")
        db.commit()
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT sp_skip")
        except:
            pass
        db.rollback()
    finally:
        cur.close()


# ============================================================
# CDP 客户端
# ============================================================

class CDPClient:
    """单个 CDP Tab 的 WebSocket 客户端"""

    def __init__(self, ws_url: str, timeout: int = 30):
        self.ws: WebSocket = create_connection(ws_url, timeout=timeout)
        self._mid = 0

    def _next_id(self) -> int:
        self._mid += 1
        return self._mid

    def send_and_wait(self, method: str, params: dict = None, timeout: int = 20) -> Optional[dict]:
        cid = self._next_id()
        msg = {"id": cid, "method": method}
        if params:
            msg["params"] = params
        self.ws.send(json.dumps(msg))
        self.ws.settimeout(timeout)
        start = time.time()
        while time.time() - start < timeout:
            try:
                raw = self.ws.recv()
                resp = json.loads(raw)
                if resp.get("id") == cid:
                    return resp.get("result")
            except:
                pass
        return None

    def navigate(self, url: str, timeout: int = 30) -> bool:
        result = self.send_and_wait("Page.navigate", {"url": url}, timeout=timeout)
        if result and "errorText" in result:
            log.warning(f"Navigate error: {result.get('errorText')}")
            return False
        return True

    def evaluate(self, expression: str, timeout: int = 15) -> Optional[str]:
        result = self.send_and_wait("Runtime.evaluate", {
            "returnByValue": True,
            "expression": expression,
        }, timeout=timeout)
        if result and "result" in result:
            return result["result"].get("value")
        return None

    def reconnect(self, ws_url: str, timeout: int = 30):
        """重新连接 WebSocket"""
        try:
            self.ws.close()
        except:
            pass
        self.ws = create_connection(ws_url, timeout=timeout)

    def close(self):
        try:
            self.ws.close()
        except:
            pass


def get_or_create_page() -> tuple[Optional[str], str]:
    """获取或创建 CDP 页面，返回 (ws_url, page_url)"""
    try:
        r = httpx.get(f"{CDP_HOST}/json/list", timeout=5)
        pages = r.json()
        for p in pages:
            url = p.get("url", "")
            if p.get("type") == "page" and ("wsj.com" in url or "blank" in url or "newtab" in url or url == "about:blank"):
                return p["webSocketDebuggerUrl"], url
        # 创建新页面
        r2 = httpx.get(f"{CDP_HOST}/json/new?url=", timeout=5)
        new_page = r2.json()
        return new_page["webSocketDebuggerUrl"], "about:blank"
    except Exception as e:
        log.error(f"Failed to get CDP page: {e}")
        return None, ""


# ============================================================
# Phase 1: 采集文章 URL
# ============================================================

def collect_day_articles(client: CDPClient, year: int, month: int, day: int, retries: int = 3) -> list[dict]:
    """采集某一天的文章列表（轮询等待页面加载完成，带重试）"""
    url = f"{CHROME_WSJ_ARCHIVE}/{year}/{month:02d}/{day:02d}"
    date_str = f"{year}-{month:02d}-{day:02d}"
    # 用于验证 __NEXT_DATA__ 属于目标日期的 JS
    # 提取 __NEXT_DATA__ 中第一篇文章的日期来判断页面是否已更新
    verify_js = """(() => {
        const el = document.getElementById('__NEXT_DATA__');
        if (!el) return null;
        try {
            const nd = JSON.parse(el.textContent);
            const arts = nd.props.pageProps.newsArchiveArticles;
            if (!arts || arts.length === 0) return 'empty';
            // 检查第一篇文章的时间戳是否匹配目标日期
            const ts = arts[0].timestamp || '';
            return ts.substring(0, 10);
        } catch(e) { return null; }
    })()"""

    for attempt in range(retries):
        client.navigate(url)

        # 轮询等待页面加载到目标日期（最多等 max_wait 秒）
        max_wait = 8.0 + attempt * 4.0  # 第1次8秒，第2次12秒，第3次16秒
        poll_interval = 1.0
        elapsed = 0.0
        raw = None

        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval

            # 检查页面日期是否已更新
            page_date = client.evaluate(verify_js, timeout=5)
            if page_date == date_str:
                # 页面已加载到目标日期，提取完整数据
                js = "document.getElementById('__NEXT_DATA__') ? document.getElementById('__NEXT_DATA__').textContent : null"
                raw = client.evaluate(js, timeout=15)
                break
            elif page_date == 'empty':
                # 页面加载了但该日期没有文章
                log.info(f"  {date_str}: 0 articles (empty archive day)")
                return []
            # page_date 为 null 或其他日期 → 继续等待

        if raw:
            try:
                nd = json.loads(raw)
                articles = nd["props"]["pageProps"].get("newsArchiveArticles", [])
                log.info(f"  {date_str}: {len(articles)} articles (loaded in {elapsed:.0f}s)")
                return articles
            except (json.JSONDecodeError, KeyError) as e:
                log.warning(f"  Parse error for {date_str}: {e}")
                if attempt < retries - 1:
                    log.info(f"  Retry {attempt + 1}/{retries} for {date_str}...")
                    continue
                return []
        else:
            log.warning(f"No __NEXT_DATA__ for {date_str} after {max_wait:.0f}s (attempt {attempt + 1}/{retries})")
            if attempt < retries - 1:
                log.info(f"  Retry {attempt + 1}/{retries} for {date_str}...")
                continue

    log.warning(f"  Gave up on {date_str} after {retries} attempts")
    return []


def run_phase1(max_articles: int = None):
    """Phase 1: 采集所有日期的文章 URL，达到 max_articles 条后停止"""
    log.info("=" * 50)
    log.info(f"Phase 1: Collecting article URLs from archive (max_articles={max_articles or '∞'})")
    log.info("=" * 50)

    db = get_db()
    init_db()

    ws_url, _ = get_or_create_page()
    if not ws_url:
        log.error("No CDP page available. Is Chrome running with --remote-debugging-port=9222?")
        db.close()
        return

    client = CDPClient(ws_url, timeout=30)

    total_days = 0
    total_articles = 0
    skipped_days = 0
    consecutive_errors = 0

    def ensure_client():
        nonlocal client, ws_url, consecutive_errors
        if consecutive_errors >= 3:
            log.warning(f"{consecutive_errors} consecutive errors, reconnecting...")
            new_ws_url, _ = get_or_create_page()
            if new_ws_url:
                ws_url = new_ws_url
                try:
                    client.reconnect(ws_url)
                    log.info("Reconnected CDP client")
                    consecutive_errors = 0
                except Exception as e:
                    log.error(f"Reconnect failed: {e}")
                    consecutive_errors = 0

    import calendar
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            # 检查是否已完成该月
            progress_key = f"archive_month_{year}_{month:02d}"
            if get_progress(db, progress_key) == "done":
                # 仍需计入天数
                _, days_in_month = calendar.monthrange(year, month)
                skipped_days += days_in_month
                log.info(f"Skipping {year}-{month:02d} (already done)")
                continue

            _, days_in_month = calendar.monthrange(year, month)
            month_articles = 0

            for day in range(1, days_in_month + 1):
                # 判断是否超过当前日期（不采集未来日期）
                now = datetime.now()
                if year > now.year or (year == now.year and month > now.month) or \
                   (year == now.year and month == now.month and day > now.day):
                    continue

                try:
                    articles = collect_day_articles(client, year, month, day)
                    if articles:
                        db = ensure_db(db)
                        n = insert_articles(db, articles)
                        month_articles += n
                        total_articles += n
                        consecutive_errors = 0
                    else:
                        consecutive_errors += 1
                except psycopg2.OperationalError as e:
                    log.error(f"DB error on {year}-{month:02d}-{day:02d}: {e}")
                    db = ensure_db(db)
                    consecutive_errors += 1
                except Exception as e:
                    log.error(f"Error on {year}-{month:02d}-{day:02d}: {e}")
                    consecutive_errors += 1

                total_days += 1
                ensure_client()

                # 达到上限则退出
                if max_articles and total_articles >= max_articles:
                    log.info(f"Reached max_articles limit ({max_articles}), stopping Phase 1")
                    break

                # 每5天提交一次进度
                if day % 5 == 0:
                    try:
                        db = ensure_db(db)
                        db.commit()
                    except Exception as e:
                        log.warning(f"Commit failed: {e}, reconnecting...")
                        db = ensure_db(db)
                    log.info(f"  Progress: {year}-{month:02d} day {day}/{days_in_month}, "
                             f"total articles so far: {total_articles}")

                time.sleep(DAY_PAGE_DELAY)

            # 标记该月已完成
            db = ensure_db(db)
            set_progress(db, f"archive_month_{year}_{month:02d}", "done")
            log.info(f"Month {year}-{month:02d}: {month_articles} articles, "
                     f"running total: {total_articles} from {total_days} days")

            # 达到上限则退出月份循环
            if max_articles and total_articles >= max_articles:
                break

        # 达到上限则退出年份循环
        if max_articles and total_articles >= max_articles:
            break

    client.close()
    try:
        db = ensure_db(db)
        db.close()
    except:
        pass

    log.info(f"\nPhase 1 complete: {total_articles} articles from {total_days} days "
             f"({skipped_days} days skipped)")
    return total_articles


# ============================================================
# Phase 2: 采集文章正文
# ============================================================

def extract_article_body(client: CDPClient, article_url: str) -> Optional[dict]:
    """通过 CDP 提取文章正文（含人类行为模拟）"""
    # 1. 导航到文章页
    ok = client.navigate(article_url, timeout=15)
    if not ok:
        return {"error": "navigate failed"}

    # 2. 模拟人类阅读行为：随机等待 + 逐步滚动
    initial_wait = random.uniform(3.0, 6.0)
    time.sleep(initial_wait)

    # 3. 模拟滚动阅读（分步滚动，触发懒加载和正常人行为）
    scroll_js = """
    (function() {
        var steps = %d;
        var delay = %d;
        var count = 0;
        function scrollOne() {
            if (count >= steps) return;
            var y = (count + 1) * (document.body.scrollHeight / (steps + 1));
            window.scrollTo({top: y, behavior: 'smooth'});
            count++;
            if (count < steps) setTimeout(scrollOne, delay);
        }
        scrollOne();
    })()
    """ % (random.randint(2, 4), random.randint(400, 800))
    client.evaluate(scroll_js, timeout=5)
    time.sleep(random.uniform(1.5, 3.0))

    # 4. 提取正文
    js = """
    (function() {
        // 标题：优先 meta og:title，其次 document.title，最后 H1
        var title = '';
        var metaOg = document.querySelector('meta[property="og:title"]');
        if (metaOg) title = metaOg.getAttribute('content') || '';
        if (!title) title = document.title || '';
        if (!title) {
            var h1 = document.querySelector('h1');
            if (h1) title = h1.innerText.trim();
        }

        // 作者：优先 meta author
        var author = '';
        var metaAuthor = document.querySelector('meta[name="author"]');
        if (metaAuthor) author = metaAuthor.getAttribute('content') || '';
        if (!author) {
            var authorLink = document.querySelector('a[href*="/author/"]');
            if (authorLink) author = authorLink.innerText.trim();
        }

        // 发布时间：meta article:published_time
        var pubTime = '';
        var metaTime = document.querySelector('meta[property="article:published_time"]');
        if (metaTime) pubTime = metaTime.getAttribute('content') || '';

        // 正文段落
        var paragraphs = [];
        var article = document.querySelector('article');
        if (article) {
            var ps = article.querySelectorAll('p');
            ps.forEach(function(p) {
                var text = p.innerText.trim();
                if (text.length > 20) paragraphs.push(text);
            });
        }

        // 检测付费墙：检查显式付费提示文本
        var bodyText = document.body ? document.body.innerText : '';
        var paywallEl = document.querySelector('[class*="paywall"], [id*="paywall"], [id*="cx-paywall"]');
        var paywallText = paywallEl ? (paywallEl.innerText || '') : '';
        // 显式付费墙信号
        var hasSubscribePrompt = bodyText.indexOf('Subscribe to continue') > -1
            || bodyText.indexOf('Sign in to continue') > -1
            || bodyText.indexOf('Continue reading your article') > -1
            || paywallText.indexOf('Subscribe') > -1
            || paywallText.indexOf('subscribe now') > -1;
        // 段落太少 + 付费提示 = 付费墙；段落太少但无提示 = 短文
        var isPaywalled = hasSubscribePrompt || (paragraphs.length < 3 && bodyText.indexOf('Subscribe') > -1);

        return JSON.stringify({
            title: title,
            author: author,
            pubTime: pubTime,
            paragraphs: paragraphs,
            text: paragraphs.join('\\n\\n'),
            wordCount: paragraphs.join(' ').split(/\\s+/).length,
            paywalled: isPaywalled,
            paragraphCount: paragraphs.length
        });
    })()
    """
    result = client.evaluate(js, timeout=10)
    if result:
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {"error": "json decode failed", "raw": str(result)[:200]}
    return None


def run_phase2(max_articles: int = None):
    """Phase 2: 采集文章正文"""
    log.info("=" * 50)
    log.info("Phase 2: Collecting article bodies")
    log.info("=" * 50)

    db = get_db()

    ws_url, _ = get_or_create_page()
    if not ws_url:
        log.error("No CDP page available")
        db.close()
        return

    client = CDPClient(ws_url, timeout=30)

    done = 0
    failed = 0
    skipped = 0
    paywalled = 0
    consecutive_errors = 0
    consecutive_paywalls = 0
    batch_size = 200
    start_time = time.time()
    PAYWALL_PAUSE_THRESHOLD = 10  # 连续10篇付费墙则暂停等待登录
    articles_since_homepage = 0  # 计数器：距上次回首页的文章数

    def refresh_homepage():
        """回 WSJ 首页模拟正常用户浏览，降低风控"""
        nonlocal articles_since_homepage
        client.navigate("https://www.wsj.com", timeout=15)
        time.sleep(random.uniform(3.0, 5.0))
        # 模拟在首页随便滚动一下
        client.evaluate(f"window.scrollTo({{top: {random.randint(200,800)}, behavior: 'smooth'}})", timeout=5)
        time.sleep(random.uniform(1.0, 2.0))
        articles_since_homepage = 0
        log.info("  Refreshed homepage to reset session fingerprint")

    def ensure_client():
        nonlocal client, ws_url, consecutive_errors
        if consecutive_errors >= 3:
            log.warning(f"{consecutive_errors} consecutive errors, reconnecting...")
            new_ws_url, _ = get_or_create_page()
            if new_ws_url:
                ws_url = new_ws_url
                try:
                    client.reconnect(ws_url)
                    log.info("Reconnected CDP client")
                    consecutive_errors = 0
                except Exception as e:
                    log.error(f"Reconnect failed: {e}")
                    consecutive_errors = 0

    while True:
        urls = get_pending_article_urls(db, limit=batch_size)
        if not urls:
            log.info("No more pending articles!")
            break

        for article_url, title, date_str in urls:
            if max_articles and done >= max_articles:
                break

            ensure_client()

            try:
                body = extract_article_body(client, article_url)
            except Exception as e:
                log.warning(f"Extract error: {e}")
                body = None
                consecutive_errors += 1

            if body and "error" not in body:
                # 检查付费墙
                if body.get("paywalled"):
                    paywalled += 1
                    consecutive_paywalls += 1
                    consecutive_errors = 0
                    # 标记为已跳过，避免下次重试
                    mark_article_skipped(db, article_url, "PAYWALLED")
                    if consecutive_paywalls == 1:
                        log.warning(f"  Paywalled: {title[:60]}")
                    if consecutive_paywalls >= PAYWALL_PAUSE_THRESHOLD:
                        log.error(f"{consecutive_paywalls} consecutive paywalled articles!")
                        log.error("Pausing 5 minutes. Consider re-logging in to WSJ.")
                        time.sleep(300)
                        consecutive_paywalls = 0
                    continue

                if not body.get("text"):
                    log.warning(f"  Skipped (empty body): {title[:60]}")
                    skipped += 1
                    continue
                cur = db.cursor()
                now = datetime.now(timezone.utc).isoformat()
                art_id = article_url.split("/")[-1].split("?")[0]
                if len(art_id) > 80:
                    art_id = art_id[:80]

                try:
                    cur.execute("SAVEPOINT sp_phase2")
                    cur.execute(
                        """INSERT INTO Article_Info (Art_ID, Art_Title, Art_Author, Art_Time, Art_URL, Art_Text, scrape_time)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (Art_ID) DO UPDATE SET
                           Art_Text = EXCLUDED.Art_Text, Art_Title = EXCLUDED.Art_Title,
                           Art_Author = EXCLUDED.Art_Author, scrape_time = EXCLUDED.scrape_time""",
                        (art_id, body.get("title", title), body.get("author", ""),
                         body.get("pubTime", ""), article_url, body.get("text", ""), now)
                    )
                    cur.execute("RELEASE SAVEPOINT sp_phase2")
                    db.commit()
                    done += 1
                    consecutive_errors = 0
                except Exception as e:
                    try:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_phase2")
                    except:
                        pass
                    log.error(f"DB insert error: {str(e)[:80]}")
                    db.rollback()
                    failed += 1
                finally:
                    cur.close()
            elif body and body.get("error"):
                log.warning(f"  Skipped (no article): {title[:60]}")
                skipped += 1
                consecutive_errors += 1
            else:
                log.warning(f"  Failed (no body): {title[:60]}")
                failed += 1
                consecutive_errors += 1

            # 进度报告 + ETA
            if done % 50 == 0 and done > 0:
                elapsed = time.time() - start_time
                rate = done / elapsed * 3600  # articles per hour
                remaining = get_pending_count(db)
                eta_h = (remaining / rate) if rate > 0 else 0
                set_progress(db, "phase2_done", str(done))
                log.info(f"  Progress: {done} done, {failed} failed, {skipped} skipped, {paywalled} paywalled | "
                         f"{rate:.0f}/hr | ~{remaining} left | ETA {eta_h:.1f}h")

            time.sleep(random.uniform(ARTICLE_DELAY_MIN, ARTICLE_DELAY_MAX))

            # 每N篇文章回首页一次，降低风控检测
            articles_since_homepage += 1
            if articles_since_homepage >= HOMEPAGE_REFRESH_EVERY:
                refresh_homepage()

        if max_articles and done >= max_articles:
            break

    set_progress(db, "phase2_done", str(done))
    client.close()
    try:
        db = ensure_db(db)
        db.close()
    except:
        pass
    elapsed = time.time() - start_time
    log.info(f"\nPhase 2 complete: {done} done, {failed} failed, {skipped} skipped, {paywalled} paywalled in {elapsed/3600:.1f}h")


# ============================================================
# 主入口
# ============================================================

def main():
    init_db()

    if len(sys.argv) < 2:
        print("""
WSJ Archive Collector

Usage:
  python archive_collector.py clear       清空数据库
  python archive_collector.py phase1      采集所有文章 URL（1997-2026）
  python archive_collector.py phase2      采集文章正文
  python archive_collector.py phase2 N    采集文章正文（最多N篇）
  python archive_collector.py stats       查看数据库统计
  python archive_collector.py all         从头开始：清空→Phase1→Phase2
""")
        return

    cmd = sys.argv[1]

    if cmd == "clear":
        clear_db()
    elif cmd == "phase1":
        max_n = int(sys.argv[2]) if len(sys.argv) > 2 else None
        run_phase1(max_n)
    elif cmd == "phase2":
        max_n = int(sys.argv[2]) if len(sys.argv) > 2 else None
        run_phase2(max_n)
    elif cmd == "stats":
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM Daily_Articles"); da = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM Article_Info"); ai = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT Date) FROM Daily_Articles"); days = cur.fetchone()[0]
        cur.execute("SELECT MIN(Date), MAX(Date) FROM Daily_Articles"); dr = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM scrape_progress"); sp = cur.fetchone()[0]
        print(f"Daily_Articles: {da} rows ({days} unique dates, range: {dr[0]} ~ {dr[1]})")
        print(f"Article_Info (with body): {ai} rows")
        print(f"scrape_progress: {sp} entries")
        cur.close(); db.close()
    elif cmd == "all":
        clear_db()
        run_phase1()
        run_phase2()
    else:
        print(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
