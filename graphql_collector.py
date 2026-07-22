"""WSJ APP GraphQL 正文采集器

通过 WSJ APP 的 GraphQL API 采集文章正文，无需浏览器/CDP。
使用 Client JWT（从 wsj.com/client 获取，有效期 2 天）进行认证。

流程:
  1. 从 Daily_Articles 读取待采集 URL
  2. ArticleMetaByUrl (无 auth) → 获取 originId
  3. ArticleContent (需 Client JWT) → 获取正文
  4. 写入 Article_Info

用法:
  python graphql_collector.py          # 全量采集（从旧到新）
  python graphql_collector.py test     # 采集 10 篇测试
  python graphql_collector.py N        # 采集 N 篇
"""

import json
import os
import time
import random
import logging
import sys
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import psycopg2
import psycopg2.extras
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# 配置
# ============================================================
from config import PG_CONFIG

# GraphQL 端点
GQL_ENDPOINT = "https://shared-data.dowjones.io/gateway/graphql"

# Apollo Persisted Query Hashes
HASH_ARTICLE_META_BY_URL = "e36182f1af30342ab15b1cb80595a91da68b0fa62365a5d98781e7b2cb6f4843"
HASH_ARTICLE_CONTENT = "e03e6948cd028f43cbeac977eb337f719cdd82678e124fef2afec86d9d02d2e7"

# 请求头模板
BASE_HEADERS = {
    "accept": "application/json",
    "user-agent": "okhttp/4.12.0",
    "apollographql-client-name": "wsj-reader",
    "content-type": "application/json",
}

# 采集配置
REQUEST_DELAY_MIN = 0.5   # 请求最小间隔（秒）
REQUEST_DELAY_MAX = 2.0   # 请求最大间隔（秒）
BATCH_SIZE = 500          # 每批从 DB 取的文章数
SAVE_INTERVAL = 50        # 每 N 篇文章提交一次 DB
MAX_RETRIES = 3           # 单个文章最大重试次数
RATE_LIMIT_COOLDOWN = 60  # 遇到 429 限流后的冷却时间（秒）
MAX_WORKERS = 4           # 多线程默认线程数

# Token 文件（存储 Client JWT，过期时自动重新登录获取）
TOKEN_FILE = Path(__file__).parent / "client_jwt.txt"

# 日志
LOG_FILE = Path(__file__).parent / "graphql_collector.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("graphql_collector")


# ============================================================
# Token 管理
# ============================================================

def _load_credentials() -> tuple:
    """从 .env 文件加载 WSJ 账号密码"""
    env_file = Path(__file__).parent / ".env"
    user = os.environ.get("WSJ_USER", "")
    password = os.environ.get("WSJ_PASS", "")
    if (not user or not password) and env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("WSJ_USER=") and not user:
                user = line.split("=", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("WSJ_PASS=") and not password:
                password = line.split("=", 1)[1].strip().strip('"').strip("'")
    return user, password


def load_client_jwt(auto_login: bool = True) -> Optional[str]:
    """从文件加载 Client JWT，过期时尝试自动登录"""
    if TOKEN_FILE.exists():
        jwt = TOKEN_FILE.read_text().strip()
        if jwt:
            # 检查是否过期（简单检查：解码 payload 看 exp）
            try:
                parts = jwt.split(".")
                if len(parts) == 3:
                    pay = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
                    payload = json.loads(base64.urlsafe_b64decode(pay))
                    exp = payload.get("exp", 0)
                    if exp < time.time():
                        log.warning(f"Client JWT expired at {datetime.fromtimestamp(exp)}")
                    else:
                        log.info(f"Client JWT loaded, expires: {datetime.fromtimestamp(exp)}")
                        return jwt
            except Exception:
                pass
            # JWT 可能过期或损坏，如果有密码尝试自动登录
            if auto_login:
                user, password = _load_credentials()
                if user and password:
                    log.info("Client JWT expired/missing, attempting auto-login...")
                    try:
                        from login_flow import login as sso_login
                        new_jwt = sso_login(user, password)
                        if new_jwt:
                            return new_jwt
                    except Exception as e:
                        log.warning(f"Auto-login failed: {e}")
            return None
    # 无文件，尝试自动登录
    if auto_login:
        user, password = _load_credentials()
        if user and password:
            log.info("No Client JWT file found, attempting auto-login...")
            try:
                from login_flow import login as sso_login
                new_jwt = sso_login(user, password)
                if new_jwt:
                    return new_jwt
            except Exception as e:
                log.warning(f"Auto-login failed: {e}")
    return None


def save_client_jwt(jwt: str):
    """保存 Client JWT 到文件"""
    TOKEN_FILE.write_text(jwt)
    log.info("Client JWT saved to file")


# ============================================================
# 数据库
# ============================================================

def get_db():
    return psycopg2.connect(**PG_CONFIG)


def get_pending_urls(db, limit: int = BATCH_SIZE) -> list[tuple]:
    """获取尚未采集正文的文章 URL（从旧到新）"""
    cur = db.cursor()
    cur.execute("""
        SELECT d.article_url, d.article_title, d.date
        FROM daily_articles d
        LEFT JOIN article_info a ON d.article_url = a.art_url
        WHERE a.art_id IS NULL
        ORDER BY d.date ASC, d.id
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    return rows


def get_pending_count(db) -> int:
    cur = db.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM daily_articles d
        LEFT JOIN article_info a ON d.article_url = a.art_url
        WHERE a.art_id IS NULL
    """)
    cnt = cur.fetchone()[0]
    cur.close()
    return cnt


def save_article_body(db, article_url: str, origin_id: str, title: str,
                       author: str, pub_time: str, title_short: str,
                       tag1: str, tag2: str,
                       body_json: str, body_text: str, word_count: int) -> bool:
    """保存文章正文到 Article_Info"""
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()
    art_id = origin_id  # 使用 originId 作为主键

    try:
        cur.execute("SAVEPOINT sp_save")
        cur.execute("""
            INSERT INTO Article_Info
                (art_id, art_title, art_title_short, art_author, art_time,
                 art_tag_1, art_tag_2, art_url, art_text, art_text_html, scrape_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (art_id) DO UPDATE SET
                art_title = EXCLUDED.art_title,
                art_title_short = EXCLUDED.art_title_short,
                art_author = EXCLUDED.art_author,
                art_time = EXCLUDED.art_time,
                art_tag_1 = EXCLUDED.art_tag_1,
                art_tag_2 = EXCLUDED.art_tag_2,
                art_text = EXCLUDED.art_text,
                art_text_html = EXCLUDED.art_text_html,
                scrape_time = EXCLUDED.scrape_time
        """, (art_id, title, title_short, author, pub_time,
              tag1, tag2, article_url, body_text, body_json, now))
        # 从 comment_info 同步 comments_count，防止因 article_info 重建导致计数丢失
        cur.execute("""
            UPDATE Article_Info
            SET comments_count = (SELECT COUNT(*) FROM comment_info WHERE article_id = %s)
            WHERE art_id = %s
        """, (art_id, art_id))
        cur.execute("RELEASE SAVEPOINT sp_save")
        db.commit()
        return True
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT sp_save")
        except:
            pass
        db.rollback()
        log.error(f"DB save error: {e}")
        return False
    finally:
        cur.close()


def mark_article_failed(db, article_url: str, reason: str):
    """标记文章为采集失败，避免无限重试"""
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()
    art_id = article_url.split("/")[-1].split("?")[0][:80]
    log.info(f"  -> FAILED:{reason} for {article_url[:80]}")
    try:
        cur.execute("SAVEPOINT sp_fail")
        cur.execute("""
            INSERT INTO Article_Info (art_id, art_title, art_url, art_text, scrape_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (art_id) DO UPDATE SET
                art_text = EXCLUDED.art_text,
                scrape_time = EXCLUDED.scrape_time
        """, (art_id, f"FAILED:{reason}", article_url, f"FAILED:{reason}", now))
        cur.execute("RELEASE SAVEPOINT sp_fail")
        db.commit()
    except Exception as e:
        log.warning(f"mark_article_failed error for {article_url[:60]}: {e}")
        try:
            cur.execute("ROLLBACK TO SAVEPOINT sp_fail")
        except:
            pass
        db.rollback()
    finally:
        cur.close()


# ============================================================
# GraphQL API
# ============================================================

def url_to_origin_id(session: httpx.Client, article_url: str) -> Optional[str]:
    """通过 ArticleMetaByUrl 将 WSJ URL 转换为 originId（无需 auth）"""
    # API 只识别 https:// URL，数据库中有 http:// 的需要转换
    lookup_url = article_url.replace("http://", "https://")
    headers = BASE_HEADERS.copy()
    headers["x-apollo-operation-id"] = HASH_ARTICLE_META_BY_URL
    headers["x-apollo-operation-name"] = "ArticleMetaByUrl"

    try:
        r = session.get(GQL_ENDPOINT, params={
            "operationName": "ArticleMetaByUrl",
            "variables": json.dumps({"url": lookup_url}),
            "extensions": json.dumps({
                "persistedQuery": {"version": 1, "sha256Hash": HASH_ARTICLE_META_BY_URL}
            })
        }, headers=headers, timeout=30)

        if r.status_code != 200:
            return None

        data = r.json()
        article_by_url = data.get("data", {}).get("articleByUrl", {})
        article = article_by_url.get("article", {})
        # 优先从 article 中取，fallback 到 articleByUrl 层级
        return article.get("originId") or article_by_url.get("originId")
    except Exception as e:
        log.debug(f"MetaByUrl error for {article_url[:80]}: {e}")
        return None


def fetch_article_body(session: httpx.Client, origin_id: str, client_jwt: str) -> Optional[dict]:
    """通过 ArticleContent 获取文章正文（需要 Client JWT）"""
    headers = BASE_HEADERS.copy()
    headers["authorization"] = f"Bearer {client_jwt}"
    headers["x-apollo-operation-id"] = HASH_ARTICLE_CONTENT
    headers["x-apollo-operation-name"] = "ArticleContent"

    try:
        r = session.get(GQL_ENDPOINT, params={
            "operationName": "ArticleContent",
            "variables": json.dumps({"id": origin_id, "idType": "originid"}),
            "extensions": json.dumps({
                "persistedQuery": {"version": 1, "sha256Hash": HASH_ARTICLE_CONTENT}
            })
        }, headers=headers, timeout=30)

        if r.status_code == 401:
            return {"error": "auth_expired"}
        if r.status_code == 429:
            return {"error": "rate_limited"}
        if r.status_code != 200:
            return {"error": f"http_{r.status_code}"}

        data = r.json()
        article = data.get("data", {}).get("articleContent", {})

        if not article:
            return {"error": "empty_response"}

        # 提取正文
        result = {
            "originId": article.get("originId", origin_id),
            "title": (article.get("articleHeadline") or {}).get("flattened", {}).get("text", ""),
            "author": "",
            "title_short": "",
            "tag1": "",
            "tag2": "",
            "pubTime": "",
            "body_json": json.dumps(article, ensure_ascii=False),
            "body_items": len(article.get("articleBody", [])),
        }

        # --- 简短描述: standFirst.flattened.text ---
        standfirst = article.get("standFirst") or {}
        if isinstance(standfirst, dict):
            result["title_short"] = (standfirst.get("textAndDecorations") or {}).get("flattened", {}).get("text", "")
            if not result["title_short"]:
                result["title_short"] = standfirst.get("flattened", {}).get("text", "")

        # --- 标签: sectionName / sectionType ---
        result["tag1"] = (article.get("sectionName") or "").upper()
        result["tag2"] = (article.get("sectionType") or "").upper()

        # --- 作者: 优先顶层 authors 数组，其次 articleByline.authors ---
        authors_list = article.get("authors") or []
        if not authors_list:
            byline = article.get("articleByline") or {}
            authors_list = byline.get("authors") or []
        if authors_list:
            author_names = []
            for a in authors_list:
                if isinstance(a, dict):
                    name = a.get("text", "") or a.get("formattedName", "") or a.get("name", "")
                    if name:
                        author_names.append(name)
            result["author"] = ", ".join(author_names)

        # --- 发布时间: publishedDateTimeUtc → 格式化 ---
        pub_utc = article.get("publishedDateTimeUtc", "") or article.get("updatedDateTimeUtc", "")
        if pub_utc:
            result["pubTime"] = _format_utc_to_et(pub_utc)
        if not result["pubTime"]:
            # fallback: articleTrackingMeta.articlePublish
            tracking = article.get("articleTrackingMeta") or {}
            result["pubTime"] = tracking.get("articlePublish", "")

        # 从 body 中提取文本
        body_items = article.get("articleBody", [])
        paragraphs = []
        has_paragraph = False  # 是否有真正的文本段落
        for item in body_items:
            if item is None:
                continue
            typename = item.get("__typename", "")
            
            # 段落类型
            if typename in ("ParagraphArticleBody",):
                text = (item.get("textAndDecorations") or {}).get("flattened", {}).get("text", "")
                if text.strip():
                    paragraphs.append(text.strip())
                    has_paragraph = True
            
            # 标题类型（提取作者/时间）
            elif typename in ("Heading2ArticleBody", "Heading1ArticleBody"):
                text = (item.get("text") or {}).get("flattened", {}).get("text", "")
                if text.strip():
                    paragraphs.append(text.strip())
            
            # Pull quote
            elif typename == "PullQuoteArticleBody":
                text = (item.get("quote") or {}).get("flattened", {}).get("text", "")
                attribution = (item.get("attribution") or {}).get("flattened", {}).get("text", "")
                if text.strip():
                    paragraphs.append(f'"{text.strip()}" — {attribution.strip()}')
            
            # 图片说明
            elif typename == "ImageArticleBody":
                caption = item.get("caption")
                credit = item.get("credit")
                if isinstance(caption, dict):
                    caption = (caption or {}).get("flattened", {}).get("text", "")
                elif not isinstance(caption, str):
                    caption = ""
                if isinstance(credit, dict):
                    credit = (credit or {}).get("flattened", {}).get("text", "")
                elif not isinstance(credit, str):
                    credit = ""
                if caption.strip():
                    paragraphs.append(f"[Image: {caption.strip()}]")
            
            # 图集 / 视频 / 其他非文本类型
            elif typename in ("GalleryArticleBody", "VideoArticleBody", "SlideshowArticleBody"):
                pass  # 这些类型没有文本内容

        result["text"] = "\n\n".join(paragraphs)
        result["word_count"] = len(result["text"].split()) if result["text"] else 0
        result["has_paragraph"] = has_paragraph

        return result

    except httpx.TimeoutException:
        return {"error": "timeout"}
    except Exception as e:
        import traceback
        log.warning(f"ArticleContent error for {origin_id}: {e}\n{traceback.format_exc()}")
        return {"error": str(e)[:100]}


def _format_utc_to_et(utc_str: str) -> str:
    """将 UTC ISO 时间字符串转换为美东时间格式化字符串
    例: '2016-01-07T14:08:00Z' → 'Updated Jan. 7, 2016 2:38 pm ET'
    """
    try:
        from datetime import timedelta, timezone as dt_timezone
        # 解析 UTC 时间
        if utc_str.endswith("Z"):
            utc_str = utc_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(utc_str)
        # 转换到美东 (UTC-5, 简化处理，不考虑夏令时)
        et = dt - timedelta(hours=5)
        return f"Updated {et.strftime('%b. %d, %Y %I:%M %p').replace(' 0', ' ')} ET"
    except Exception:
        return utc_str


# ============================================================
# 多线程支持
# ============================================================

class CollectorState:
    """线程安全的采集状态计数器"""
    def __init__(self):
        self.lock = threading.Lock()
        self.done = 0
        self.run_done = 0
        self.failed = 0
        self.skipped = 0
        self.meta_failed = 0
        self.rate_limit_hits = 0
        self.auth_expired = False

    def inc_done(self):
        with self.lock:
            self.done += 1
            self.run_done += 1

    def inc_failed(self):
        with self.lock:
            self.failed += 1

    def inc_skipped(self):
        with self.lock:
            self.skipped += 1

    def inc_meta_failed(self):
        with self.lock:
            self.meta_failed += 1

    def inc_rate_limit(self):
        with self.lock:
            self.rate_limit_hits += 1

    def set_auth_expired(self):
        with self.lock:
            self.auth_expired = True

    def is_auth_expired(self):
        with self.lock:
            return self.auth_expired


# 线程局部存储：每个线程独立的 httpx.Client 和 DB 连接
_thread_local = threading.local()


def _get_thread_session() -> httpx.Client:
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = httpx.Client(
            timeout=30,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            http2=True,
        )
    return _thread_local.session


def _get_thread_db():
    if not hasattr(_thread_local, 'db'):
        _thread_local.db = get_db()
    return _thread_local.db


def _process_one_article(article_url: str, title_hint: str,
                          client_jwt: str, state: CollectorState) -> str:
    """处理单篇文章（线程安全），返回状态字符串"""
    session = _get_thread_session()
    db = _get_thread_db()

    retries = 0
    success = False
    origin_id = None

    while retries < MAX_RETRIES and not success:
        if state.is_auth_expired():
            return "auth_expired"

        try:
            # Step 1: URL → originId (支持重试)
            if origin_id is None:
                origin_id = url_to_origin_id(session, article_url)
                if not origin_id:
                    retries += 1
                    if retries >= MAX_RETRIES:
                        state.inc_meta_failed()
                        state.inc_skipped()
                        mark_article_failed(db, article_url, "NO_ORIGIN_ID")
                        return "no_origin_id"
                    else:
                        log.warning(f"MetaByUrl failed for {article_url[:80]}, retry {retries}/{MAX_RETRIES}")
                        time.sleep(random.uniform(1, 3))
                    continue

            # Step 2: originId → body
            body = fetch_article_body(session, origin_id, client_jwt)

            if body is None:
                retries += 1
                log.warning(f"fetch_article_body returned None for {origin_id}")
                continue

            if "error" in body:
                err = body["error"]
                if err == "auth_expired":
                    log.error("JWT expired! Need to refresh token.")
                    state.set_auth_expired()
                    return "auth_expired"
                elif err == "rate_limited":
                    state.inc_rate_limit()
                    log.warning(f"Rate limited (total hits: {state.rate_limit_hits}), cooling down...")
                    time.sleep(RATE_LIMIT_COOLDOWN)
                    retries += 1
                    continue
                elif err == "empty_response":
                    state.inc_skipped()
                    mark_article_failed(db, article_url, "EMPTY_RESPONSE")
                    return "empty_response"
                else:
                    log.warning(f"ArticleContent error for {origin_id}: {err}")
                    retries += 1
                    continue

            # Step 3: 检查是否有实际文本内容
            has_paragraph = body.get("has_paragraph", False)
            text_len = len(body.get("text", ""))
            if not has_paragraph and text_len < 100:
                article_type = "NON_TEXT"
                if body.get("body_items", 0) > 0:
                    article_type = "GALLERY_OR_VIDEO"
                mark_article_failed(db, article_url, article_type)
                state.inc_skipped()
                return article_type.lower()

            # Step 4: 保存
            ok = save_article_body(
                db, article_url, origin_id,
                body.get("title", title_hint),
                body.get("author", ""),
                body.get("pubTime", ""),
                body.get("title_short", ""),
                body.get("tag1", ""),
                body.get("tag2", ""),
                body.get("body_json", ""),
                body.get("text", ""),
                body.get("word_count", 0),
            )

            if ok:
                state.inc_done()
                return "ok"
            else:
                log.warning(f"save_article_body failed for {origin_id} (retry {retries+1})")
                retries += 1

        except Exception as e:
            retries += 1
            log.warning(f"Error for {article_url[:60]}: {e}")

    if not success and not state.is_auth_expired():
        state.inc_failed()
        mark_article_failed(db, article_url, "MAX_RETRIES")
        return "max_retries"

    return "unknown"


# ============================================================
# 主采集逻辑
# ============================================================

def run_collector(max_articles: int = None, num_workers: int = MAX_WORKERS):
    """主采集循环（多线程）"""
    log.info("=" * 60)
    log.info(f"WSJ GraphQL Body Collector ({num_workers} workers)")
    log.info("=" * 60)

    # 1. 加载 Client JWT
    client_jwt = load_client_jwt()
    if not client_jwt:
        log.error("No valid Client JWT found!")
        log.error("Options:")
        log.error(f"  1. Set WSJ_USER/WSJ_PASS in .env for auto-login")
        log.error(f"  2. Run: python login_flow.py -u EMAIL -p PASSWORD")
        log.error(f"  3. Save JWT manually to: {TOKEN_FILE}")
        return

    # 2. 连接数据库（主线程用）
    try:
        db = get_db()
        pending = get_pending_count(db)
        log.info(f"Database connected. Pending articles: {pending:,}")
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        return

    # 3. 线程安全的计数器
    state = CollectorState()
    start_time = time.time()

    # 断点续传：检查上次进度
    try:
        cur = db.cursor()
        cur.execute("SELECT value FROM scrape_progress WHERE key = 'graphql_done'")
        row = cur.fetchone()
        if row:
            state.done = int(row[0])
            log.info(f"Resuming from {state.done} articles")
        cur.close()
    except:
        pass

    # 清理之前因 http→https 导致的 FAILED:NO_ORIGIN_ID 记录，以便重新采集
    try:
        cur = db.cursor()
        cur.execute("DELETE FROM article_info WHERE art_text = 'FAILED:NO_ORIGIN_ID'")
        cleared = cur.rowcount
        if cleared > 0:
            db.commit()
            log.info(f"Cleared {cleared} FAILED:NO_ORIGIN_ID records for re-collection")
        cur.close()
    except Exception as e:
        log.warning(f"Failed to clear NO_ORIGIN_ID records: {e}")

    log.info(f"Starting collection (max: {max_articles or 'unlimited'}, workers: {num_workers})...")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        while True:
            # 检查是否达到上限
            if max_articles and state.run_done >= max_articles:
                log.info(f"Reached max limit: {max_articles}")
                break

            if state.is_auth_expired():
                log.error("Auth expired, stopping...")
                break

            # 取一批待采集 URL
            urls = get_pending_urls(db, limit=BATCH_SIZE)
            if not urls:
                log.info("No more pending articles!")
                break

            # 提交本批所有 URL 到线程池
            futures = {}
            submit_count = 0
            for article_url, title_hint, date_str in urls:
                if max_articles and state.run_done + submit_count >= max_articles:
                    break
                if state.is_auth_expired():
                    break

                f = executor.submit(
                    _process_one_article,
                    article_url, title_hint, client_jwt, state
                )
                futures[f] = article_url
                submit_count += 1

            if not futures:
                continue

            # 等待本批完成
            for f in as_completed(futures):
                article_url = futures[f]
                try:
                    result = f.result(timeout=120)
                except Exception as e:
                    log.error(f"Worker exception for {article_url[:60]}: {e}")

                # 达到上限时取消剩余任务
                if max_articles and state.run_done >= max_articles:
                    for ff in futures:
                        if not ff.done():
                            ff.cancel()
                    break

                if state.is_auth_expired():
                    for ff in futures:
                        if not ff.done():
                            ff.cancel()
                    break

            # 每批提交 DB 并报告进度
            db.commit()
            elapsed = time.time() - start_time
            rate = state.done / elapsed * 3600 if elapsed > 0 else 0
            remaining = get_pending_count(db)
            eta_h = (remaining / rate) if rate > 0 else 0
            log.info(f"Progress: {state.done:,} done | {state.failed} failed | "
                     f"{state.skipped} skipped | {state.meta_failed} meta_fail | "
                     f"{rate:.0f}/hr | ~{remaining:,} left | ETA {eta_h:.1f}h")

            # 保存进度
            try:
                cur = db.cursor()
                cur.execute("""
                    INSERT INTO scrape_progress (key, value)
                    VALUES ('graphql_done', %s)
                    ON CONFLICT (key) DO UPDATE SET value = %s
                """, (str(state.done), str(state.done)))
                db.commit()
                cur.close()
            except:
                pass

    # 清理
    db.commit()
    db.close()

    elapsed = time.time() - start_time
    log.info(f"\n{'='*60}")
    log.info(f"Collection finished!")
    log.info(f"  Done: {state.done:,}")
    log.info(f"  Failed: {state.failed}")
    log.info(f"  Skipped: {state.skipped}")
    log.info(f"  MetaByUrl failures: {state.meta_failed}")
    log.info(f"  Rate limit hits: {state.rate_limit_hits}")
    log.info(f"  Time: {elapsed/3600:.1f}h")
    if state.done > 0:
        log.info(f"  Avg rate: {state.done/elapsed*3600:.0f}/hr")
    log.info(f"{'='*60}")


# ============================================================
# 主入口
# ============================================================

def main():
    # 解析 --workers N 或 --workers=N 参数
    num_workers = MAX_WORKERS
    raw_args = sys.argv[1:]
    args = []
    skip_next = False
    for i, a in enumerate(raw_args):
        if skip_next:
            skip_next = False
            continue
        if a == '--workers':
            if i + 1 < len(raw_args):
                try:
                    num_workers = int(raw_args[i + 1])
                    skip_next = True
                except ValueError:
                    pass
            continue
        elif a.startswith('--workers='):
            try:
                num_workers = int(a.split('=', 1)[1])
            except ValueError:
                pass
            continue
        args.append(a)

    if len(args) < 1:
        run_collector(num_workers=num_workers)
    elif args[0] == "test":
        log.info(f"Running test mode (10 articles, {num_workers} workers)...")
        run_collector(max_articles=10, num_workers=num_workers)
    elif args[0] == "stats":
        try:
            db = get_db()
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM daily_articles")
            da = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM article_info")
            ai = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM article_info
                WHERE art_text IS NOT NULL
                AND art_text NOT LIKE 'FAILED:%'
                AND art_text != 'PAYWALLED'
            """)
            with_body = cur.fetchone()[0]
            pending = da - ai
            print(f"daily_articles: {da:,}")
            print(f"article_info (total): {ai:,}")
            print(f"article_info (with body): {with_body:,}")
            print(f"Pending: {pending:,}")
            cur.close()
            db.close()
        except Exception as e:
            print(f"Error: {e}")
    else:
        try:
            n = int(args[0])
            run_collector(max_articles=n, num_workers=num_workers)
        except ValueError:
            print(f"Usage: python graphql_collector.py [test|stats|N] [--workers N]")


if __name__ == "__main__":
    main()
