"""
WSJ APP Spot.im 评论采集器

通过 mobile-gw.spot.im/conversation/read API 采集文章评论。
使用 APP 抓包中的 Bearer JWT 认证（无需浏览器/CDP）。

API: POST https://mobile-gw.spot.im/conversation/read
Auth: Bearer {spotim_jwt} + x-post-id + x-spot-id

用法:
  python comment_collector.py          # 测试模式：采集 5 篇
  python comment_collector.py N        # 采集 N 篇
  python comment_collector.py all      # 全量采集（需在服务器上跑）
"""

import json
import os
import time
import random
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import psycopg2

# ============================================================
# 配置
# ============================================================
from config import PG_CONFIG

# Spot.im APP API
SPOTIM_BASE = "https://mobile-gw.spot.im"
SPOTIM_CONVERSATION_URL = f"{SPOTIM_BASE}/conversation/read"
SPOTIM_RANK_USERS_URL = f"{SPOTIM_BASE}/conversation/rank/message/users"
SPOT_ID = "sp_92LbaOI5"

# Token 文件
TOKEN_FILE = Path(__file__).parent / "spotim_jwt.txt"

# 请求头模板（APP User-Agent）
BASE_HEADERS = {
    "accept": "application/json",
    "user-agent": "wsj-version-6.18.1.1-code-61801001-android-32",
    "content-type": "application/json; charset=utf-8",
    "x-spot-id": SPOT_ID,
}

# 采集配置
REQUEST_DELAY_MIN = 0.5
REQUEST_DELAY_MAX = 2.0
BATCH_SIZE = 100
SAVE_INTERVAL = 10
MAX_RETRIES = 3
COMMENT_PAGE_SIZE = 50  # 每页评论数

# 日志
LOG_FILE = Path(__file__).parent / "comment_collector.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("comment_collector")


# ============================================================
# Token 管理
# ============================================================

def load_spotim_jwt() -> Optional[str]:
    """加载 Spot.im Bearer JWT"""
    if TOKEN_FILE.exists():
        jwt = TOKEN_FILE.read_text().strip()
        if jwt:
            try:
                import base64
                parts = jwt.split(".")
                if len(parts) == 3:
                    pay = parts[1] + "=" * (4 - len(parts[1]) % 4)
                    payload = json.loads(base64.b64decode(pay))
                    exp = payload.get("exp", 0)
                    if exp < time.time():
                        log.warning(f"Spot.im JWT expired at {datetime.fromtimestamp(exp)}")
                        return None
                    log.info(f"Spot.im JWT loaded, expires: {datetime.fromtimestamp(exp)}")
            except Exception:
                pass
            return jwt
    return None


# ============================================================
# 数据库
# ============================================================

def get_db():
    return psycopg2.connect(**PG_CONFIG)


def get_articles_for_comments(db, limit: int):
    """获取有正文但尚未采集评论的文章（只选有 originId 的）"""
    cur = db.cursor()
    cur.execute("""
        SELECT a.Art_ID, a.Art_Title, a.Art_URL
        FROM Article_Info a
        WHERE a.Art_Text IS NOT NULL
          AND a.Art_Title NOT LIKE 'FAILED:%%'
          AND a.Art_ID IS NOT NULL
          AND a.Art_ID != ''
          AND (a.Art_ID LIKE 'WP-WSJ-%%' OR a.Art_ID LIKE 'SB%%')
        ORDER BY a.scrape_time ASC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    return rows


def get_comment_count_for_article(db, art_id: str) -> int:
    """查询某文章已采集的评论数"""
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM comment_info WHERE article_id = %s", (art_id,))
    cnt = cur.fetchone()[0]
    cur.close()
    return cnt


def save_comment(db, art_id: str, comment: dict) -> bool:
    """保存单条评论到 comment_info

    comment dict keys:
        comment_id, parent_id, text, time, likes,
        user_id, user_name,
        like_user_ids (optional), like_user_names (optional)
    """
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()

    try:
        cur.execute("SAVEPOINT sp_cmt")
        cur.execute("""
            INSERT INTO comment_info
                (article_id, comment_id, reply_2_comment_id, comment_text,
                 comment_time, cmt_likes_count, user_id, user_nm,
                 cmt_likes_user_id, cmt_likes_user_nm,
                 scrape_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_id, comment_id) DO UPDATE SET
                comment_text = EXCLUDED.comment_text,
                cmt_likes_count = EXCLUDED.cmt_likes_count,
                cmt_likes_user_id = EXCLUDED.cmt_likes_user_id,
                cmt_likes_user_nm = EXCLUDED.cmt_likes_user_nm,
                scrape_time = EXCLUDED.scrape_time
        """, (
            art_id,
            comment["comment_id"],
            comment.get("parent_id"),
            comment["text"],
            comment["time"],
            comment["likes"],
            comment["user_id"],
            comment["user_name"],
            comment.get("like_user_ids") or [],
            comment.get("like_user_names") or [],
            now,
        ))
        cur.execute("RELEASE SAVEPOINT sp_cmt")
        return True
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT sp_cmt")
        except:
            pass
        db.rollback()
        log.warning(f"save_comment error: {e}")
        return False
    finally:
        cur.close()


def update_comments_count(db, art_id: str, count: int):
    """更新 article_info 中的评论数"""
    cur = db.cursor()
    try:
        cur.execute("UPDATE Article_Info SET Comments_Count = %s WHERE Art_ID = %s",
                    (count, art_id))
        db.commit()
    except Exception as e:
        db.rollback()
        log.warning(f"update_comments_count error: {e}")
    finally:
        cur.close()


def save_user_info(db, user_id: str, user_nm: str):
    """保存/更新用户信息到 user_info 表"""
    if not user_id or not user_nm:
        return
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()
    try:
        cur.execute("SAVEPOINT sp_ui")
        cur.execute("""
            INSERT INTO user_info (user_id, user_nm, user_posts, user_likes, user_url, scrape_time)
            VALUES (%s, %s, 0, 0, NULL, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                user_nm = EXCLUDED.user_nm,
                scrape_time = EXCLUDED.scrape_time
        """, (user_id, user_nm, now))
        cur.execute("RELEASE SAVEPOINT sp_ui")
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT sp_ui")
        except:
            pass
        log.debug(f"save_user_info skip: {e}")
    finally:
        cur.close()


def save_user_post(db, art_id: str, art_title: str, comment: dict):
    """保存用户发帖记录到 user_post_info 表"""
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()
    parent_id = comment.get("parent_id")

    # 查询父评论的用户信息
    rply2_uid = None
    rply2_unm = None
    if parent_id:
        try:
            cur.execute(
                "SELECT user_id, user_nm FROM comment_info WHERE article_id = %s AND comment_id = %s",
                (art_id, parent_id)
            )
            row = cur.fetchone()
            if row:
                rply2_uid, rply2_unm = row
        except:
            pass

    try:
        cur.execute("SAVEPOINT sp_up")
        cur.execute("""
            INSERT INTO user_post_info
                (user_id, user_nm, post_art_title, post_art_id,
                 post_text, post_rply, rply2_user_id, rply2_user_nm,
                 post_time, scrape_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, post_art_id, post_text, post_time) DO NOTHING
        """, (
            comment["user_id"],
            comment["user_name"],
            art_title,
            art_id,
            comment["text"],
            parent_id,
            rply2_uid,
            rply2_unm,
            comment.get("time"),
            now,
        ))
        cur.execute("RELEASE SAVEPOINT sp_up")
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT sp_up")
        except:
            pass
        log.debug(f"save_user_post skip: {e}")
    finally:
        cur.close()


def refresh_user_stats(db, user_ids: list = None):
    """从 comment_info 重新计算用户的 posts 和 likes 统计
    
    Args:
        user_ids: 指定用户ID列表，None 则刷新全部
    """
    cur = db.cursor()
    now = datetime.now(timezone.utc).isoformat()
    try:
        if user_ids:
            cur.execute("""
                INSERT INTO user_info (user_id, user_nm, user_posts, user_likes, user_url, scrape_time)
                SELECT user_id, MAX(user_nm), COUNT(*), COALESCE(SUM(cmt_likes_count), 0), NULL, %s
                FROM comment_info
                WHERE user_id = ANY(%s)
                GROUP BY user_id
                ON CONFLICT (user_id) DO UPDATE SET
                    user_nm = EXCLUDED.user_nm,
                    user_posts = EXCLUDED.user_posts,
                    user_likes = EXCLUDED.user_likes,
                    scrape_time = EXCLUDED.scrape_time
            """, (now, user_ids))
        else:
            cur.execute("""
                INSERT INTO user_info (user_id, user_nm, user_posts, user_likes, user_url, scrape_time)
                SELECT user_id, MAX(user_nm), COUNT(*), COALESCE(SUM(cmt_likes_count), 0), NULL, %s
                FROM comment_info
                GROUP BY user_id
                ON CONFLICT (user_id) DO UPDATE SET
                    user_nm = EXCLUDED.user_nm,
                    user_posts = EXCLUDED.user_posts,
                    user_likes = EXCLUDED.user_likes,
                    scrape_time = EXCLUDED.scrape_time
            """, (now,))
        db.commit()
    except Exception as e:
        db.rollback()
        log.warning(f"refresh_user_stats error: {e}")
    finally:
        cur.close()


# ============================================================
# Spot.im Conversation API
# ============================================================

def fetch_conversation_page(
    session: httpx.Client,
    post_id: str,
    jwt: str,
    offset: int = 0,
    count: int = COMMENT_PAGE_SIZE,
    sort_by: str = "newest",
) -> Optional[dict]:
    """获取一页评论

    Args:
        post_id: WSJ originId (e.g. WP-WSJ-0003749573)
        jwt: Spot.im Bearer JWT
        offset: 分页偏移
        count: 每页数量

    Returns:
        API 响应 JSON，或 None
    """
    headers = BASE_HEADERS.copy()
    headers["authorization"] = f"Bearer {jwt}"
    headers["x-post-id"] = post_id

    body = {
        "offset": offset,
        "count": count,
        "sort_by": sort_by,
        "extract_data": True,
        "depth": 2,
        "tab_id": "all",
        "with_star_rating": False,
        "sort_mapping": {"default": {"best": "likes_newest"}},
    }

    try:
        r = session.post(
            SPOTIM_CONVERSATION_URL,
            json=body,
            headers=headers,
            timeout=30,
        )

        if r.status_code == 401:
            log.warning("Spot.im JWT expired (401)")
            return {"error": "auth_expired"}
        if r.status_code == 429:
            return {"error": "rate_limited"}
        if r.status_code != 200:
            log.warning(f"Spot.im API HTTP {r.status_code}")
            return {"error": f"http_{r.status_code}"}

        return r.json()
    except Exception as e:
        log.warning(f"fetch_conversation_page error: {e}")
        return None


def fetch_comment_like_users(
    session: httpx.Client,
    comment_id: str,
    post_id: str,
    jwt: str,
    max_users: int = 200,
) -> tuple[list[str], list[str]]:
    """获取点赞了某评论的用户列表

    POST https://mobile-gw.spot.im/conversation/rank/message/users
    只需传 {"message_id": "<full_comment_id>"}，不需要 operation 参数。

    Args:
        session: httpx Client
        comment_id: 完整 comment ID (e.g. sp_92LbaOI5_WP-WSJ-xxx_c_xxx)
        post_id: WSJ article originId (用于 x-post-id header)
        jwt: Spot.im Bearer JWT
        max_users: 最多获取的点赞用户数

    Returns:
        (user_ids, user_names) 两个列表
    """
    headers = BASE_HEADERS.copy()
    headers["authorization"] = f"Bearer {jwt}"
    headers["x-post-id"] = post_id

    all_user_ids: list[str] = []
    all_user_names: list[str] = []
    offset = 0
    page_size = 100

    for _ in range(5):  # 最多 5 页
        body = {"message_id": comment_id, "count": page_size, "offset": offset}

        try:
            r = session.post(
                SPOTIM_RANK_USERS_URL,
                json=body,
                headers=headers,
                timeout=15,
            )
            if r.status_code == 429:
                log.warning(f"Rate limited fetching likes for {comment_id[:50]}")
                break
            if r.status_code != 200:
                log.warning(f"Like users API HTTP {r.status_code} for {comment_id[:50]}")
                break

            data = r.json()
            users = data.get("Users", [])
            if not users:
                break

            for u in users:
                uid = u.get("id", "")
                name = u.get("display_name") or u.get("user_name", "")
                if uid:
                    all_user_ids.append(uid)
                    all_user_names.append(name)

            if len(users) < page_size:
                break  # 最后一页

            offset += page_size
            if len(all_user_ids) >= max_users:
                break

            time.sleep(random.uniform(0.2, 0.5))

        except Exception as e:
            log.warning(f"fetch_comment_like_users error: {e}")
            break

    return all_user_ids, all_user_names


def parse_comments(data: dict, art_id: str) -> list[dict]:
    """解析 Spot.im conversation 响应中的所有评论（含回复）

    Args:
        data: API 响应 JSON
        art_id: WSJ 文章 ID

    Returns:
        扁平化的评论列表，每条格式: {comment_id, parent_id, text, time, likes, user_id, user_name}
    """
    conversation = data.get("conversation", {})
    raw_comments = conversation.get("comments", [])
    users = conversation.get("users", {})

    result = []

    def _parse(comments: list, parent_id: str = None):
        for raw in comments:
            if not isinstance(raw, dict):
                continue

            cid = raw.get("id", "")
            if not cid:
                continue

            # 用户信息
            uid = raw.get("user_id", "")
            user_data = users.get(uid, {}) if uid else {}
            user_name = (
                user_data.get("display_name")
                or raw.get("user_display_name")
                or user_data.get("user_name")
                or "unknown"
            )

            # 内容
            content_raw = raw.get("content", "")
            if isinstance(content_raw, list):
                text = "\n".join(
                    block.get("text", "") for block in content_raw
                    if isinstance(block, dict) and block.get("text")
                )
            elif isinstance(content_raw, str):
                text = content_raw
            else:
                text = raw.get("body", "")

            # 时间
            ts = raw.get("written_at") or raw.get("time")
            if isinstance(ts, (int, float)) and ts > 0:
                time_str = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            elif isinstance(ts, str):
                time_str = ts
            else:
                time_str = None

            # 点赞
            rank = raw.get("rank", {})
            likes = rank.get("ranks_up", 0) if isinstance(rank, dict) else 0

            result.append({
                "comment_id": cid,
                "parent_id": parent_id,
                "text": text,
                "time": time_str,
                "likes": likes,
                "user_id": uid,
                "user_name": user_name,
            })

            # 递归解析子评论
            replies = raw.get("replies", [])
            if replies:
                _parse(replies, cid)

    _parse(raw_comments)
    return result


def fetch_all_comments(
    session: httpx.Client,
    post_id: str,
    jwt: str,
) -> tuple[list[dict], int]:
    """获取文章的全部评论（自动翻页）

    Returns:
        (comments_list, total_messages_count)
    """
    all_comments: dict[str, dict] = {}  # comment_id → comment
    total = 0
    offset = 0
    max_pages = 60  # 安全上限：3000 条

    for page in range(max_pages):
        data = fetch_conversation_page(session, post_id, jwt, offset=offset)

        if not data:
            break
        if "error" in data:
            log.warning(f"Page {page} error: {data['error']}")
            break

        conversation = data.get("conversation", {})
        if page == 0:
            total = conversation.get("messages_count", 0)

        comments = parse_comments(data, post_id)
        for c in comments:
            cid = c["comment_id"]
            if cid not in all_comments:
                all_comments[cid] = c

        has_next = conversation.get("has_next", False)
        new_offset = conversation.get("offset", offset + len(conversation.get("comments", [])))
        offset = new_offset

        if not has_next:
            break

        # 翻页延迟
        time.sleep(random.uniform(0.2, 0.5))

    return list(all_comments.values()), total


# ============================================================
# 点赞用户回填
# ============================================================

def backfill_likes(max_comments: int = None):
    """回填已有评论的点赞用户数据

    遍历 comment_info 中有 cmt_likes_count > 0 但 cmt_likes_user_id
    为空的评论，调用 API 获取点赞用户并更新。

    Args:
        max_comments: 最多回填的评论数（None = 全量）
    """
    log.info("=" * 60)
    log.info("Backfill: Like Users for Existing Comments")
    log.info("=" * 60)

    jwt = load_spotim_jwt()
    if not jwt:
        log.error("No valid Spot.im JWT found.")
        return

    db = get_db()
    cur = db.cursor()

    # 找出需要回填的评论：有 likes 但 likes_user_id 为空
    limit_clause = f"LIMIT {max_comments}" if max_comments else ""
    cur.execute(f"""
        SELECT article_id, comment_id, cmt_likes_count, user_nm
        FROM comment_info
        WHERE cmt_likes_count > 0
          AND (cmt_likes_user_id IS NULL OR cardinality(cmt_likes_user_id) = 0)
        ORDER BY cmt_likes_count DESC
        {limit_clause}
    """)
    rows = cur.fetchall()
    cur.close()

    log.info(f"Candidates for backfill: {len(rows)}")

    if not rows:
        log.info("No comments need backfill.")
        db.close()
        return

    session = httpx.Client(timeout=30, http2=True)
    updated = 0
    empty = 0
    errors = 0

    try:
        for i, (art_id, cid, likes, user_nm) in enumerate(rows):
            if i > 0 and i % 20 == 0:
                log.info(f"Progress: {i}/{len(rows)} (updated={updated} empty={empty} err={errors})")
                db.commit()

            like_uids, like_unms = fetch_comment_like_users(
                session, cid, art_id, jwt
            )

            if like_uids:
                # 更新数据库
                cur2 = db.cursor()
                try:
                    cur2.execute("""
                        UPDATE comment_info
                        SET cmt_likes_user_id = %s,
                            cmt_likes_user_nm = %s,
                            scrape_time = %s
                        WHERE article_id = %s AND comment_id = %s
                    """, (
                        like_uids,
                        like_unms,
                        datetime.now(timezone.utc).isoformat(),
                        art_id,
                        cid,
                    ))
                    updated += 1
                except Exception as e:
                    db.rollback()
                    log.warning(f"DB update error for {cid[:50]}: {e}")
                    errors += 1
                finally:
                    cur2.close()

            else:
                empty += 1
                log.debug(f"No like users returned for {cid[:50]} (likes={likes})")

            # 延迟
            time.sleep(random.uniform(0.3, 0.8))

        db.commit()

    finally:
        session.close()
        db.close()

    log.info("=" * 60)
    log.info(f"Backfill done: updated={updated} empty={empty} errors={errors}")
    log.info("=" * 60)


# ============================================================
# 主流程
# ============================================================

def run(max_articles: int = 5):
    """运行评论采集

    Args:
        max_articles: 最多处理的文章数（None = 全量）
    """
    log.info("=" * 60)
    log.info("WSJ Spot.im Comment Collector (APP API)")
    log.info("=" * 60)

    # 加载 JWT
    jwt = load_spotim_jwt()
    if not jwt:
        log.error("No valid Spot.im JWT found. Run _extract_spotim_jwt.py first.")
        return

    db = get_db()
    session = httpx.Client(timeout=30, http2=True)

    try:
        # 获取待采集文章
        limit = max_articles if max_articles else BATCH_SIZE
        articles = get_articles_for_comments(db, limit)
        log.info(f"Articles to process: {len(articles)}")

        if not articles:
            log.info("No articles with body found. Run graphql_collector.py first.")
            return

        stats = {"done": 0, "failed": 0, "no_comments": 0, "total_comments": 0}

        for i, (art_id, art_title, art_url) in enumerate(articles):
            # 跳过已采集过评论的文章
            existing = get_comment_count_for_article(db, art_id)
            if existing > 0:
                log.info(f"[{i+1}/{len(articles)}] SKIP (has {existing} comments): {art_title[:50]}")
                # 确保 article_info.comments_count 与实际评论数一致
                update_comments_count(db, art_id, existing)
                continue

            log.info(f"[{i+1}/{len(articles)}] {art_title[:60]}")

            # 获取评论
            comments, total = fetch_all_comments(session, art_id, jwt)

            if not comments:
                log.info(f"  → 0 comments (API reports {total} total)")
                stats["no_comments"] += 1
                # 标记已处理（防止重复查询）
                update_comments_count(db, art_id, 0)
                continue

            # 保存评论
            saved = 0
            affected_users = set()
            for c in comments:
                # 先保存评论基本信息
                if save_comment(db, art_id, c):
                    saved += 1

                # 同步写入 user_info 和 user_post_info
                save_user_info(db, c["user_id"], c["user_name"])
                save_user_post(db, art_id, art_title, c)
                affected_users.add(c["user_id"])

                # 如果有点赞，尝试获取点赞用户
                if c["likes"] > 0:
                    like_uids, like_unms = fetch_comment_like_users(
                        session, c["comment_id"], art_id, jwt
                    )
                    if like_uids:
                        c["like_user_ids"] = like_uids
                        c["like_user_names"] = like_unms
                        save_comment(db, art_id, c)  # 回填
                        log.debug(f"  +{len(like_uids)} like users for {c['user_name']}")
                    time.sleep(random.uniform(0.3, 0.8))

            # 更新评论计数
            update_comments_count(db, art_id, len(comments))

            # 刷新本批用户的统计信息
            if affected_users:
                refresh_user_stats(db, list(affected_users))

            stats["done"] += 1
            stats["total_comments"] += saved
            log.info(f"  → {saved} comments saved (API reports {total} total)")

            # 定期提交
            if i > 0 and i % SAVE_INTERVAL == 0:
                db.commit()
                log.info(f"Progress: {stats['done']} articles, {stats['total_comments']} comments")

            # 间隔延迟
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        db.commit()

        log.info("=" * 60)
        log.info(f"Done: {stats['done']} articles, {stats['total_comments']} comments, "
                 f"{stats['failed']} failed, {stats['no_comments']} no comments")
        log.info("=" * 60)

    finally:
        session.close()
        db.close()


if __name__ == "__main__":
    # 回填模式
    if "--backfill-likes" in sys.argv:
        max_comments = None
        for arg in sys.argv:
            if arg.isdigit():
                max_comments = int(arg)
                break
        backfill_likes(max_comments)
        sys.exit(0)

    # 普通采集模式
    max_articles = 5  # 默认测试 5 篇

    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "all":
            max_articles = None  # 全量
        elif arg == "test":
            max_articles = 3
        else:
            try:
                max_articles = int(arg)
            except ValueError:
                log.error(f"Invalid argument: {arg}. Use: N | test | all | --backfill-likes [N]")
                sys.exit(1)

    run(max_articles)
