"""WSJ 全量采集器 —— 列表 → 正文 → 评论 三段式

用法:
    python wsj_collector.py list          # 采集文章列表 (GraphQL 直连)
    python wsj_collector.py content       # 采集正文 (Clash + Cookie)
    python wsj_collector.py comments      # 采集评论 (Spot.IM Token)
    python wsj_collector.py all           # 全流程
    python wsj_collector.py all --count 1000
"""

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from cdp_fetcher import (
    CDPClient,
    fetch_article_via_cdp,
    get_wsj_tab_ws_url,
    ensure_cdp_forward,
)

# ============================================================
# 配置
# ============================================================

GRAPHQL_URL = "https://shared-data.dowjones.io/gateway/graphql"
BASE_DIR = Path(__file__).parent

# 已验证的 Section IDs（每个返回 ~50 篇文章，多数不重复）
SECTION_IDS = [
    "Mobile_Section_wsj_us_WEB_NOW_TOP_NEWS_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_MARKETS_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_ECONOMY_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_BUSINESS_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_TECH_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_POLITICS_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_WORLD_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_LIFE_WORK_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_OPINION_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_REAL_ESTATE_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_HEALTH_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_SCIENCE_PROD",
    "Mobile_Section_wsj_us_WEB_NOW_PERSONAL_FINANCE_PROD",
]

PERSISTED_HASHES = {
    "SectionQuery": "ab7186abca6a58eda629a7f27b9aa918aa114ccb35ac6d8a8a4569720f22fe5d",
    "ArticleFetchInfoByIds": "a454670f78621ececadb9fd719b02f76ffc83f30cba4d966a49f9a4c06d82f0d",
}

GQL_HEADERS = {
    "accept": "multipart/mixed; deferSpec=20220824, application/json",
    "user-agent": "wsj-version-6.18.1.1-code-61801001-android-32",
    "apollographql-client-name": "wsj-mobile-android-release",
    "apollographql-client-version": "6.18.1.1",
    "content-type": "application/json",
}

PROXY = "http://127.0.0.1:7890"  # Clash
DATA_DIR = BASE_DIR / "collector_data"
DATA_DIR.mkdir(exist_ok=True)

# 并发控制
CONTENT_CONCURRENCY = 5
COMMENT_CONCURRENCY = 3
RETRY_DELAY = 2


def load_credentials():
    """加载从浏览器抓包中提取的认证凭据"""
    cookie_file = BASE_DIR / "capture" / "_wsj_cookie.txt"
    ua_file = BASE_DIR / "capture" / "_wsj_ua.txt"
    token_file = BASE_DIR / "capture" / "_spotim_token.txt"

    creds = {}
    if cookie_file.exists():
        creds["cookie"] = cookie_file.read_text().strip()
    if ua_file.exists():
        creds["ua"] = ua_file.read_text().strip()
    if token_file.exists():
        creds["spotim_token"] = token_file.read_text().strip()
    return creds


# ============================================================
# 第1段：文章列表（GraphQL 直连，无需认证）
# ============================================================

def fetch_section(section_id: str, sem: asyncio.Semaphore | None = None) -> list[dict]:
    """采集一个栏目，返回标准化文章列表"""
    body = {
        "operationName": "SectionQuery",
        "variables": {"id": section_id},
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": PERSISTED_HASHES["SectionQuery"],
            }
        },
    }
    resp = httpx.post(GRAPHQL_URL, json=body, headers=GQL_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("data", {}).get("summaryCollectionContent", {}).get("collectionItems", [])
    articles = []
    seen = set()

    for item in items:
        if item.get("__typename") != "SummaryCollection":
            continue
        for art in item.get("collectionItems", []):
            c = art.get("content", {})
            if c.get("__typename") != "Article":
                continue
            oid = c.get("originId")
            if not oid or oid in seen:
                continue
            seen.add(oid)

            summary = c.get("mobileSummary", {}) or {}
            headline_block = summary.get("summaryHeadline", {}) or {}
            flashline_block = summary.get("summaryFlashline", {}) or {}
            descs = summary.get("descriptions", []) or []

            articles.append({
                "originId": oid,
                "sourceUrl": c.get("sourceUrl", ""),
                "headline": _flatten_text(headline_block.get("flattened", {})),
                "section": _flatten_text(flashline_block.get("flattened", {})),
                "summary": "; ".join(
                    _flatten_text(
                        (d.get("textAndDecorations", {}) or {}).get("flattened", {})
                    )
                    for d in descs
                ) if descs else "",
                "publishedAt": c.get("publishedDateTimeUtc", ""),
                "isFree": c.get("articleIsFree", False),
                "sectionSource": section_id,
            })
    return articles


def _flatten_text(block: dict) -> str:
    return (block.get("text") or "").strip()


async def collect_article_list(target_count: int = 1000) -> list[dict]:
    """聚合多个栏目，去重后收集到 target_count 篇文章"""
    print(f"[列表] 目标: {target_count} 篇，遍历 {len(SECTION_IDS)} 个栏目...")
    all_articles: dict[str, dict] = {}

    for sid in SECTION_IDS:
        if len(all_articles) >= target_count:
            break
        try:
            batch = fetch_section(sid)
            new_count = 0
            for art in batch:
                oid = art["originId"]
                if oid not in all_articles:
                    all_articles[oid] = art
                    new_count += 1
            print(f"  {sid.split('_')[-1]}: +{new_count} unique → total {len(all_articles)}")
            if new_count == 0:
                break  # 无新文章，后面栏目大概率也重复
            time.sleep(0.3)
        except Exception as e:
            print(f"  [WARN] {sid}: {e}")

    result = list(all_articles.values())
    print(f"[列表] 完成: {len(result)} 篇\n")
    return result


# ============================================================
# 第2段：文章正文（wsj.com → Clash 代理 + Cookie）
# ============================================================

def extract_article_body(html: str) -> str:
    """从 wsj.com HTML 提取正文纯文本"""
    # <article> 标签内的内容
    m = re.search(r'<article\b[^>]*>([\s\S]*?)</article>', html)
    if not m:
        m = re.search(r'<article[\s\S]*?</article>', html)
    if m:
        content = m.group(0)
    else:
        # 降级：找 page-article-content 或 article-content
        m2 = re.search(r'(?:class="[^"]*article[^"]*body[^"]*"|data-page-article-content)[\s\S]{200,}', html)
        content = m2.group(0) if m2 else html

    # 去除 script/style
    content = re.sub(r'<script[\s\S]*?</script>', '', content)
    content = re.sub(r'<style[\s\S]*?</style>', '', content)
    # 保留段落和换行
    content = re.sub(r'<br\s*/?>', '\n', content)
    content = re.sub(r'</p>', '\n\n', content)
    content = re.sub(r'</h\d>', '\n\n', content)
    content = re.sub(r'</div>', '\n', content)
    content = re.sub(r'</li>', '\n', content)
    content = re.sub(r'<[^>]+>', '', content)
    # 解码 HTML 实体
    content = content.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    content = content.replace('&quot;', '"').replace('&#x27;', "'").replace('&#39;', "'")
    content = content.replace('&nbsp;', ' ')
    # 清理空白
    content = re.sub(r'\n{3,}', '\n\n', content)
    content = re.sub(r'[ \t]{2,}', ' ', content)
    lines = [l.strip() for l in content.split('\n')]
    # 过滤太短的行（导航、按钮等）
    lines = [l for l in lines if len(l) > 25 or (10 < len(l) < 80 and l[0].isupper())]
    return '\n'.join(lines).strip()


async def fetch_one_article(client: httpx.AsyncClient, art: dict, sem: asyncio.Semaphore) -> dict:
    """抓取单篇文章正文"""
    url = art.get("sourceUrl", "")
    result = {**art, "body": "", "contentStatus": "unknown"}

    if not url:
        result["contentStatus"] = "no_url"
        return result

    async with sem:
        for attempt in range(3):
            try:
                resp = await client.get(url, timeout=30)
                if resp.status_code == 200:
                    body = extract_article_body(resp.text)
                    if len(body) > 200:
                        result["body"] = body
                        result["contentStatus"] = "ok"
                    else:
                        result["body"] = body
                        result["contentStatus"] = "short"
                        print(f"  [SHORT {len(body)}c] {art['originId']}")
                    return result
                elif resp.status_code == 401:
                    result["contentStatus"] = "auth_required"
                    return result
                elif resp.status_code == 403:
                    result["contentStatus"] = "forbidden"
                    return result
                else:
                    result["contentStatus"] = f"http_{resp.status_code}"
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            except Exception as e:
                result["contentStatus"] = f"error: {e}"
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))

    return result


async def collect_content(articles: list[dict], checkpoint_file: Path) -> list[dict]:
    """批量采集文章正文"""
    creds = load_credentials()
    cookie = creds.get("cookie", "")
    ua = creds.get("ua", "Mozilla/5.0")

    if not cookie:
        print("[正文] 错误: 缺少 Cookie，请先运行 capture")
        return articles

    # 恢复断点
    if checkpoint_file.exists():
        done = json.loads(checkpoint_file.read_text())
        done_map = {a["originId"]: a for a in done}
        print(f"[正文] 断点恢复: {len(done)} 已完成")
    else:
        done = []
        done_map = {}

    pending = [a for a in articles if a["originId"] not in done_map]
    total = len(pending)
    print(f"[正文] 待抓取: {total} 篇 (并发={CONTENT_CONCURRENCY})")

    sem = asyncio.Semaphore(CONTENT_CONCURRENCY)
    headers = {
        "Cookie": cookie,
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    async with httpx.AsyncClient(
        proxy=PROXY,
        headers=headers,
        follow_redirects=True,
        timeout=httpx.Timeout(30),
    ) as client:
        tasks = [fetch_one_article(client, a, sem) for a in pending]
        completed = 0

        for coro in asyncio.as_completed(tasks):
            result = await coro
            done.append(result)
            done_map[result["originId"]] = result
            completed += 1

            if completed % 20 == 0 or completed == total:
                pct = completed * 100 // total
                ok_count = sum(1 for a in done[-completed:] if a.get("contentStatus") == "ok")
                print(f"  [{completed}/{total}] {pct}% | ok={ok_count}/{min(20,completed)}")

            # 定期保存检查点
            if completed % 50 == 0:
                checkpoint_file.write_text(json.dumps(done, ensure_ascii=False, indent=2), encoding='utf-8')

    checkpoint_file.write_text(json.dumps(done, ensure_ascii=False, indent=2), encoding='utf-8')
    ok_count = sum(1 for a in done if a.get("contentStatus") == "ok")
    print(f"[正文] 完成: ok={ok_count}/{len(done)}\n")
    return done


# ============================================================
# 第2段（CDP版）：通过 Chrome DevTools 获取正文，绕过 DataDome
# ============================================================

def collect_content_via_cdp(articles: list[dict], checkpoint_file: Path, device_id: str = "emulator-5556") -> list[dict]:
    """通过 CDP 控制模拟器 Chrome 批量采集正文（串行，绕过 DataDome TLS 检测）"""

    # 0. 确保 ADB forward 和 WSJ Tab 可用
    print("[正文-CDP] 初始化...")
    if not ensure_cdp_forward(device_id):
        print("[正文-CDP] 错误: 无法建立 ADB forward，请确认模拟器已启动")
        return articles

    ws_url = get_wsj_tab_ws_url()
    if not ws_url:
        print("[正文-CDP] 错误: 未找到 WSJ Tab，请在模拟器 Chrome 中打开 wsj.com")
        return articles

    # 1. 恢复断点
    if checkpoint_file.exists():
        done = json.loads(checkpoint_file.read_text(encoding='utf-8'))
        done_map = {a["originId"]: a for a in done}
        print(f"[正文-CDP] 断点恢复: {len(done)} 已完成")
        # 回填已完成的 body
        for art in articles:
            if art["originId"] in done_map:
                done_art = done_map[art["originId"]]
                art["body"] = done_art.get("body", "")
                art["contentStatus"] = done_art.get("contentStatus", "ok")
    else:
        done = []
        done_map = {}

    pending = [a for a in articles if a["originId"] not in done_map]
    total = len(pending)
    print(f"[正文-CDP] 待抓取: {total} 篇 (串行，每篇约 10-15s)")

    if total == 0:
        print("[正文-CDP] 全部已完成!")
        return articles

    # 2. 串行抓取（复用同一个 Tab）
    ws_url = get_wsj_tab_ws_url()  # 重新获取（可能因导航变化）
    for i, art in enumerate(pending):
        url = art.get("sourceUrl", "")
        oid = art["originId"]

        if not url:
            art["body"] = ""
            art["contentStatus"] = "no_url"
            done.append(art)
            done_map[oid] = art
            continue

        print(f"  [{i+1}/{total}] {oid} ...", end=" ", flush=True)

        # 每次重新连接（更稳定）
        try:
            result = fetch_article_via_cdp(ws_url, url, timeout=25)
        except Exception as e:
            result = {"body": "", "contentStatus": f"cdp_error: {e}"}

        art["body"] = result.get("body", "")
        art["contentStatus"] = result.get("contentStatus", "error")
        art["title"] = result.get("title", art.get("headline", ""))

        status = art["contentStatus"]
        body_len = len(art["body"])
        print(f"{status} ({body_len}c)")

        done.append(art)
        done_map[oid] = art

        # 定期保存断点
        if (i + 1) % 10 == 0:
            checkpoint_file.write_text(
                json.dumps(done, ensure_ascii=False, indent=2), encoding='utf-8'
            )
            print(f"  [checkpoint] {len(done)} saved")

        # 请求间隔
        time.sleep(0.5)

    # 3. 最终保存
    checkpoint_file.write_text(
        json.dumps(done, ensure_ascii=False, indent=2), encoding='utf-8'
    )

    ok_count = sum(1 for a in articles if a.get("contentStatus") == "ok")
    print(f"[正文-CDP] 完成: ok={ok_count}/{len(articles)}\n")
    return articles


# ============================================================
# 第3段：Spot.IM 评论
# ============================================================

async def fetch_comments_for_article(client: httpx.AsyncClient, art: dict, token: str, sem: asyncio.Semaphore) -> dict:
    """抓取单篇文章的 Spot.IM 评论"""
    url = art.get("sourceUrl", "")
    result = {**art, "comments": [], "commentCount": 0, "commentStatus": "unknown"}

    if not url or not token:
        result["commentStatus"] = "no_token"
        return result

    # 需要用 article URL 作为 post_id（Spot.IM 用 URL hash 定位 conversation）
    post_id = url

    async with sem:
        try:
            # Spot.IM conversation read API
            api_url = f"https://api-2-0.spot.im/v1.0.0/conversation/read"
            params = {
                "conversation_id": post_id,
                "sort_by": "newest",
                "count": 50,
                "sp_out": "organic",
            }
            headers = {
                "x-access-token": token,
                "x-spot-id": "sp_PtNMEcTVvWqI",
                "accept": "application/json",
            }
            resp = await client.get(api_url, params=params, headers=headers, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                comments = data.get("conversation", {}).get("comments", [])
                result["comments"] = [
                    {
                        "id": c.get("id"),
                        "content": (c.get("content", [{}])[0].get("text", "") if c.get("content") else ""),
                        "author": c.get("user", {}).get("display_name", ""),
                        "createdAt": c.get("created_at", ""),
                        "likes": c.get("likes_count", 0),
                        "replyCount": c.get("replies_count", 0),
                    }
                    for c in comments
                ]
                result["commentCount"] = len(result["comments"])
                result["commentStatus"] = "ok"
            else:
                result["commentStatus"] = f"http_{resp.status_code}"
        except Exception as e:
            result["commentStatus"] = f"error: {e}"

    return result


async def collect_comments(articles: list[dict]) -> list[dict]:
    """批量采集评论"""
    creds = load_credentials()
    token = creds.get("spotim_token", "")
    if not token:
        print("[评论] 错误: 缺少 Spot.IM token")
        return articles

    # 只对有正文的文章采集评论
    target = [a for a in articles if a.get("contentStatus") == "ok"]
    if not target:
        print("[评论] 没有可采集评论的文章")
        return articles

    # 恢复断点
    cp_file = DATA_DIR / "comments_checkpoint.json"
    done_map = {}
    if cp_file.exists():
        done = json.loads(cp_file.read_text())
        done_map = {a["originId"]: a for a in done}
        print(f"[评论] 断点恢复: {len(done)} 已完成")
    else:
        done = []

    # 合并已有到 articles
    for art in articles:
        if art["originId"] in done_map:
            art.update(done_map[art["originId"]])

    pending = [a for a in target if a["originId"] not in done_map]
    total = len(pending)
    print(f"[评论] 待抓取: {total} 篇 (并发={COMMENT_CONCURRENCY})")

    sem = asyncio.Semaphore(COMMENT_CONCURRENCY)
    async with httpx.AsyncClient(proxy=PROXY, timeout=httpx.Timeout(20)) as client:
        tasks = [fetch_comments_for_article(client, a, token, sem) for a in pending]
        completed = 0
        new_done = []

        for coro in asyncio.as_completed(tasks):
            result = await coro
            new_done.append(result)
            done_map[result["originId"]] = result
            completed += 1

            if completed % 20 == 0 or completed == total:
                pct = completed * 100 // total
                has_comments = sum(1 for a in new_done[-min(20, completed):] if a.get("commentCount", 0) > 0)
                print(f"  [{completed}/{total}] {pct}% | with_comments={has_comments}")

            if completed % 50 == 0:
                all_done = [a for a in done_map.values()]
                cp_file.write_text(json.dumps(all_done, ensure_ascii=False, indent=2), encoding='utf-8')

    # 最终合并
    for art in articles:
        if art["originId"] in done_map:
            art.update(done_map[art["originId"]])

    has_comments = sum(1 for a in articles if a.get("commentCount", 0) > 0)
    print(f"[评论] 完成: {has_comments}/{len(articles)} 篇有评论\n")
    return articles


# ============================================================
# 主流程
# ============================================================

def save_results(articles: list[dict], name: str):
    """保存结果到 JSON 文件"""
    out = DATA_DIR / f"wsj_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"[保存] {out} ({len(articles)} 篇)")
    return out


def print_stats(articles: list[dict]):
    """打印统计摘要"""
    total = len(articles)
    with_content = sum(1 for a in articles if a.get("contentStatus") == "ok")
    with_comments = sum(1 for a in articles if a.get("commentCount", 0) > 0)
    total_comments = sum(a.get("commentCount", 0) for a in articles)
    free_only = sum(1 for a in articles if a.get("isFree"))

    print(f"\n{'='*50}")
    print(f"  总计: {total} 篇")
    print(f"  有正文: {with_content} 篇 ({with_content*100//max(1,total)}%)")
    print(f"  有评论: {with_comments} 篇 ({with_comments*100//max(1,total)}%), 共 {total_comments} 条")
    print(f"  免费文章: {free_only} 篇")
    print(f"{'='*50}\n")


async def run_all(target_count: int):
    """完整流程"""
    print(f"{'='*50}")
    print(f" WSJ 全量采集器 (target={target_count})")
    print(f"{'='*50}\n")

    # 1. 文章列表
    list_file = DATA_DIR / "articles_list.json"
    if list_file.exists():
        articles = json.loads(list_file.read_text(encoding='utf-8'))
        print(f"[列表] 加载缓存: {len(articles)} 篇")
    else:
        articles = await collect_article_list(target_count)
        list_file.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding='utf-8')

    # 2. 正文（CDP 绕过 DataDome）
    content_cp = DATA_DIR / "content_checkpoint_cdp.json"
    articles = collect_content_via_cdp(articles, content_cp)
    save_results(articles, "content")

    # 3. 评论
    articles = await collect_comments(articles)
    final_file = save_results(articles, "full")
    print_stats(articles)
    print(f"最终输出: {final_file}")


async def main():
    parser = argparse.ArgumentParser(description="WSJ 全量采集器")
    parser.add_argument("stage", nargs="?", default="all",
                        choices=["list", "content", "comments", "all"],
                        help="采集阶段")
    parser.add_argument("--count", type=int, default=1000, help="目标文章数")
    parser.add_argument("--resume", action="store_true", help="断点续传")
    args = parser.parse_args()

    stage = args.stage

    if stage == "list":
        articles = await collect_article_list(args.count)
        save_results(articles, "list")
        print_stats(articles)

    elif stage == "content":
        list_files = sorted(DATA_DIR.glob("wsj_list_*.json"))
        if not list_files:
            print("错误: 先运行 'list' 生成文章列表")
            return
        list_file = list_files[-1]
        print(f"[正文] 加载文章列表: {list_file.name}")
        articles = json.loads(list_file.read_text(encoding='utf-8'))
        content_cp = DATA_DIR / "content_checkpoint_cdp.json"
        if not args.resume and content_cp.exists():
            content_cp.unlink()
        articles = collect_content_via_cdp(articles, content_cp)
        save_results(articles, "content")
        print_stats(articles)

    elif stage == "comments":
        # 从最近的 content 输出加载
        content_files = sorted(DATA_DIR.glob("wsj_content_*.json"))
        if not content_files:
            print("错误: 先运行 'content' 生成正文输出")
            return
        articles = json.loads(content_files[-1].read_text(encoding='utf-8'))
        articles = await collect_comments(articles)
        save_results(articles, "full")
        print_stats(articles)

    elif stage == "all":
        await run_all(args.count)


if __name__ == "__main__":
    asyncio.run(main())
