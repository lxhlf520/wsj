"""探测 WSJ ArticleContent 响应里是否包含 AI Quick Summary 字段

用途：
  1. 取最近采集的几篇文章，重新调 ArticleContent 拿完整原始 JSON
  2. 递归扫描所有 key，找 summary / takeaway / bullet / quick / keypoint / tldr 相关字段
  3. 列出 articleBody 里出现的全部 __typename（Quick Summary 可能是一种 body item）

在 sver1 上运行：uv run python probe_summary.py [篇数，默认3]
"""

import sys
import json
import re
import httpx
import psycopg2

from config import PG_CONFIG
from graphql_collector import (
    load_client_jwt, fetch_article_body, PROXY, log,
)

# 疑似 AI 摘要相关的 key 特征
KEY_RE = re.compile(r"summar|takeaway|bullet|quick|keypoint|key_point|tldr|digest|recap", re.I)


def walk_keys(obj, path="$", hits=None):
    """递归收集所有 key 路径；命中特征的记录 (path, 值预览)"""
    if hits is None:
        hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}"
            if KEY_RE.search(k):
                preview = json.dumps(v, ensure_ascii=False)[:300] if not isinstance(v, str) else v[:300]
                hits.append((p, preview))
            walk_keys(v, p, hits)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:5]):  # 每层最多看5个元素，防刷屏
            walk_keys(v, f"{path}[{i}]", hits)
    return hits


def collect_typenames(obj, names=None):
    if names is None:
        names = set()
    if isinstance(obj, dict):
        t = obj.get("__typename")
        if t:
            names.add(t)
        for v in obj.values():
            collect_typenames(v, names)
    elif isinstance(obj, list):
        for v in obj:
            collect_typenames(v, names)
    return names


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    jwt = load_client_jwt()
    if not jwt:
        log.error("拿不到 Client JWT，先确保调试 Chrome 登录态正常")
        return

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    # 取最近入库的正常文章（Quick Summary 是近年才有的功能，选新文章）
    cur.execute("""
        SELECT art_id, art_url FROM Article_Info
        WHERE art_text NOT LIKE 'FAILED%' AND length(art_text) > 200
        ORDER BY scrape_time DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    session = httpx.Client(timeout=30, http2=True, proxy=PROXY or None)

    for art_id, art_url in rows:
        print("=" * 70)
        print(f"文章: {art_id}  {art_url[:80]}")
        result = fetch_article_body(session, art_id, jwt)
        if not result:
            print("  ArticleContent 请求失败")
            continue
        article = json.loads(result["body_json"])

        typenames = sorted(collect_typenames(article))
        print(f"  articleBody 相关 __typename ({len(typenames)}):")
        for t in typenames:
            print(f"    - {t}")

        hits = walk_keys(article)
        if hits:
            print(f"  ★ 命中疑似摘要字段 {len(hits)} 处:")
            for p, preview in hits[:20]:
                print(f"    {p}")
                print(f"      => {preview}")
        else:
            print("  ✗ 未发现任何 summary/takeaway/bullet 相关字段")

    session.close()


if __name__ == "__main__":
    main()
