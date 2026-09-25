"""存量回填：把 AI Quick Summary 补进已有文章的 Art_Text（零 WSJ 请求）

原理：Article_Info.art_text_html 存的是 ArticleContent 的完整原始 JSON，
AI 摘要就在其中（flattenedAltSummaries[].list 非空者）。本脚本只读本地库
重抽摘要并前置拼进 art_text，不发起任何外部请求、不重新采集。

特性：
  - keyset 分页（art_id 递增），内存占用恒定
  - 每批 commit，可随时 Ctrl-C，重跑自动跳过已处理行（art_text 以 'Quick Summary:' 开头）
  - 无摘要的文章原样跳过

用法: python backfill_summary.py [batch_size 默认500]
"""
import json
import sys

import psycopg2

from config import PG_CONFIG
from graphql_collector import extract_quick_summary, log

BATCH = int(sys.argv[1]) if len(sys.argv) > 1 else 500


def flush(conn, batch):
    w = conn.cursor()
    w.executemany(
        "UPDATE Article_Info SET art_text=%s, scrape_time=scrape_time WHERE art_id=%s",
        batch,
    )
    w.close()
    conn.commit()


def main():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    last_id = ""
    scanned = updated = 0
    while True:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT art_id, art_text, art_text_html FROM Article_Info
            WHERE art_id > %s
              AND art_text_html IS NOT NULL
              AND art_text NOT LIKE 'Quick Summary:%%'
            ORDER BY art_id
            LIMIT %s
            """,
            (last_id, BATCH),
        )
        rows = cur.fetchall()
        cur.close()
        if not rows:
            break
        last_id = rows[-1][0]
        batch = []
        for art_id, art_text, raw in rows:
            scanned += 1
            try:
                article = json.loads(raw)
            except Exception:
                continue
            bullets = extract_quick_summary(article)
            if not bullets:
                continue
            block = "Quick Summary:\n" + "\n".join(f"- {b}" for b in bullets)
            new_text = (block + "\n\n" + art_text) if art_text else block
            batch.append((new_text, art_id))
        if batch:
            flush(conn, batch)
            updated += len(batch)
        log.info(f"progress: scanned={scanned} updated={updated} last_id={last_id}")
    conn.commit()
    conn.close()
    log.info(f"DONE scanned={scanned} updated={updated}")


if __name__ == "__main__":
    main()
