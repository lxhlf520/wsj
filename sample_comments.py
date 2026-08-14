# -*- coding: utf-8 -*-
"""按年份抽查 Spot.IM 评论分布，确定评论真实起点年份。

当前采集器把所有 2015+ 的文章都当「有评论潜力」处理，但 WSJ 接入
Spot.IM 评论系统的实际时间晚于 2015 年，导致大量早期文章白跑 API。

本脚本每个年份随机抽 N 篇有正文的文章，调一次 conversation/read，
统计哪年才开始有评论。据此可把 claim 查询的年份阈值收紧到真实起点。

用法:
    python sample_comments.py [每年抽样数=5]
"""
import sys

import httpx

from comment_collector import get_db, load_spotim_jwt, fetch_all_comments


def main():
    sample_n = int(sys.argv[1]) if len(sys.argv) > 1 else 5

    jwt = load_spotim_jwt()
    if not jwt:
        print("No valid Spot.im JWT. Run _extract_spotim_jwt.py first.")
        return

    db = get_db()
    cur = db.cursor()
    session = httpx.Client(timeout=30, http2=True)

    print(f"{'年份':>6} {'抽样':>4} {'有评论':>6} {'评论总数':>8}")
    try:
        for yr in range(2015, 2026):
            cur.execute(
                """
                SELECT Art_ID FROM Article_Info
                WHERE Art_Text IS NOT NULL
                  AND Art_ID IS NOT NULL AND Art_ID != ''
                  AND (Art_ID LIKE 'WP-WSJ-%%' OR Art_ID LIKE 'SB%%')
                  AND Art_Time ~ %s
                ORDER BY random() LIMIT %s
                """,
                (str(yr), sample_n),
            )
            ids = [r[0] for r in cur.fetchall()]
            if not ids:
                print(f"{yr:>6}  (无样本)")
                continue

            with_c, total_c = 0, 0
            for aid in ids:
                comments, total, _err = fetch_all_comments(session, aid, jwt)
                if comments:
                    with_c += 1
                total_c += total or 0

            print(f"{yr:>6} {len(ids):>4} {with_c:>6} {total_c:>8}")
    finally:
        session.close()
        db.close()


if __name__ == "__main__":
    main()
