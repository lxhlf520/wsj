"""WSJ APP GraphQL 采集器 - 基于 Apollo Persisted Queries

不需要 token 即可获取文章列表和内容。
评论通过 mobile-gw.spot.im 移动端网关。

使用方法:
    python collector.py section    # 按栏目采集文章列表
    python collector.py article   # 采集文章内容
    python collector.py comments  # 采集评论
"""

import json
import sys
from datetime import datetime
import httpx
from pathlib import Path

# ============================================================
# 抓包反推的配置（无需 token）
# ============================================================

GRAPHQL_URL = "https://shared-data.dowjones.io/gateway/graphql"

HEADERS = {
    "accept": "multipart/mixed; deferSpec=20220824, application/json",
    "user-agent": "wsj-version-6.18.1.1-code-61801001-android-32",
    "apollographql-client-name": "wsj-mobile-android-release",
    "apollographql-client-version": "6.18.1.1",
    "content-type": "application/json",
}

# Persisted Query Hashes (从 APK 抓包提取)
HASHES = {
    "SectionQuery": "ab7186abca6a58eda629a7f27b9aa918aa114ccb35ac6d8a8a4569720f22fe5d",
    "ArticleFetchInfoByIds": "a454670f78621ececadb9fd719b02f76ffc83f30cba4d966a49f9a4c06d82f0d",
    "WhatsNewsCarousel": "7f61b9fd6b3feb6f2d8bf4950f20ee95f940da2a3c34c8bc0a34627c63eca992",
    "IssueQuery": "d938226e7d1c1fff050e7d084c72179e2713dcf4736d3a442c618c55b896f847",
    "MobileRecommendationsContent": "582330befcac1b7d011d8f2c5bac0a4cab9ce916497575a3bab8b78555928293",
    "MobileSettings": "9b90c83a0b339e071a63a09799f425a15ce81a4e7f9697862279f90286abb5e5",
    "QuotesByDialect": "12f31f975fcbc8d3c2cbb6d7760033004f5b5b269a780ee82bb681c97c1eb581",
    "SavedContent": "72d3d538e1d163471833eea661335222dd53b3ad43a93a0ddc12695eeca390cf",
    "UserData": "7821a20c94593e9f6db036153cec9ff16100fae7ad3fa280aae91d096fe632ec",
}

# 已知的栏目 Section IDs (从抓包提取，可以扩展)
SECTIONS = {
    "top_news": "Mobile_Section_wsj_us_WEB_NOW_TOP_NEWS_PROD",
    "markets": "Mobile_Section_wsj_us_WEB_NOW_MARKETS_PROD",
    "economy": "Mobile_Section_wsj_us_WEB_NOW_ECONOMY_PROD",
    # 更多栏目可通过 app 浏览不同 tab 抓包获取
}


class WSJCollector:
    def __init__(self, proxy: str | None = None):
        self.client = httpx.Client(
            timeout=30,
            proxy=proxy,
            headers=HEADERS,
            http2=True,
            verify=True,
        )
        self.output_dir = Path(__file__).parent / "output"
        self.output_dir.mkdir(exist_ok=True)

    def _graphql_call(self, operation: str, variables: dict, use_auth: bool = False) -> dict:
        """发送 persisted query（只发 hash，不发 query text）"""
        sha256 = HASHES.get(operation)
        if not sha256:
            raise ValueError(f"Unknown operation: {operation}")

        body = {
            "operationName": operation,
            "variables": variables,
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": sha256,
                }
            },
        }

        hdrs = {
            "x-apollo-operation-id": sha256,
            "x-apollo-operation-name": operation,
        }
        if use_auth:
            hdrs["authorization"] = f"Bearer {self.token}"

        resp = self.client.post(GRAPHQL_URL, json=body, headers=hdrs)
        resp.raise_for_status()
        return resp.json()

    def fetch_section(self, section_id: str) -> list[dict]:
        """获取栏目下的文章列表"""
        resp = self._graphql_call("SectionQuery", {"id": section_id})
        result = resp.get("data", {}).get("summaryCollectionContent", {})
        articles = []
        for item in result.get("collectionItems", []):
            if item.get("__typename") == "SummaryCollection":
                for sub in item.get("collectionItems", []):
                    articles.append(sub)
        return articles

    def fetch_articles_by_ids(self, ids: list[str], id_type: str = "originid") -> dict:
        """批量获取文章信息"""
        resp = self._graphql_call("ArticleFetchInfoByIds", {
            "idType": id_type,
            "ids": ids,
        })
        return resp.get("data", {}).get("articlesByIds", {})

    def fetch_whats_news(self, date_str: str | None = None) -> list[dict]:
        """获取 What's News 轮播"""
        if not date_str:
            date_str = datetime.utcnow().strftime("%Y-%m-%dT04:00:01Z")
        resp = self._graphql_call("WhatsNewsCarousel", {"utcDate": date_str})
        return resp

    def fetch_issue(self, publication: str = "WSJ", region: str = "US", masthead: str = "WEB") -> dict:
        """获取报纸版面"""
        resp = self._graphql_call("IssueQuery", {
            "publication": publication,
            "region": region,
            "masthead": masthead,
        })
        return resp


# ============================================================
# 命令行
# ============================================================

def main():
    # 直连可用，如需代理改为 proxy="http://127.0.0.1:7890"
    collector = WSJCollector(proxy=None)

    if len(sys.argv) < 2:
        print("用法: python collector.py [section|article|comments|test]")
        return

    cmd = sys.argv[1]

    if cmd == "test":
        print("测试 SectionQuery ...")
        articles = collector.fetch_section(SECTIONS["top_news"])
        print(f"获取到 {len(articles)} 篇文章")

        for art in articles[:5]:
            content = art.get("content", {})
            print(f"  - {content.get('originId')} | {content.get('sourceUrl', '')[:80]}")
            print(f"    发布时间: {content.get('publishedDateTimeUtc')}")

        # 保存
        out = collector.output_dir / f"top_news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json.dump(articles, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"已保存到 {out}")

    elif cmd == "section":
        section_name = sys.argv[2] if len(sys.argv) > 2 else "top_news"
        section_id = SECTIONS.get(section_name, section_name)
        print(f"采集栏目: {section_id}")
        articles = collector.fetch_section(section_id)
        print(f"获取到 {len(articles)} 篇文章")
        out = collector.output_dir / f"{section_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json.dump(articles, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"已保存到 {out}")

    elif cmd == "article":
        # 先获取文章列表，再批量查详情
        section_name = sys.argv[2] if len(sys.argv) > 2 else "top_news"
        articles = collector.fetch_section(SECTIONS.get(section_name, section_name))
        ids = []
        for art in articles:
            c = art.get("content", {})
            if c.get("originId"):
                ids.append(c["originId"])

        print(f"获取到 {len(ids)} 个文章 ID，批量查询详情...")
        details = collector.fetch_articles_by_ids(ids[:50])  # 批次限制
        out = collector.output_dir / f"articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json.dump(details, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"已保存到 {out}")


if __name__ == "__main__":
    main()
