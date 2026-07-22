"""CDP (Chrome DevTools Protocol) 文章提取器
通过 ADB 转发控制模拟器 Chrome，完全绕过 DataDome TLS 指纹检测。

前置条件:
    adb -s <device> forward tcp:9222 localabstract:chrome_devtools_remote
"""

import json
import re
import time
import httpx
from typing import Optional
from websocket import create_connection, WebSocket


class CDPClient:
    """单个 CDP Tab 的 WebSocket 客户端"""

    def __init__(self, ws_url: str, timeout: int = 60):
        self.ws: WebSocket = create_connection(ws_url, timeout=timeout)
        self._msg_id = 0

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    def send_and_wait(self, method: str, params: dict | None = None, timeout: int = 20) -> dict | None:
        """发送 CDP 命令并等待响应"""
        cid = self._next_id()
        msg = {"id": cid, "method": method}
        if params is not None:
            msg["params"] = params

        self.ws.send(json.dumps(msg))
        self.ws.settimeout(3)

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                raw = self.ws.recv()
            except Exception:
                continue
            resp = json.loads(raw)
            if resp.get("id") == cid:
                return resp
        return None

    def evaluate(self, expression: str, timeout: int = 15) -> str | None:
        """在页面执行 JavaScript 并返回结果"""
        result = self.send_and_wait("Runtime.evaluate", {
            "returnByValue": True,
            "expression": expression,
        }, timeout=timeout)
        if result:
            value = result.get("result", {}).get("result", {}).get("value")
            return value
        return None

    def navigate(self, url: str) -> bool:
        """导航到指定 URL"""
        result = self.send_and_wait("Page.navigate", {"url": url})
        if result is None:
            return False
        err = result.get("result", {}).get("errorText", "")
        if err:
            print(f"    [CDP] nav error: {err}")
        return True

    def wait_ready(self, timeout: int = 20) -> bool:
        """轮询 document.readyState == 'complete'"""
        deadline = time.time() + timeout
        last_state = ""
        while time.time() < deadline:
            state = self.evaluate("document.readyState", timeout=5)
            if state:
                last_state = state
                if state == "complete":
                    return True
            time.sleep(1)
        print(f"    [CDP] wait_ready timeout, last state: {last_state}")
        return False

    def get_current_url(self) -> str:
        """获取当前页面 URL"""
        url = self.evaluate("window.location.href", timeout=5)
        return url or ""

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def extract_article_from_html(html: str) -> str:
    """从 HTML 中提取文章正文纯文本"""
    # 取 <article> 标签
    m = re.search(r"<article[\s\S]*?</article>", html)
    if not m:
        m = re.search(r'<article\b[^>]*>([\s\S]*?)</article>', html)
    content = m.group(0) if m else html

    # 去脚本/样式
    content = re.sub(r"<script[\s\S]*?</script>", "", content)
    content = re.sub(r"<style[\s\S]*?</style>", "", content)
    # 保留结构
    content = re.sub(r"<br\s*/?>", "\n", content)
    content = re.sub(r"</p>", "\n\n", content)
    content = re.sub(r"</h\d>", "\n\n", content)
    content = re.sub(r"</div>", "\n", content)
    content = re.sub(r"</li>", "\n", content)
    content = re.sub(r"<[^>]+>", "", content)
    # 解码
    for entity, char in [
        ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
        ("&quot;", '"'), ("&#x27;", "'"), ("&#39;", "'"),
        ("&nbsp;", " "),
    ]:
        content = content.replace(entity, char)
    # 清理空白
    content = re.sub(r"\n{3,}", "\n\n", content)
    content = re.sub(r"[ \t]{2,}", " ", content)
    lines = [l.strip() for l in content.split("\n")]
    lines = [l for l in lines if len(l) > 25 or (10 < len(l) < 80 and l[0].isupper())]
    return "\n".join(lines).strip()


ARTICLE_EXTRACT_JS = """
(function() {
    let article = document.querySelector('article');
    if (!article) {
        let containers = document.querySelectorAll('[class*="article"]');
        for (let c of containers) {
            if (c.innerText && c.innerText.length > 500) {
                article = c;
                break;
            }
        }
    }
    if (!article) return JSON.stringify({error: 'no article element'});

    let h1 = document.querySelector('h1');
    let title = h1 ? h1.innerText.trim() : '';
    let paragraphs = article.querySelectorAll('p');
    let text_parts = [];
    for (let p of paragraphs) {
        let t = p.innerText.trim();
        if (t.length > 20) text_parts.push(t);
    }
    let text = text_parts.join('\\n\\n');
    if (!text) text = article.innerText;

    return JSON.stringify({
        title: title,
        text: text.substring(0, 50000),
        url: window.location.href,
        char_count: text.length,
        html: document.documentElement.outerHTML.substring(0, 500000)
    });
})()
"""


def fetch_article_via_cdp(ws_url: str, article_url: str, timeout: int = 25) -> dict:
    """通过 CDP 获取单篇文章正文

    Returns:
        {"body": "...", "contentStatus": "ok"|"short"|"fail", ...}
    """
    client = CDPClient(ws_url, timeout=timeout)

    try:
        # 1. 导航
        if not client.navigate(article_url):
            client.close()
            return {"body": "", "contentStatus": "nav_failed"}

        # 2. 等待加载
        if not client.wait_ready(timeout=timeout):
            client.close()
            return {"body": "", "contentStatus": "timeout"}

        # 3. 额外等待 JS 渲染
        time.sleep(1)

        # 4. 提取
        raw = client.evaluate(ARTICLE_EXTRACT_JS, timeout=10)
        if raw:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = {"error": "json_parse_failed", "raw": raw[:200]}

            if "error" in data:
                # 降级：取整页 HTML 再提取
                html = client.evaluate(
                    "document.documentElement.outerHTML", timeout=10
                )
                if html:
                    body = extract_article_from_html(html)
                    status = "ok" if len(body) > 200 else "short"
                    client.close()
                    return {
                        "body": body,
                        "contentStatus": status,
                        "title": "",
                        "char_count": len(body),
                    }
                client.close()
                return {"body": "", "contentStatus": "fallback_failed"}

            body = data.get("text", "")
            status = "ok" if len(body) > 200 else "short"
            client.close()
            return {
                "body": body,
                "contentStatus": status,
                "title": data.get("title", ""),
                "url": data.get("url", ""),
                "char_count": data.get("char_count", len(body)),
            }

        client.close()
        return {"body": "", "contentStatus": "eval_failed"}

    except Exception as e:
        client.close()
        return {"body": "", "contentStatus": f"error: {e}"}


def get_wsj_tab_ws_url() -> str | None:
    """获取模拟器 Chrome 中 WSJ Tab 的 WebSocket URL"""
    try:
        r = httpx.get("http://localhost:9222/json", timeout=5)
        pages = r.json()
        for p in pages:
            if "wsj.com" in p.get("url", ""):
                return p["webSocketDebuggerUrl"]
    except Exception:
        pass
    return None


def ensure_cdp_forward(device_id: str = "emulator-5556") -> bool:
    """确保 ADB forward 已建立"""
    import subprocess
    import os

    adb = os.path.expandvars(r"%USERPROFILE%\adb_temp\platform-tools\adb.exe")

    try:
        # 检查 forward 是否已存在
        result = subprocess.run(
            [adb, "-s", device_id, "forward", "--list"],
            capture_output=True, text=True, timeout=5,
        )
        if "tcp:9222" in result.stdout:
            return True

        # 建立 forward
        subprocess.run(
            [adb, "-s", device_id, "forward", "tcp:9222", "localabstract:chrome_devtools_remote"],
            capture_output=True, text=True, timeout=10,
        )
        return True
    except Exception as e:
        print(f"[CDP] ADB forward failed: {e}")
        return False
