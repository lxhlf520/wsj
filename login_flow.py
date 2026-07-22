"""
SSO 自动登录获取 Client JWT

链路:
  1. GET sso.accounts.dowjones.com/authorize → 302 → 获取 state/nonce
  2. GET /login-page → 获取 CSRF token
  3. POST /authenticate → 提交用户名密码 → 返回 auto-submit 表单
  4. POST /postauth/handler → 302 → wsj.com/client/auth?code=...
  5. GET wsj.com/client/auth → follow redirects → 种 connect.sid
  6. GET wsj.com/client?legacy=false → 返回 JWT

用法:
  python login_flow.py --user EMAIL --password PASSWORD
  python login_flow.py  # 从 .env 或环境变量 WSJ_USER / WSJ_PASS 读取
"""

import os
import sys
import json
import base64
import re
import uuid
import time
import argparse
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, unquote, parse_qs, urlparse

import httpx

# --- 常量 ---
SSO_BASE = "https://sso.accounts.dowjones.com"
WSJ_BASE = "https://www.wsj.com"
CLIENT_ID = "lppaldtu1DpMsDI07TKYgTgBaCQ0a54TzPlaQA75"
REDIRECT_URI = f"{WSJ_BASE}/client/auth"
SCOPE = (
    "openid idp_id roles tags email given_name family_name "
    "uuid djid djUsername djStatus trackid prts updated_at "
    "created_at offline_access group"
)
UI_LOCALES = "en-us-x-wsj-223-2"

TOKEN_FILE = Path(__file__).parent / "client_jwt.txt"

UA = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36"
)


def get_jwt_info(jwt: str) -> dict | None:
    """解析 JWT payload"""
    parts = jwt.split(".")
    if len(parts) != 3:
        return None
    pay = parts[1] + "=" * (4 - len(parts[1]) % 4)
    return json.loads(base64.b64decode(pay))


def print_jwt_info(jwt: str):
    """打印 JWT 信息"""
    p = get_jwt_info(jwt)
    if not p:
        return
    print(f"  iss:       {p.get('iss')}")
    print(f"  email:     {p.get('email')}")
    print(f"  name:      {p.get('given_name')} {p.get('family_name')}")
    print(f"  roles:     {p.get('roles')}")
    print(f"  iat:       {datetime.fromtimestamp(p['iat'])}")
    print(f"  exp:       {datetime.fromtimestamp(p['exp'])}")
    remaining = p.get("exp", 0) - time.time()
    print(f"  剩余有效:  {remaining/3600:.1f} 小时")


def login(user: str, password: str) -> str | None:
    """执行完整 SSO 登录流程，返回 Client JWT。失败返回 None。"""

    base_headers = {
        "user-agent": UA,
        "accept-language": "zh-CN",
        "dnt": "1",
        "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
    }

    with httpx.Client(
        timeout=30,
        follow_redirects=False,
        headers=base_headers,
    ) as c:
        # ═══════════════════════════════════════════
        # Step 1: GET /authorize → 302 → /login-page
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 1] GET /authorize")

        nonce = str(uuid.uuid4())
        auth_params = {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPE,
            "ui_locales": UI_LOCALES,
            "nonce": nonce,
            "state": "https://www.wsj.com",
        }
        r = c.get(
            f"{SSO_BASE}/authorize",
            params=auth_params,
            headers={
                "accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "sec-fetch-site": "cross-site",
                "sec-fetch-mode": "navigate",
                "sec-fetch-dest": "document",
                "referer": f"{WSJ_BASE}/",
            },
        )
        print(f"  Status: {r.status_code}")

        if r.status_code != 302:
            print(f"  ❌ 期望 302, 实际 {r.status_code}")
            print(f"  Body: {r.text[:400]}")
            return None

        location = r.headers.get("location", "")
        print(f"  → /login-page")

        # ═══════════════════════════════════════════
        # Step 2: GET /login-page → 获取 CSRF, state, nonce
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 2] GET /login-page")

        login_url = f"{SSO_BASE}{location}" if location.startswith("/") else location
        r = c.get(
            login_url,
            headers={
                "accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "navigate",
                "sec-fetch-dest": "document",
            },
        )
        print(f"  Status: {r.status_code}")

        # 从最终 URL 提取 state, nonce
        final_url = str(r.url)
        parsed = parse_qs(urlparse(final_url).query)
        state = parsed.get("state", [""])[0]
        nonce_extracted = parsed.get("nonce", [nonce])[0]
        print(f"  State: {state[:60]}...")
        print(f"  Nonce: {nonce_extracted}")

        # 获取 CSRF token (从 cookie)
        csrf = None
        for cookie in c.cookies.jar:
            if cookie.name == "csrf":
                csrf = cookie.value
                break
        if not csrf:
            print("  ❌ 未获取到 CSRF cookie!")
            return None
        print(f"  CSRF:  {csrf[:40]}...")

        # ═══════════════════════════════════════════
        # Step 3: POST /authenticate
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 3] POST /authenticate")

        r = c.post(
            f"{SSO_BASE}/authenticate",
            json={
                "username": user,
                "password": password,
                "state": state,
                "client_id": CLIENT_ID,
                "csrf": csrf,
                "response_mode": None,
                "scope": SCOPE,
                "code_challenge": None,
                "realm": "DJldap",
                "code_challenge_method": None,
                "nonce": nonce_extracted,
                "ui_locales": UI_LOCALES,
                "redirect_uri": REDIRECT_URI,
                "response_type": "code",
            },
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "origin": SSO_BASE,
                "referer": final_url,
            },
        )
        print(f"  Status: {r.status_code}")

        if r.status_code != 200:
            print(f"  Body: {r.text[:500]}")
            try:
                err = r.json()
                # 常见错误: 密码错误
                desc = err.get("description", "") or err.get("error_description", "")
                msg = err.get("message", "")
                print(f"  ❌ 错误: {desc or msg or json.dumps(err)}")
            except json.JSONDecodeError:
                pass
            return None

        # ═══════════════════════════════════════════
        # Step 4: 解析 HTML 表单 → POST /postauth/handler
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 4] Parse form → POST /postauth/handler")

        html = r.text

        # 从 HTML 中提取 form 参数
        token_m = re.search(r'name="token"\s+value="([^"]*)"', html)
        cid_m = re.search(r'name="client_id"\s+value="([^"]*)"', html)
        params_m = re.search(r'name="params"\s+value="([^"]*)"', html)

        if not token_m:
            # 备用: 宽松匹配
            token_m = re.search(r'name="token"\s*value="([^"]*)"', html)
        if not token_m:
            print("  ❌ 响应中未找到 token!")
            print(f"  HTML preview: {html[:600]}")
            return None

        token_val = token_m.group(1)
        print(f"  Token (len={len(token_val)}): {token_val[:50]}...")

        r = c.post(
            f"{SSO_BASE}/postauth/handler",
            data={
                "token": token_val,
                "client_id": cid_m.group(1) if cid_m else CLIENT_ID,
                "params": params_m.group(1) if params_m else "",
            },
            headers={
                "content-type": "application/x-www-form-urlencoded",
                "origin": "null",
                "accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "navigate",
                "sec-fetch-dest": "document",
            },
        )
        print(f"  Status: {r.status_code}")

        if r.status_code != 302:
            print(f"  ❌ 期望 302, 实际 {r.status_code}")
            print(f"  Body: {r.text[:400]}")
            return None

        location = r.headers.get("location", "")
        print(f"  → wsj.com/client/auth")

        # 从 URL 提取 code
        code_m = re.search(r'[?&]code=([^&]+)', location)
        if not code_m:
            print(f"  ❌ Location 中未找到 code: {location[:200]}")
            return None
        code = unquote(code_m.group(1))
        print(f"  Code: {code[:40]}...")

        # ═══════════════════════════════════════════
        # Step 5: GET wsj.com/client/auth → follow redirects to wsj.com
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 5] GET wsj.com/client/auth → follow redirects")

        # 提取 wsj state
        state_m = re.search(r'[?&]state=([^&"]+)', location)
        wsj_state = unquote(state_m.group(1)) if state_m else "https://www.wsj.com"

        auth_cb = f"{WSJ_BASE}/client/auth?provider=djop&code={code}&state={wsj_state}"

        # 手动跟随重定向链（最多 5 跳）
        next_url = auth_cb
        for hop in range(5):
            r = c.get(
                next_url,
                headers={
                    "accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                    "sec-fetch-site": "cross-site" if hop == 0 else "same-origin",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-dest": "document",
                },
            )
            print(f"  Hop {hop}: {r.status_code} → {next_url[:120]}")

            if r.status_code in (301, 302, 303, 307, 308):
                loc = r.headers.get("location", "")
                if loc.startswith("/"):
                    # relative → absolute
                    parsed = urlparse(next_url)
                    next_url = f"{parsed.scheme}://{parsed.netloc}{loc}"
                else:
                    next_url = loc
                print(f"    Location: {next_url[:120]}")
            else:
                print(f"    Final destination: {r.url}")
                # 最后一跳后，确保加载了 wsj.com 主页以获取 connect.sid
                if "wsj.com" in str(r.url) and r.status_code == 200:
                    # 可能还需要访问一次 / 来触发 connect.sid
                    pass
                break
        else:
            print(f"  ⚠️  重定向次数过多")

        # ═══════════════════════════════════════════
        # Step 6: GET wsj.com/ (确保 connect.sid 被设置)
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 6] GET wsj.com/ (ensure connect.sid)")

        r = c.get(
            f"{WSJ_BASE}/",
            headers={
                "accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "sec-fetch-site": "none",
                "sec-fetch-mode": "navigate",
                "sec-fetch-dest": "document",
            },
        )
        print(f"  Status: {r.status_code}")

        # 检查关键 cookies
        connect_sid = c.cookies.get("connect.sid", domain=".wsj.com")
        ca_rt = c.cookies.get("ca_rt", domain=".wsj.com")
        print(f"  connect.sid: {'✅' if connect_sid else '❌'}")
        print(f"  ca_rt:       {'✅' if ca_rt else '❌'}")

        # ═══════════════════════════════════════════
        # Step 7: GET /client?legacy=false → 获取 JWT
        # ═══════════════════════════════════════════
        print("=" * 60)
        print("[Step 7] GET /client?legacy=false")

        r = c.get(
            f"{WSJ_BASE}/client?legacy=false",
            headers={
                "accept": "*/*",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
                "referer": f"{WSJ_BASE}/",
            },
        )
        print(f"  Status: {r.status_code}")

        try:
            data = r.json()
        except json.JSONDecodeError:
            print(f"  ❌ 非 JSON 响应: {r.text[:300]}")
            return None

        is_logged = data.get("isLoggedIn", False)
        jwt = data.get("jwt", "")
        print(f"  isLoggedIn: {is_logged}")
        print(f"  JWT: {'✅ ' + jwt[:50] + '...' if jwt else '❌ 未获取到'}")

        if jwt:
            print()
            print("🎉 登录成功! Client JWT 信息:")
            print_jwt_info(jwt)
            TOKEN_FILE.write_text(jwt)
            print(f"\n✅ JWT 已保存到: {TOKEN_FILE}")
            return jwt

        if not is_logged:
            print(f"  ⚠️  未登录! 完整响应: {json.dumps(data, indent=2)}")
        return None


def main():
    parser = argparse.ArgumentParser(description="WSJ SSO 自动登录获取 Client JWT")
    parser.add_argument("--user", "-u", help="邮箱地址")
    parser.add_argument("--password", "-p", help="密码")
    args = parser.parse_args()

    user = args.user or os.environ.get("WSJ_USER", "")
    password = args.password or os.environ.get("WSJ_PASS", "")

    # 从 .env 文件读取
    if not user or not password:
        env_file = Path(__file__).parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line.startswith("WSJ_USER="):
                    user = line.split("=", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("WSJ_PASS="):
                    password = line.split("=", 1)[1].strip().strip('"').strip("'")

    if not user or not password:
        print("请提供账号密码:")
        print("  python login_flow.py -u EMAIL -p PASSWORD")
        print("  或在 .env 文件中设置 WSJ_USER / WSJ_PASS")
        print("  或在环境变量中设置 WSJ_USER / WSJ_PASS")
        sys.exit(1)

    jwt = login(user, password)
    sys.exit(0 if jwt else 1)


if __name__ == "__main__":
    main()
