# -*- coding: utf-8 -*-
"""
フロー（JST）:
1) コピー元スプレッドシート 'Yahoo'!A:D から「前日15:00〜当日14:59:59」に入る行だけ取得
   A:タイトル / B:URL / C:投稿日 / D:掲載元
2) 出力先（当日タブ yyMMdd）へ A〜E列として追記
   A:ソース("Yahoo") / B:タイトル / C:URL / D:投稿日(yy/m/d HH:MM) / E:掲載元
   - URL重複は当日タブ内でスキップ
3) その当日タブの C列URLを起点に、本文（最大10ページ）を F..O、コメント数を P、コメント本文を Q.. に追記

認証:
- GitHub Secrets: GOOGLE_CREDENTIALS（サービスアカウントJSONの“中身”）
- 出力先は固定: 1UVwusLRcL4cZ3J9hnO6Z-f_d_sTFmocQJ9DcX3-v9u0

必要ライブラリ:
- gspread, oauth2client, requests, beautifulsoup4, selenium (4.24+)
"""

import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ====== 設定 ======
# コピー元（Yahooリスト）
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"
SOURCE_SHEET_NAME = "Yahoo"  # A:タイトル / B:URL / C:投稿日 / D:掲載元

# 出力先（固定・ご指定のシート）
DEST_SPREADSHEET_ID = "1UVwusLRcL4cZ3J9hnO6Z-f_d_sTFmocQJ9DcX3-v9u0"

# 本文・コメント取得設定
MAX_BODY_PAGES = 10  # 本文ページ上限は従来通り（Yahoo本文は多くても数ページ想定）
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}

# コメントの安全上限（無限スクレイプ暴走防止）。必要なら変更可。
MAX_TOTAL_COMMENTS = 5000

TZ_JST = timezone(timedelta(hours=9))

# ====== 共通ユーティリティ ======
def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    """
    コピー元C列（投稿日）を JST datetime に変換
    許容: "MM/DD HH:MM"（年は当年補完）, "YYYY/MM/DD HH:MM", "YYYY/MM/DD HH:MM:SS", Excelシリアル
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.strip()
        for fmt in ("%m/%d %H:%M", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None
    if isinstance(raw, (int, float)):
        epoch = datetime(1899, 12, 30, tzinfo=TZ_JST)  # Excel起点
        return epoch + timedelta(days=float(raw))
    if isinstance(raw, datetime):
        return raw.astimezone(TZ_JST) if raw.tzinfo else raw.replace(tzinfo=TZ_JST)
    return None

def format_yy_m_d_hm(dt: datetime) -> str:
    """yy/m/d HH:MM に整形（先頭ゼロの月日を避ける）"""
    yy = dt.strftime("%y")
    m = str(int(dt.strftime("%m")))
    d = str(int(dt.strftime("%d")))
    hm = dt.strftime("%H:%M")
    return f"{yy}/{m}/{d} {hm}"

# ====== 認証 ======
def build_gspread_client() -> gspread.Client:
    try:
        creds_str = os.environ.get("GOOGLE_CREDENTIALS")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
        else:
            # ローカル実行用フォールバック
            with open("credentials.json", "r", encoding="utf-8") as f:
                info = json.load(f)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
        return gspread.authorize(credentials)
    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

# ====== 出力先タブ操作 ======
def ensure_today_sheet(sh: gspread.Spreadsheet, today_tab: str) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(today_tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=today_tab, rows="3000", cols="300")
    return ws

def get_existing_urls(ws: gspread.Worksheet) -> Set[str]:
    vals = ws.col_values(3)  # C列=URL
    return set(vals[1:] if len(vals) > 1 else [])

def ensure_ae_header(ws: gspread.Worksheet) -> None:
    # A:ソース / B:タイトル / C:URL / D:投稿日 / E:掲載元
    head = ws.row_values(1)
    target = ["ソース", "タイトル", "URL", "投稿日", "掲載元"]
    if head != target:
        ws.update('A1', [target])

def ensure_body_comment_headers(ws: gspread.Worksheet, max_comments: int) -> None:
    """
    1行目に F..O(本文1〜10), P(コメント数), Q..(コメント1〜N) を整える
    """
    current = ws.row_values(1)
    base = ["ソース", "タイトル", "URL", "投稿日", "掲載元"]
    body_headers = [f"本文({i}ページ)" for i in range(1, 11)]  # F..O
    comments_count = ["コメント数"]  # P
    comment_headers = [f"コメント{i}" for i in range(1, max(1, max_comments) + 1)]  # Q..
    target = base + body_headers + comments_count + comment_headers
    if current != target:
        ws.update('A1', [target])

# ====== コピー元 → 出力先（A〜E列） ======
def transfer_a_to_e(gc: gspread.Client, dest_ws: gspread.Worksheet) -> int:
    sh_src = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    ws_src = sh_src.worksheet(SOURCE_SHEET_NAME)
    rows = ws_src.get('A:D')  # ヘッダー含む

    now = jst_now()
    start = (now - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    end = now.replace(hour=14, minute=59, second=59, microsecond=0)

    ensure_ae_header(dest_ws)
    existing = get_existing_urls(dest_ws)

    to_append: List[List[str]] = []
    for i, r in enumerate(rows):
        if i == 0:
            continue  # ヘッダー
        title = r[0].strip() if len(r) > 0 and r[0] else ""
        url = r[1].strip() if len(r) > 1 and r[1] else ""
        posted_raw = r[2] if len(r) > 2 else ""
        site = r[3].strip() if len(r) > 3 and r[3] else ""
        if not title or not url:
            continue
        dt = parse_post_date(posted_raw, now)
        if not dt or not (start <= dt <= end):
            continue
        if url in existing:
            continue
        to_append.append(["Yahoo", title, url, format_yy_m_d_hm(dt), site])

    if to_append:
        # まとめて追記（A:E）
        dest_ws.append_rows(to_append, value_input_option="USER_ENTERED")
    return len(to_append)

# ====== 本文取得 ======
def fetch_article_pages(base_url: str) -> Tuple[str, str, List[str]]:
    title = "取得不可"
    article_date = "取得不可"
    bodies: List[str] = []
    for page in range(1, MAX_BODY_PAGES + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            res.raise_for_status()
        except Exception:
            break
        soup = BeautifulSoup(res.text, "html.parser")
        if page == 1:
            t = soup.find("title")
            if t and t.get_text(strip=True):
                title = t.get_text(strip=True).replace(" - Yahoo!ニュース", "")
            time_tag = soup.find("time")
            if time_tag:
                article_date = time_tag.get_text(strip=True)

        body_text = ""
        article = soup.find("article")
        if article:
            ps = article.find_all("p")
            body_text = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
        if not body_text:
            main = soup.find("main")
            if main:
                ps = main.find_all("p")
                body_text = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))

        if not body_text or (bodies and body_text == bodies[-1]):
            break
        bodies.append(body_text)
    return title, article_date, bodies

# ====== コメント取得（全ページ・全件） ======
def fetch_comments_with_selenium(base_url: str) -> List[str]:
    """
    Yahoo!ニュースのコメントを /comments?page=1 ... で全ページ巡回して全件取得。
    空ページに達するか、前後ページで重複（同一境界）を検知したら終了。
    暴走防止に MAX_TOTAL_COMMENTS を超えたら打ち切り。
    """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,2000")
    driver = webdriver.Chrome(options=options)  # Selenium Manager が自動でDriver解決

    comments: List[str] = []
    last_tail: Optional[str] = None
    page = 1
    try:
        while True:
            c_url = f"{base_url}/comments?page={page}"
            driver.get(c_url)
            # 軽い待機（必要に応じて調整）
            time.sleep(2.0)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # できるだけ堅牢なセレクタ群（将来変化に備えて複数併用）
            selectors = [
                "p.sc-169yn8p-10",                       # 既存
                "p[data-ylk*='cm_body']",                # 既存
                "p[class*='comment']",                    # 既存
                "div.commentBody, p.commentBody",        # 汎用
                "div[data-ylk*='cm_body']"               # 汎用
            ]

            p_candidates = []
            for sel in selectors:
                p_candidates.extend(soup.select(sel))

            page_comments = [p.get_text(strip=True) for p in p_candidates if p.get_text(strip=True)]
            # 重複除去（同一ページ内の重複）
            page_comments = list(dict.fromkeys(page_comments))

            # 空ページなら終了
            if not page_comments:
                break

            # 前ページ末尾と今ページ先頭が同じなら終了（巡回終了のシンプル判定）
            if last_tail is not None and page_comments and page_comments[0] == last_tail:
                break

            # 蓄積
            comments.extend(page_comments)

            # 安全上限で打ち切り
            if len(comments) >= MAX_TOTAL_COMMENTS:
                comments = comments[:MAX_TOTAL_COMMENTS]
                break

            # 次ページへ
            last_tail = page_comments[-1]
            page += 1

    finally:
        driver.quit()

    return comments

# ====== 本文＆コメントを書き込み ======
def write_bodies_and_comments(ws: gspread.Worksheet) -> None:
    urls = ws.col_values(3)[1:]  # C列URL（2行目以降）
    total = len(urls)
    print(f"🔎 URLs to process: {total}")
    if total == 0:
        return

    rows_data: List[List[str]] = []
    max_comments = 0
    for idx, url in enumerate(urls, start=2):
        try:
            print(f"  - ({idx-1}/{total}) {url}")
            _title, _date, bodies = fetch_article_pages(url)
            comments = fetch_comments_with_selenium(url)

            body_cells = bodies[:MAX_BODY_PAGES] + [""] * (MAX_BODY_PAGES - len(bodies))
            cnt = len(comments)
            row = body_cells + [cnt] + comments
            rows_data.append(row)
            if cnt > max_comments:
                max_comments = cnt
        except Exception as e:
            print(f"    ! Error: {e}")
            rows_data.append(([""] * MAX_BODY_PAGES) + [0])

    # 列幅を最大コメント数に
    need_cols = MAX_BODY_PAGES + 1 + max_comments
    for i in range(len(rows_data)):
        if len(rows_data[i]) < need_cols:
            rows_data[i].extend([""] * (need_cols - len(rows_data[i])))

    # ヘッダー整備（本文とコメントの列を含む完全版）
    ensure_body_comment_headers(ws, max_comments=max_comments)

    # F2 から一括更新
    if rows_data:
        ws.update("F2", rows_data)
        print(f"✅ 本文・コメントを書き込み: {len(rows_data)} 行（最大コメント列={max_comments}）")

# ====== メイン ======
def main():
    print(f"📄 DEST Spreadsheet: {DEST_SPREADSHEET_ID}")
    gc = build_gspread_client()
    dest_sh = gc.open_by_key(DEST_SPREADSHEET_ID)
    today_tab = jst_now().strftime("%y%m%d")
    ws = ensure_today_sheet(dest_sh, today_tab)

    # 1) A〜E を埋める
    added = transfer_a_to_e(gc, ws)
    print(f"📝 A〜E 追記: {added} 行")

    # 2) F以降（本文＆コメント）を埋める
    write_bodies_and_comments(ws)

if __name__ == "__main__":
    main()
