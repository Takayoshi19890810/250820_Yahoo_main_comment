# -*- coding: utf-8 -*-
"""
当日タブ（例: yyMMdd）にあるURL（C列）をもとに、
F列以降に本文（最大10ページ）、P列にコメント数、Q列以降にコメント本文を追記する。

前提:
- A:ソース / B:タイトル / C:URL / D:投稿日 / E:掲載元 は既に存在（main.py等で作成済み）
- 当日タブ名は JST の yyMMdd
- 認証は GOOGLE_CREDENTIALS(サービスアカウントJSONの中身) または credentials.json

仕様:
- 本文は最大10ページ分を F..O 列へ (本文(1ページ) ～ 本文(10ページ))
- コメント数を P 列へ
- コメント本文を Q 列以降に横並びで格納（コメント1, コメント2, ...）
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ===== 設定 =====
SPREADSHEET_ID = "1UVwusLRcL4cZ3J9hnO6Z-f_d_sTFmocQJ9DcX3-v9u0"  # 出力先シート
SHEET_NAME = datetime.now(timezone(timedelta(hours=9))).strftime("%y%m%d")  # 当日タブ
MAX_BODY_PAGES = 10
MAX_COMMENT_PAGES = 10
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}

# ===== 認証 =====
def build_gspread_client() -> gspread.Client:
    try:
        creds_str = os.environ.get("GOOGLE_CREDENTIALS")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
        else:
            with open("credentials.json", "r", encoding="utf-8") as f:
                info = json.load(f)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
        return gspread.authorize(credentials)
    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

# ===== ヘッダ管理 =====
def ensure_sheet_and_headers(ws: gspread.Worksheet, max_comments: int) -> None:
    values = ws.get('A1:Z1')
    header = values[0] if values else []
    required = ["ソース","タイトル","URL","投稿日","掲載元"]
    body_headers = [f"本文({i}ページ)" for i in range(1, 11)]
    comments_count_header = ["コメント数"]
    comment_headers = [f"コメント{i}" for i in range(1, max(1, max_comments) + 1)]
    target_header = required + body_headers + comments_count_header + comment_headers
    if header != target_header:
        ws.update('A1', [target_header])

# ===== 本文取得 =====
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

# ===== コメント取得 =====
def fetch_comments_with_selenium(base_url: str) -> List[str]:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,2000")
    driver = webdriver.Chrome(options=options)  # ✅ Selenium Manager が自動解決
    comments: List[str] = []
    try:
        for page in range(1, MAX_COMMENT_PAGES + 1):
            c_url = f"{base_url}/comments?page={page}"
            driver.get(c_url)
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            p_candidates = []
            p_candidates.extend(soup.find_all("p", class_="sc-169yn8p-10"))
            p_candidates.extend(soup.select("p[data-ylk*='cm_body']"))
            p_candidates.extend(soup.select("p[class*='comment']"))
            page_comments = [p.get_text(strip=True) for p in p_candidates if p.get_text(strip=True)]
            if not page_comments:
                break
            if comments and page_comments and page_comments[0] == comments[-1]:
                break
            comments.extend(page_comments)
    finally:
        driver.quit()
    return comments

# ===== メイン処理 =====
def main():
    print(f"📄 Spreadsheet: {SPREADSHEET_ID}")
    print(f"📑 Sheet: {SHEET_NAME}")
    gc = build_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows="2000", cols="200")
    urls = ws.col_values(3)[1:]
    total = len(urls)
    print(f"🔎 URLs to process: {total}")
    if total == 0:
        return
    rows_data: List[List[str]] = []
    max_comments = 0
    for idx, url in enumerate(urls, start=2):
        try:
            print(f"  - ({idx-1}/{total}) {url}")
            title, article_date, bodies = fetch_article_pages(url)
            comments = fetch_comments_with_selenium(url)
            body_cells = bodies[:MAX_BODY_PAGES] + [""] * (MAX_BODY_PAGES - len(bodies))
            comment_count = len(comments)
            row = body_cells + [comment_count] + comments
            rows_data.append(row)
            if comment_count > max_comments:
                max_comments = comment_count
        except Exception as e:
            print(f"    ! Error: {e}")
            row = ([""] * MAX_BODY_PAGES) + [0]
            rows_data.append(row)
    need_cols = MAX_BODY_PAGES + 1 + max_comments
    for i in range(len(rows_data)):
        if len(rows_data[i]) < need_cols:
            rows_data[i].extend([""] * (need_cols - len(rows_data[i])))
    ensure_sheet_and_headers(ws, max_comments=max_comments)
    ws.update("F2", rows_data)
    print(f"✅ 書き込み完了: {len(rows_data)}行 / コメント列={max_comments}")

if __name__ == "__main__":
    main()
