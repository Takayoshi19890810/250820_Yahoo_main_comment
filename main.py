# -*- coding: utf-8 -*-
"""
Yahooニュースのコピー元（SOURCE_SPREADSHEET_ID の 'Yahoo'!A:D）から
JST 15:00〜翌日14:59:59 の範囲に入るデータだけを抽出し、
出力先（DESTINATION_SPREADSHEET_ID）の当日タブ（yyMMdd）に
【ソース / タイトル / URL / 投稿日 / 引用元】の5列のみで追記します。
- URL重複はスキップ（当日タブ内で判定）
- 投稿日の表示形式は「yy/m/d HH:MM」（例: 25/8/20 15:01）
認証は環境変数 GOOGLE_CREDENTIALS（サービスアカウントJSONの中身）を使用。
"""

import os
import json
import datetime
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ===== 設定（必要に応じて変更） =====
SOURCE_SPREADSHEET_ID = '1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8'  # コピー元（Yahooリスト）
DESTINATION_SPREADSHEET_ID = '19c6yIGr5BiI7XwstYhUPptFGksPPXE4N1bEq5iFoPok'  # 出力先
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def build_sheets_service():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise RuntimeError("環境変数 GOOGLE_CREDENTIALS が未設定です。サービスアカウントJSONの“中身”をセットしてください。")
    try:
        info = json.loads(creds_json)
    except Exception as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS のJSONが不正です: {e}")
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

def jst_now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

def parse_post_date(raw, today_jst: datetime.datetime) -> datetime.datetime | None:
    """
    コピー元C列（投稿日時想定）の文字列/数値を datetime(JST) に変換して返す。
    想定:
      - "MM/DD HH:MM" -> 今年の年を補う
      - "YYYY/MM/DD HH:MM:SS"
      - Excelシリアル（float）
    """
    if raw is None:
        return None

    # 文字列
    if isinstance(raw, str):
        s = raw.strip()
        for fmt in ("%m/%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                return dt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
            except ValueError:
                pass
        return None

    # Excelシリアル（float）
    if isinstance(raw, (int, float)):
        # Excel序数: 1899-12-30起点
        epoch = datetime.datetime(1899, 12, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
        return epoch + datetime.timedelta(days=float(raw))

    # 日付/日時オブジェクト（稀）
    if isinstance(raw, datetime.date):
        if isinstance(raw, datetime.datetime):
            return raw
        return datetime.datetime.combine(raw, datetime.time()).replace(
            tzinfo=datetime.timezone(datetime.timedelta(hours=9))
        )
    return None

def format_yy_m_d_hm(dt: datetime.datetime) -> str:
    """
    投稿日の表示を「yy/m/d HH:MM」に整形。
    先頭ゼロを避けるため月・日はint化して組み立てる。
    """
    yy = dt.strftime("%y")
    m = str(int(dt.strftime("%m")))
    d = str(int(dt.strftime("%d")))
    hm = dt.strftime("%H:%M")
    return f"{yy}/{m}/{d} {hm}"

def ensure_destination_tab(service, spreadsheet_id: str, sheet_name: str):
    info = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = info.get('sheets', [])
    if not any(sh['properties']['title'] == sheet_name for sh in sheets):
        body = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def get_existing_urls(service, spreadsheet_id: str, sheet_name: str) -> set:
    rng = f"'{sheet_name}'!A:E"
    res = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = res.get('values', [])
    urls = set()
    if values:
        # ヘッダー判定
        start = 1 if values[0] and values[0][0] == "ソース" else 0
        for row in values[start:]:
            if len(row) > 2 and row[2]:
                urls.add(row[2])
    return urls

def append_rows(service, spreadsheet_id: str, sheet_name: str, rows: List[List[str]], header_if_needed=True):
    # 既存データを確認してヘッダー有無を判定
    rng = f"'{sheet_name}'!A1:E"
    res = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = res.get('values', [])
    header_needed = True
    if values and values[0] and values[0][0] == "ソース":
        header_needed = False

    if header_if_needed and header_needed:
        header = [["ソース", "タイトル", "URL", "投稿日", "引用元"]]
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption='USER_ENTERED',
            body={'values': header}
        ).execute()

    if rows:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:E",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': rows}
        ).execute()

def transfer_news():
    service = build_sheets_service()

    # JSTの期間：昨日15:00〜今日14:59:59
    now = jst_now()
    start = (now - datetime.timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    end = now.replace(hour=14, minute=59, second=59, microsecond=0)
    today_tab = now.strftime("%y%m%d")

    print(f"出力先タブ: {today_tab}")
    print(f"期間: {start.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end.strftime('%Y/%m/%d %H:%M:%S')}")

    # 出力先タブ用意 & 既存URL
    ensure_destination_tab(service, DESTINATION_SPREADSHEET_ID, today_tab)
    existing = get_existing_urls(service, DESTINATION_SPREADSHEET_ID, today_tab)
    print(f"既存URL: {len(existing)} 件")

    # コピー元読み取り（Yahoo）
    source_name = "Yahoo"
    src_range = f"'{source_name}'!A:D"
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=SOURCE_SPREADSHEET_ID, range=src_range
        ).execute()
        rows = resp.get('values', [])
    except HttpError as e:
        print(f"エラー: コピー元シート取得失敗: {e}")
        rows = []

    # 収集
    out: List[List[str]] = []
    if rows:
        # 1行目はヘッダー想定: A=タイトル, B=URL, C=投稿日, D=引用元
        for i, r in enumerate(rows):
            if i == 0:
                continue
            title = r[0] if len(r) > 0 else ""
            url = r[1] if len(r) > 1 else ""
            posted_raw = r[2] if len(r) > 2 else ""
            cite = r[3] if len(r) > 3 else ""

            dt = parse_post_date(posted_raw, now)
            if not dt:
                continue
            if not (start <= dt <= end):
                continue
            if url in existing:
                continue

            # 出力: ソース/タイトル/URL/投稿日/引用元
            out.append([source_name, title, url, format_yy_m_d_hm(dt), cite])

    if not out:
        print("新規追加なし")
    else:
        append_rows(service, DESTINATION_SPREADSHEET_ID, today_tab, out)
        print(f"新規 {len(out)} 件を追加")

if __name__ == "__main__":
    transfer_news()
