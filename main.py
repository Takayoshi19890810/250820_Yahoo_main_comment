# -*- coding: utf-8 -*-
"""
コピー元スプレッドシートの 'Yahoo' シート（A:タイトル, B:URL, C:投稿日, D:掲載元）から
JSTの「前日15:00〜当日14:59:59」に入るデータのみを抽出し、
出力先スプレッドシートの当日タブ（yyMMdd）へ
【ソース / タイトル / URL / 投稿日 / 掲載元】の5列で追記します。

仕様:
- ソース列は固定で "Yahoo"
- 出力タブが無ければ自動作成
- 同一URLは当日タブ内で重複スキップ
- 投稿日は表示書式「yy/m/d HH:MM」（例: 25/8/21 15:01）
- 認証は環境変数 GOOGLE_CREDENTIALS（サービスアカウントJSONの中身）を使用
"""

import os
import json
import datetime
from typing import List, Optional, Set

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ===== 設定 =====
# コピー元：Yahooニュース集計シート（固定の想定）
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"  # ※必要なら変更
SOURCE_SHEET_NAME = "Yahoo"  # コピー元のシート名

# 出力先：ご指定のスプレッドシート
DESTINATION_SPREADSHEET_ID = "1UVwusLRcL4cZ3J9hnO6Z-f_d_sTFmocQJ9DcX3-v9u0"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TZ_JST = datetime.timezone(datetime.timedelta(hours=9))


# ===== ユーティリティ =====
def build_sheets_service():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise RuntimeError(
            "環境変数 GOOGLE_CREDENTIALS が未設定です。サービスアカウントJSONの“中身”を登録してください。"
        )
    try:
        info = json.loads(creds_json)
    except Exception as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS のJSONが不正です: {e}")
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def jst_now() -> datetime.datetime:
    return datetime.datetime.now(TZ_JST)


def parse_post_date(raw, today_jst: datetime.datetime) -> Optional[datetime.datetime]:
    """
    コピー元C列（投稿日）の想定値を JST の datetime に変換
    想定:
      - "MM/DD HH:MM" -> 年は当年補完
      - "YYYY/MM/DD HH:MM"
      - "YYYY/MM/DD HH:MM:SS"
      - Excelシリアル（float, int）
    """
    if raw is None:
        return None

    # 文字列
    if isinstance(raw, str):
        s = raw.strip()
        fmts = ("%m/%d %H:%M", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S")
        for fmt in fmts:
            try:
                dt = datetime.datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

    # Excelシリアル（数値）
    if isinstance(raw, (int, float)):
        epoch = datetime.datetime(1899, 12, 30, tzinfo=TZ_JST)  # Excel起点
        return epoch + datetime.timedelta(days=float(raw))

    # 日付/日時オブジェクト
    if isinstance(raw, datetime.datetime):
        return raw.astimezone(TZ_JST) if raw.tzinfo else raw.replace(tzinfo=TZ_JST)
    if isinstance(raw, datetime.date):
        return datetime.datetime.combine(raw, datetime.time()).replace(tzinfo=TZ_JST)

    return None


def format_yy_m_d_hm(dt: datetime.datetime) -> str:
    """
    「yy/m/d HH:MM」に整形（先頭ゼロの月日を避ける）
    """
    yy = dt.strftime("%y")
    m = str(int(dt.strftime("%m")))
    d = str(int(dt.strftime("%d")))
    hm = dt.strftime("%H:%M")
    return f"{yy}/{m}/{d} {hm}"


def ensure_destination_tab(service, spreadsheet_id: str, sheet_name: str) -> None:
    info = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = info.get("sheets", [])
    exists = any(sh["properties"]["title"] == sheet_name for sh in sheets)
    if not exists:
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def get_existing_urls(service, spreadsheet_id: str, sheet_name: str) -> Set[str]:
    rng = f"'{sheet_name}'!A:E"
    res = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = res.get("values", [])
    urls = set()
    if values:
        # ヘッダ行がある場合にスキップ
        start = 1 if values[0] and values[0][0] in ("ソース", "Source") else 0
        for row in values[start:]:
            if len(row) > 2 and row[2]:
                urls.add(row[2])
    return urls


def append_rows(service, spreadsheet_id: str, sheet_name: str, rows: List[List[str]]) -> None:
    """
    必要に応じてヘッダを付与してから行を追加
    """
    rng = f"'{sheet_name}'!A1:E"
    res = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = res.get("values", [])
    need_header = not (values and values[0] and values[0][0] == "ソース")

    if need_header:
        header = [["ソース", "タイトル", "URL", "投稿日", "掲載元"]]
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": header},
        ).execute()

    if rows:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:E",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()


def transfer_news():
    service = build_sheets_service()

    now = jst_now()
    # 期間: 前日15:00〜当日14:59:59（JST）
    start = (now - datetime.timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    end = now.replace(hour=14, minute=59, second=59, microsecond=0)
    today_tab = now.strftime("%y%m%d")

    print(f"出力タブ: {today_tab}")
    print(f"期間: {start.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end.strftime('%Y/%m/%d %H:%M:%S')}")

    # 出力先準備
    ensure_destination_tab(service, DESTINATION_SPREADSHEET_ID, today_tab)
    existing = get_existing_urls(service, DESTINATION_SPREADSHEET_ID, today_tab)
    print(f"既存URL: {len(existing)} 件")

    # コピー元取得
    src_range = f"'{SOURCE_SHEET_NAME}'!A:D"
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=SOURCE_SPREADSHEET_ID, range=src_range
        ).execute()
        rows = resp.get("values", [])
    except HttpError as e:
        print(f"エラー: コピー元シート取得失敗: {e}")
        rows = []

    # 収集
    out_rows: List[List[str]] = []
    if rows:
        # 1行目はヘッダ想定: A=タイトル, B=URL, C=投稿日, D=掲載元
        for i, r in enumerate(rows):
            if i == 0:
                continue
            title = r[0].strip() if len(r) > 0 and r[0] else ""
            url = r[1].strip() if len(r) > 1 and r[1] else ""
            posted_raw = r[2] if len(r) > 2 else ""
            source_site = r[3].strip() if len(r) > 3 and r[3] else ""

            if not title or not url:
                continue

            dt = parse_post_date(posted_raw, now)
            if not dt:
                continue
            if not (start <= dt <= end):
                continue
            if url in existing:
                continue

            out_rows.append(
                ["Yahoo", title, url, format_yy_m_d_hm(dt), source_site]
            )

    if out_rows:
        append_rows(service, DESTINATION_SPREADSHEET_ID, today_tab, out_rows)
        print(f"新規 {len(out_rows)} 件を追加")
    else:
        print("新規追加なし")


if __name__ == "__main__":
    transfer_news()
