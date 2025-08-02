import streamlit as st
import pandas as pd
import unicodedata
import time
from bs4 import BeautifulSoup
import requests
import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials

# === 定数設定 ====
HEADERS = {"User-Agent": "Mozilla/5.0"}
SPREADSHEET_ID = "1wMkpbOvqveVBkJSR85mpZcnKThYSEmusmsl710SaRKw"
SHEET_NAME = "cache_UMA"

# === Google Sheets 接続 ===
def connect_to_gspread():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    json_str = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not json_str:
        st.error("環境変数 GOOGLE_CREDENTIALS_JSON が見つかりません")
        st.stop()

    credentials_dict = json.loads(json_str)  # replace なしで読み込む
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")  # ←ここだけ変換

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(credentials)
    return client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# === 全キャッシュ一括取得 ===
@st.cache_data(show_spinner=False)
def load_entire_cache():
    sheet = connect_to_gspread()
    return sheet.get_all_records()

# === キャッシュ検索（既存関数を修正） ===
def load_cached_result(race_id, bloodline, full_cache=None):
    if full_cache is None:
        full_cache = load_entire_cache()

    matched_rows = [
        r for r in full_cache
        if str(r.get("race_id", "")) == str(race_id)
        and str(r.get("ウマ娘血統", "")) == str(bloodline)
    ]

    if not matched_rows:
        return []  # キャッシュ無し

    # ✅ 一致する全ての行が「該当なし」なら該当なしとみなす（空文字も含む）
    if all("該当なし" in str(r.get("該当箇所", "")) for r in matched_rows):
        return "該当なし"

    # それ以外は通常処理
    results = []
    for r in matched_rows:
        if "該当なし" not in str(r.get("該当箇所", "")):
            results.append({
                "馬名": r.get("馬名", ""),
                "該当箇所": r.get("該当箇所", ""),
                "競馬場": r.get("競馬場", ""),
                "レース": r.get("レース", "")
            })
    return results


# === キャッシュ保存（重複チェック付き） ===
def save_cached_result(rows, race_id=None, bloodline=None):
    sheet = connect_to_gspread()
    headers = ["馬名", "該当箇所", "競馬場", "レース", "ウマ娘血統", "race_id"]
    existing = sheet.get_all_records()

    # === 🧹 race_id と 血統が両方一致する行だけ削除 ===
    delete_indices = []
    for i, r in enumerate(existing):
        if str(r.get("race_id", "")) == str(race_id) and str(r.get("ウマ娘血統", "")) == str(bloodline):
            delete_indices.append(i + 2)  # +2 はヘッダ行 + 1-index

    if delete_indices:
        for i in sorted(delete_indices, reverse=True):
            sheet.delete_rows(i)
        time.sleep(1.2)

    # === 保存処理（該当なし対応）
    if rows == "該当なし":
        # 該当なし行として保存
        row = {
            "馬名": "",
            "該当箇所": "該当なし",
            "競馬場": "",
            "レース": "",
            "ウマ娘血統": bloodline,
            "race_id": race_id
        }
        sheet.append_row([row.get(h, "") for h in headers])
    elif isinstance(rows, list):
        for r in rows:
            r["ウマ娘血統"] = bloodline
            r["race_id"] = race_id
            sheet.append_row([r.get(h, "") for h in headers])

# === キャッシュ保存（重複チェック付き） ===
def save_cached_result(rows, race_id=None, bloodline=None):
    sheet = connect_to_gspread()
    headers = ["馬名", "該当箇所", "競馬場", "レース", "ウマ娘血統", "race_id"]
    existing = sheet.get_all_records()

    # === 🧹 race_id と 血統が両方一致する行だけ削除 ===
    delete_indices = []
    for i, r in enumerate(existing):
        if str(r.get("race_id", "")) == str(race_id) and str(r.get("ウマ娘血統", "")) == str(bloodline):
            delete_indices.append(i + 2)  # +2 はヘッダ行 + 1-index

    if delete_indices:
        for i in sorted(delete_indices, reverse=True):
            sheet.delete_rows(i)
        time.sleep(1.2)

    # === 📝 書き込み ===
    if not rows:
        dummy = {
            "馬名": "（該当なし）",
            "該当箇所": "該当なし",
            "競馬場": "",
            "レース": "",
            "ウマ娘血統": bloodline or "",
            "race_id": race_id or ""
        }
        sheet.append_row([dummy[h] for h in headers])
    else:
        values = [[row.get(h, "") for h in headers] for row in rows]
        sheet.append_rows(values)

    time.sleep(1.2)

# === 血統位置ラベル ===
def generate_position_labels():
    def dfs(pos, depth, max_depth):
        if depth > max_depth: return []
        result = [pos]
        result += dfs(pos + "父", depth + 1, max_depth)
        result += dfs(pos + "母", depth + 1, max_depth)
        return result
    return dfs("", 0, 5)[1:]
POSITION_LABELS = generate_position_labels()

# === ウマ娘血統データ読み込み ===
umamusume_df = pd.read_csv("umamusume.csv")
image_dict = dict(zip(umamusume_df["kettou"], umamusume_df["url"]))
name_to_kettou = dict(zip(umamusume_df["kettou"], umamusume_df["kettou"]))

# === 出走馬リンク取得 ===
def get_horse_links(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    res = requests.get(url, headers=HEADERS)
    res.encoding = "EUC-JP"
    soup = BeautifulSoup(res.text, "html.parser")
    horse_links = {}
    tables = soup.find_all("table", class_="RaceTable01")
    for table in tables:
        for a in table.find_all("a", href=True):
            if "/horse/" in a["href"]:
                name = a.get_text(strip=True)
                full_url = "https://db.netkeiba.com" + a["href"]
                if len(name) >= 2 and name not in horse_links:
                    horse_links[name] = full_url
    return horse_links

# === 血統取得 ===
def get_pedigree_with_positions(horse_url):
    horse_id = horse_url.rstrip("/").split("/")[-1]
    ped_url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
    res = requests.get(ped_url, headers=HEADERS)
    res.encoding = "EUC-JP"
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table", class_="blood_table")
    if not table:
        return {}
    names = {}
    td_list = table.find_all("td")
    for i, td in enumerate(td_list[:len(POSITION_LABELS)]):
        label = POSITION_LABELS[i]
        a = td.find("a")
        if a and a.text.strip():
            names[label] = a.text.strip()
    return names

# === マッチ判定 ===
def match_pedigree(pedigree_dict, target_name):
    matched_positions = []
    target_normalized = unicodedata.normalize("NFKC", target_name).strip().lower()
    for pos, name in pedigree_dict.items():
        key = unicodedata.normalize("NFKC", name).strip().lower()
        if key == target_normalized:
            matched_positions.append(pos)
    return matched_positions

# === HTMLテーブル変換（表示項目を制限） ===
def render_table_html(df):
    df = df[["馬名", "該当箇所", "競馬場", "レース"]]  # 表示用に列を限定
    df.insert(0, "No", range(1, len(df) + 1))  # No列追加
    table_html = "<table border='1' style='border-collapse:collapse; width:100%; font-size:16px;'>"
    table_html += "<thead><tr>" + "".join([f"<th>{col}</th>" for col in df.columns]) + "</tr></thead><tbody>"
    for _, row in df.iterrows():
        table_html += "<tr>" + "".join([f"<td>{row[col]}</td>" for col in df.columns]) + "</tr>"
    table_html += "</tbody></table>"
    return table_html

# === Streamlit UI ===
st.title("👧 ウマ娘逆引き血統サーチ")

# JRA日付
schedule_df = pd.read_csv("jra_2025_keibabook_schedule.csv")
schedule_df["日付"] = pd.to_datetime(
    schedule_df["年"].astype(str) + "/" + schedule_df["月日(曜日)"].str.extract(r"(\d{2}/\d{2})")[0],
    format="%Y/%m/%d"
)
today = pd.Timestamp.today()
past_31 = today - pd.Timedelta(days=31)
future_7 = today + pd.Timedelta(days=7)
schedule_df = schedule_df[schedule_df["日付"].between(past_31, future_7)]

available_dates = sorted(schedule_df["日付"].dt.strftime("%Y-%m-%d").unique(), reverse=True)
st.markdown("### 📅 開催日を選択")
selected_date = st.selectbox("", available_dates)

# ウマ娘選択
st.markdown("### 👧 ウマ娘を選択")
selected_umamusume = st.selectbox("", sorted(umamusume_df["kettou"]))
st.markdown("""
<div style='line-height: 1.5; font-size: 0,8em; color: gray;'>
あいうえお順に並んでいます。</div>
""", unsafe_allow_html=True)
target_kettou = name_to_kettou.get(selected_umamusume, "")
st.image(image_dict.get(selected_umamusume, ""), width=150)
st.markdown(f"選択したウマ娘：**{target_kettou}**")

# キャッシュ利用切替
use_cache = st.radio("キャッシュ利用", ["キャッシュを使う", "再取得する"]) == "キャッシュを使う"
st.markdown("""
<div style='line-height: 1.5; font-size: 0,8em; color: gray;'>
キャッシュ（検索結果の一時データ）があれば、結果をすぐに表示できます。<br>
基本的に「キャッシュを使う」を選択してください。<br><br>
「キャッシュを使う」を選択した場合でもデータがないは自動で情報取得が始まります。<br>
情報取得に数十秒～数分時間がかかりますのでお待ちください。<br>
</div>
""", unsafe_allow_html=True)

# 検索実行
selected_date_obj = pd.to_datetime(selected_date)
selected_rows = schedule_df[schedule_df["日付"] == selected_date_obj]
place_codes = {"札幌": "01", "函館": "02", "福島": "03", "新潟": "04", "東京": "05",
               "中山": "06", "中京": "07", "京都": "08", "阪神": "09", "小倉": "10"}

status_text = st.empty()
total_races = len(selected_rows) * 12
all_race_counter = 0
overall_progress = st.progress(0)

if st.button("🔍 ウマ娘血統サーチ開始！"):
    all_results = []
    full_cache = load_entire_cache()  # 🔁 1回だけ読み込んで共有

    for _, row in selected_rows.iterrows():
        year = row["年"]
        jj = place_codes.get(row["競馬場"], "")
        kk = f"{int(row['開催回']):02d}"
        dd = f"{int(row['日目']):02d}"
        place = row["競馬場"]

        place_status = st.empty()
        place_status.markdown(f"### 📍 {place} 競馬場の出走馬の処理中...")
        place_progress = st.progress(0)
        place_race_counter = 0
        place_results = []

        for nn in range(1, 13):  # 1R〜12R
            race_id = f"{year}{jj}{kk}{dd}{nn:02d}"

            # キャッシュ確認
            if use_cache:
                cached = load_cached_result(race_id, target_kettou, full_cache=full_cache)
                if cached == "該当なし":
                    place_race_counter += 1
                    all_race_counter += 1
                    place_progress.progress(min(place_race_counter / 12, 1.0))
                    overall_progress.progress(min(all_race_counter / total_races, 1.0))
                    continue
                elif cached:
                    all_results.extend(cached)
                    place_results.extend(cached)
                    place_race_counter += 1
                    all_race_counter += 1
                    place_progress.progress(min(place_race_counter / 12, 1.0))
                    overall_progress.progress(min(all_race_counter / total_races, 1.0))
                    continue

            # スクレイピング実行
            horse_links = get_horse_links(race_id)
            race_results = []

            for i, (name, link) in enumerate(horse_links.items(), 1):
                status_text.text(f"検索中…{place} {nn}R {i}/{len(horse_links)}頭目")
                try:
                    pedigree = get_pedigree_with_positions(link)
                    matched = match_pedigree(pedigree, target_kettou)
                    if matched:
                        result = {
                            "馬名": name,
                            "該当箇所": "、".join(matched),
                            "競馬場": place,
                            "レース": f"{nn}R",
                            "ウマ娘血統": target_kettou,
                            "race_id": race_id,
                        }
                        race_results.append(result)
                except Exception as e:
                    st.error(f"{name} の照合エラー：{e}")
                time.sleep(0.3)

            # 結果保存＋進捗更新
            save_cached_result(race_results, race_id=race_id, bloodline=target_kettou)
            place_results.extend(race_results)
            all_results.extend(race_results)

            place_race_counter += 1
            all_race_counter += 1
            place_progress.progress(min(place_race_counter / 12, 1.0))
            overall_progress.progress(min(all_race_counter / total_races, 1.0))

        if place_results:
            st.markdown(f"### ✅ {place} 競馬場の該当馬一覧")
            df = pd.DataFrame(place_results)
            st.markdown(render_table_html(df), unsafe_allow_html=True)

