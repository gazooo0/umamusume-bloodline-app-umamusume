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

# === 定数設定 ===
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

def load_cached_result(race_id, bloodline):
    sheet = connect_to_gspread()
    records = sheet.get_all_records()
    results = []
    for r in records:
        race_id_match = str(r.get("race_id", "")).strip() == str(race_id).strip()
        bloodline_match = str(r.get("ウマ娘血統", "")).strip() == str(bloodline).strip()
        if race_id_match and bloodline_match:
            # 表示に使わない項目を除外
            filtered = {
                "馬名": r.get("馬名", ""),
                "該当箇所": r.get("該当箇所", ""),
                "競馬場": r.get("競馬場", ""),
                "レース": r.get("レース", "")
            }
            results.append(filtered)
    return results

def save_cached_result(rows):
    sheet = connect_to_gspread()
    headers = ["馬名", "該当箇所", "競馬場", "レース", "ウマ娘血統", "race_id"]
    values = [[row.get(h, "") for h in headers] for row in rows]
    sheet.append_rows(values)

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
selected_date = st.selectbox("📅 開催日を選択", available_dates)

# ウマ娘選択
selected_umamusume = st.selectbox("👧 ウマ娘を選択", sorted(umamusume_df["kettou"]))
target_kettou = name_to_kettou.get(selected_umamusume, "")
st.image(image_dict.get(selected_umamusume, ""), width=150)
st.markdown(f"選択したウマ娘：**{target_kettou}**")

# キャッシュ利用切替
use_cache = st.radio("キャッシュ利用", ["キャッシュを使う", "再取得する"]) == "キャッシュを使う"

# 検索実行
selected_date_obj = pd.to_datetime(selected_date)
selected_rows = schedule_df[schedule_df["日付"] == selected_date_obj]
place_codes = {"札幌": "01", "函館": "02", "福島": "03", "新潟": "04", "東京": "05",
               "中山": "06", "中京": "07", "京都": "08", "阪神": "09", "小倉": "10"}

status_text = st.empty()
total_races = len(selected_rows) * 12
all_race_counter = 0
overall_progress = st.progress(0)

if st.button("🔍 該当馬を検索"):
    for _, row in selected_rows.iterrows():
        year = row["年"]
        jj = place_codes.get(row["競馬場"], "")
        kk = f"{int(row['開催回']):02d}"
        dd = f"{int(row['日目']):02d}"

        place_status = st.empty()
        place_status.markdown(f"### 📍 {row['競馬場']} 競馬場の出走馬の処理中...")
        place_progress = st.progress(0)
        place_race_counter = 0

        all_results = []
        place_results = []  # ← 競馬場ごとの結果をここにまとめる

        for race_num in range(1, 13):
            race_id = f"{year}{jj}{kk}{dd}{race_num:02d}"

            # キャッシュ確認
            if use_cache:
                cached = load_cached_result(race_id, target_kettou)
                if cached:
                    place_results.extend(cached)
                    all_race_counter += 1
                    place_race_counter += 1
                    place_progress.progress(min(place_race_counter / 12, 1.0))
                    overall_progress.progress(min(all_race_counter / total_races, 1.0))
                    continue

            horse_links = get_horse_links(race_id)
            race_results = []

            for i, (name, link) in enumerate(horse_links.items(), 1):
                status_text.text(f"検索中…{row['競馬場']}{race_num}R {i}/{len(horse_links)}頭目")
                try:
                    pedigree = get_pedigree_with_positions(link)
                    matched = match_pedigree(pedigree, target_kettou)
                    if matched:
                        race_results.append({
                            "馬名": name,
                            "該当箇所": "、".join(matched),
                            "競馬場": row["競馬場"],
                            "レース": f"{race_num}R",
                            "ウマ娘血統": target_kettou,
                            "race_id": race_id
                        })
                except Exception as e:
                    st.error(f"{name} の照合エラー：{e}")
                time.sleep(0.3)

            if race_results:
                df = pd.DataFrame(race_results)
                df_show = df[["馬名", "該当箇所", "競馬場", "レース"]]  # 表示用（4列）
                html = render_table_html(df_show)

                st.markdown(f"#### 🎯 {row['競馬場']} {race_num}R 該当馬", unsafe_allow_html=True)
                st.markdown(html, unsafe_allow_html=True)

                save_cached_result(race_results)
                place_results.extend(race_results)

            all_race_counter += 1
            place_race_counter += 1
            place_progress.progress(min(place_race_counter / 12, 1.0))
            overall_progress.progress(min(all_race_counter / total_races, 1.0))

        place_status.markdown(f"### ✅ {row['競馬場']} 競馬場の出走馬の抽出完了")

        # === 競馬場ごとに一括表示 ===
        if place_results:
            st.markdown(f"### 🏇 {row['競馬場']} 競馬場の該当馬一覧")
            df = pd.DataFrame(place_results)
            html = render_table_html(df)
            st.markdown(html, unsafe_allow_html=True)
