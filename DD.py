import streamlit as st
import pandas as pd
import unicodedata
import time
from bs4 import BeautifulSoup
import requests

# === 設定 ===
HEADERS = {"User-Agent": "Mozilla/5.0"}

# === ウマ娘血統データの読み込み ===
umamusume_df = pd.read_csv("umamusume.csv")
image_dict = dict(zip(umamusume_df["kettou"], umamusume_df["url"]))
name_to_kettou = dict(zip(umamusume_df["kettou"], umamusume_df["kettou"]))

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

# === HTMLテーブル変換 ===
def render_table_html(df):
    df.insert(0, "No", range(1, len(df) + 1))
    table_html = "<table border='1' style='border-collapse:collapse; width:100%; font-size:16px;'>"
    table_html += "<thead><tr>" + "".join([f"<th>{col}</th>" for col in df.columns]) + "</tr></thead><tbody>"
    for _, row in df.iterrows():
        table_html += "<tr>" + "".join([f"<td>{row[col]}</td>" for col in df.columns]) + "</tr>"
    table_html += "</tbody></table>"
    return table_html

# === UI ===
st.title("👧 ウマ娘逆引き血統サーチ")

# === JRA開催日CSVの読み込み ===
schedule_df = pd.read_csv("jra_2025_keibabook_schedule.csv")
schedule_df["日付"] = pd.to_datetime(
    schedule_df["年"].astype(str) + "/" + schedule_df["月日(曜日)"].str.extract(r"(\d{2}/\d{2})")[0],
    format="%Y/%m/%d"
)

# 過去31日 + 未来7日 の開催日を表示
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

# レース情報取得
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
        results = []
        year = row["年"]
        jj = place_codes.get(row["競馬場"], "")
        kk = f"{int(row['開催回']):02d}"
        dd = f"{int(row['日目']):02d}"

        place_status = st.empty()
        place_status.markdown(f"### 📍 {row['競馬場']} 競馬場の出走馬の処理中...")
        place_progress = st.progress(0)
        place_race_counter = 0

        for race_num in range(1, 13):
            race_id = f"{year}{jj}{kk}{dd}{race_num:02d}"
            horse_links = get_horse_links(race_id)
            horse_total = len(horse_links)
            for i, (name, link) in enumerate(horse_links.items(), 1):
                status_text.text(f"検索中…{row['競馬場']}{race_num}R {i}/{horse_total}頭目 【{place_race_counter+1}/12レース目】")
                try:
                    pedigree = get_pedigree_with_positions(link)
                    matched = match_pedigree(pedigree, target_kettou)
                    if matched:
                        results.append({
                            "競馬場": row["競馬場"],
                            "レース": f"{race_num}R",
                            "馬名": name,
                            "該当箇所": "、".join(matched)
                        })
                except Exception as e:
                    st.error(f"{name} の照合エラー：{e}")
                time.sleep(0.3)

            place_race_counter += 1
            all_race_counter += 1
            place_progress.progress(min(place_race_counter / 12, 1.0))
            overall_progress.progress(min(all_race_counter / total_races, 1.0))

        place_status.markdown(f"### ✅ {row['競馬場']} 競馬場の出走馬の抽出結果")

        if results:
            df = pd.DataFrame(results)
            html = render_table_html(df)
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.info(f"{row['競馬場']} では該当馬なし")
