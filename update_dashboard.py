#!/usr/bin/env python3
"""
OnePriceCoffee Dashboard Auto-Updater
Запускается GitHub Actions каждый день в 8:00 МСК (5:00 UTC).
Обновляет index.html: выручка из Google Sheets + посты VK + посты TG.
"""

import re
import csv
import json
import time
import datetime
import io
import sys
import urllib.request
import urllib.error

# ── Конфиг ───────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1Gx7-FIccn0qLkH7aGKzpDSu6Ixq2xh_HTiSZR2yoiBA"
# Ежедневные листы "Иваново Ленина" (gid=649208657) и "Иваново ТЦ Сер. город" (gid=2039636677)
SHEETS_LENINA_URL = (
    "https://docs.google.com/spreadsheets/d/"
    + SPREADSHEET_ID
    + "/export?format=csv&gid=649208657"
)
SHEETS_SEREBR_URL = (
    "https://docs.google.com/spreadsheets/d/"
    + SPREADSHEET_ID
    + "/export?format=csv&gid=2039636677"
)
VK_GROUP   = "onepricecoffee_ivanovo"
TG_CHANNEL = "opc_ivanovo"
HTML_FILE  = "index.html"

TODAY = datetime.date.today()
LOG = []

def log(msg):
    print(msg, flush=True)
    LOG.append(msg)

# ── Утилиты ──────────────────────────────────────────────────────────────────
def fetch(url, timeout=20):
    """HTTP GET → str. Возвращает None при ошибке."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        log(f"  [WARN] fetch error {url[:60]}: {e}")
        return None


def read_html():
    with open(HTML_FILE, encoding="utf-8") as f:
        return f.read()


def write_html(content):
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(content)


# ── Google Sheets → ежедневные данные ────────────────────────────────────────
def parse_daily_sheet(csv_text, location_name):
    """
    Парсит лист с ежедневными данными (формат: дата DD.MM.YYYY в первом столбце,
    далее числовые данные — выручка, чеки, ср.чек).
    Возвращает dict: {"YYYY-MM-DD": {"revenue": X, "customers": Y, "avg_check": Z}}
    """
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    result = {}

    for row in rows:
        if len(row) < 20:
            continue
        # Дата в колонке [3], формат DD.MM.YYYY
        date_raw = row[3].strip()
        dm = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})$', date_raw)
        if not dm:
            continue
        day, month, year = dm.group(1), dm.group(2), dm.group(3)
        date_iso = f"{year}-{month}-{day}"

        def num(s):
            s = s.strip().replace("\xa0","").replace("\u00a0","").replace(" ","").replace(",",".")
            try: return float(s)
            except: return 0.0

        # Факт выручка [5], чеки факт [19], ср.чек факт [22]
        revenue   = int(num(row[5]))
        customers = int(num(row[19]))
        avg_check = int(num(row[22]))

        if revenue <= 0:
            continue
        if customers <= 0:
            customers = max(1, round(revenue / 320))
        if avg_check <= 0:
            avg_check = round(revenue / customers)

        result[date_iso] = {"revenue": revenue, "customers": customers, "avg_check": avg_check}

    log(f"  {location_name}: спарсено {len(result)} дней")
    return result


def fetch_sheets_revenue():
    """
    Загружает ежедневные данные из листов Иваново.
    Возвращает dict:
      {
        "lenina": {"YYYY-MM-DD": {revenue, customers, avg_check}, ...},
        "serebr": {"YYYY-MM-DD": {revenue, customers, avg_check}, ...},
      }
    """
    log("📊 Загрузка Google Sheets (ежедневные листы)...")
    result = {}

    raw_lenina = fetch(SHEETS_LENINA_URL)
    if raw_lenina:
        result["lenina"] = parse_daily_sheet(raw_lenina, "Пр. Ленина")
    else:
        log("  [ERR] Не удалось загрузить лист Ленина")

    raw_serebr = fetch(SHEETS_SEREBR_URL)
    if raw_serebr:
        result["serebr"] = parse_daily_sheet(raw_serebr, "ТЦ Серебряный")
    else:
        log("  [ERR] Не удалось загрузить лист Серебряный")

    return result if result else None


def extract_existing_revenue(html):
    """
    Читает массив revenue из HTML.
    Возвращает (records, max_id, sum_rev_by_loc, sum_cust_by_loc).
    """
    m = re.search(r'let revenue\s*=\s*\[(.*?)\];', html, re.DOTALL)
    if not m:
        return [], 30, {}, {}
    block = m.group(1)
    entries = re.findall(
        r'\{id:(\d+),\s*date:"([^"]+)",\s*location:"([^"]+)",\s*'
        r'revenue:(\d+),\s*customers:(\d+),\s*avgCheck:(\d+)\}',
        block
    )
    records = []
    max_id = 0
    sum_rev  = {}
    sum_cust = {}
    for id_, date, loc, rev, cust, avg in entries:
        loc_key = "lenina" if "Ленина" in loc else "serebr"
        records.append({
            "id": int(id_), "date": date, "location": loc,
            "revenue": int(rev), "customers": int(cust), "avg_check": int(avg),
            "loc_key": loc_key
        })
        max_id = max(max_id, int(id_))
        sum_rev[loc_key]  = sum_rev.get(loc_key, 0)  + int(rev)
        sum_cust[loc_key] = sum_cust.get(loc_key, 0) + int(cust)

    return records, max_id, sum_rev, sum_cust


def build_new_revenue_entries(sheets, existing_records, max_id, known_sum_rev, known_sum_cust):
    """
    Добавляет только те дни из Sheets, которых ещё нет в HTML.
    Берёт точные ежедневные данные.
    """
    new_entries = []
    next_id = max_id + 1
    yesterday = (TODAY - datetime.timedelta(days=1)).isoformat()

    for loc_key, loc_name in [("lenina", "Проспект Ленина"), ("serebr", "ТЦ Серебряный Город")]:
        if loc_key not in sheets:
            continue
        daily = sheets[loc_key]   # dict: date_iso -> {revenue, customers, avg_check}
        existing_dates = set(r["date"] for r in existing_records if r["loc_key"] == loc_key)

        # Добавляем дни которых нет, и не позже вчера (сегодняшние могут быть неполными)
        new_dates = sorted([d for d in daily if d not in existing_dates and d <= yesterday])
        if not new_dates:
            log(f"  {loc_name}: нет новых дней")
            continue

        log(f"  {loc_name}: добавляю {len(new_dates)} дн. ({new_dates[0]} … {new_dates[-1]})")
        for date_iso in new_dates:
            d = daily[date_iso]
            new_entries.append({
                "id": next_id,
                "date": date_iso,
                "location": loc_name,
                "revenue":   d["revenue"],
                "customers": d["customers"],
                "avg_check": d["avg_check"],
            })
            next_id += 1

    return new_entries


def update_revenue_in_html(html, new_entries):
    """Добавляет новые записи в конец массива revenue."""
    if not new_entries:
        return html, 0

    lines = []
    for e in new_entries:
        lines.append(
            f'  {{id:{e["id"]},date:"{e["date"]}",location:"{e["location"]}",'
            f'      revenue:{e["revenue"]}, customers:{e["customers"]}, avgCheck:{e["avg_check"]}}},'
        )
    insert = "\n" + "\n".join(lines)
    html = re.sub(r'(\];)\s*\n// Контент-план', insert + r'\n\1\n// Контент-план', html, count=1)
    return html, len(new_entries)


# ── VK ────────────────────────────────────────────────────────────────────────
def get_last_post_date(html, platform):
    m_all = re.findall(
        r'\{id:[^,]+,\s*date:"(\d{4}-\d{2}-\d{2})",platform:"' + platform + '"',
        html
    )
    return max(m_all) if m_all else "2026-04-01"


def get_current_subs(html):
    m = re.search(r'subVk:(\d+)', html)
    sub_vk = int(m.group(1)) if m else 568
    m = re.search(r'subTg:(\d+)', html)
    sub_tg = int(m.group(1)) if m else 68
    return sub_vk, sub_tg


def get_min_post_id(html):
    ids = re.findall(r'\{id:(-?\d+),\s*date:', html)
    return min(int(x) for x in ids) if ids else 0


def fetch_vk_posts(last_vk_date):
    log("📘 Получение данных VK...")
    url = f"https://vk.com/{VK_GROUP}"
    html = fetch(url)
    if not html:
        log("  [ERR] VK недоступен")
        return [], None

    sub_vk = None
    m = re.search(r'(\d[\d\s\xa0]+)\s*(?:подписчик|участник)', html, re.IGNORECASE)
    if m:
        sub_vk = int(re.sub(r'\D', '', m.group(1)))
        log(f"  VK подписчики: {sub_vk}")

    dates_found = re.findall(r'datetime="(\d{4}-\d{2}-\d{2})', html)
    texts_found = re.findall(r'class="wall_post_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)

    posts = []
    seen  = set()
    text_pool = list(texts_found)

    for date_str in dates_found:
        if date_str <= last_vk_date or date_str in seen:
            continue
        seen.add(date_str)

        topic = "Новый пост"
        if text_pool:
            raw = text_pool.pop(0)
            clean = re.sub(r'<[^>]+>', '', raw).strip()
            clean = re.sub(r'\s+', ' ', clean)[:75]
            if clean:
                topic = clean

        posts.append({"date": date_str, "type": "photo", "topic": topic,
                      "likes": 0, "comments": 0, "reposts": 0})

    posts.sort(key=lambda x: x["date"], reverse=True)
    log(f"  VK новых постов: {len(posts)}")
    return posts, sub_vk


def fetch_tg_posts(last_tg_date):
    log("✈️  Получение данных Telegram...")
    url = f"https://t.me/s/{TG_CHANNEL}"
    html = fetch(url)
    if not html:
        log("  [ERR] Telegram недоступен")
        return [], None

    sub_tg = None
    m = re.search(r'(\d[\d\s\xa0]+)\s*(?:subscriber|подписчик)', html, re.IGNORECASE)
    if m:
        sub_tg = int(re.sub(r'\D', '', m.group(1)))
        log(f"  TG подписчики: {sub_tg}")

    all_times = re.findall(r'datetime="(\d{4}-\d{2}-\d{2})', html)
    all_views = re.findall(r'class="tgme_widget_message_views"[^>]*>([\d\s,KkMm]+)<', html)
    all_texts = re.findall(r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)

    posts = []
    seen  = set()

    for i, date_str in enumerate(all_times):
        if date_str <= last_tg_date or date_str in seen:
            continue
        seen.add(date_str)

        views = 0
        if i < len(all_views):
            v = all_views[i].strip().replace(" ", "").replace(",", "")
            if v.lower().endswith("k"):
                views = int(float(v[:-1]) * 1000)
            elif re.match(r'^\d+$', v):
                views = int(v)

        topic = "Новый пост"
        if i < len(all_texts):
            clean = re.sub(r'<[^>]+>', '', all_texts[i]).strip()
            clean = re.sub(r'\s+', ' ', clean)[:75]
            if clean:
                topic = clean

        posts.append({"date": date_str, "type": "photo", "topic": topic,
                      "reach": views, "likes": 0})

    posts.sort(key=lambda x: x["date"], reverse=True)
    log(f"  TG новых постов: {len(posts)}")
    return posts, sub_tg


def build_post_js(post, pid, platform, sub_vk, sub_tg):
    topic = post["topic"].replace('"', "'")[:75]
    reach = post.get("reach", 0)
    likes = post.get("likes", 0)
    return (
        f'  {{id:{pid}, date:"{post["date"]}",platform:"{platform}",'
        f'       postType:"{post.get("type","photo")}", topic:"{topic}",'
        f'   reach:{reach},  likes:{likes},  comments:0, reposts:0,'
        f' subVk:{sub_vk}, subTg:{sub_tg}}},'
    )


def inject_posts(html, vk_posts, tg_posts, sub_vk, sub_tg):
    if not vk_posts and not tg_posts:
        return html, 0, 0

    pid = get_min_post_id(html) - 1

    if vk_posts:
        vk_lines = "\n".join(build_post_js(p, pid - i, "vk", sub_vk, sub_tg)
                             for i, p in enumerate(vk_posts)) + "\n"
        pid -= len(vk_posts)
        html = re.sub(r'(// VK\n)', r'\g<1>' + vk_lines, html, count=1)

    if tg_posts:
        tg_lines = "\n".join(build_post_js(p, pid - i, "telegram", sub_vk, sub_tg)
                             for i, p in enumerate(tg_posts)) + "\n"
        html = re.sub(r'(// Telegram @opc_ivanovo\n)', r'\g<1>' + tg_lines, html, count=1)

    return html, len(vk_posts), len(tg_posts)


def update_sub_comment(html, sub_vk, sub_tg):
    date_str = TODAY.strftime("%d.%m.%Y")
    html = re.sub(
        r'// VK: vk\.ru/onepricecoffee_ivanovo — \d+ подп\..*',
        f'// VK: vk.ru/onepricecoffee_ivanovo — {sub_vk} подп. (обновлено {date_str})',
        html
    )
    html = re.sub(
        r'// TG: t\.me/opc_ivanovo — \d+ подп\.',
        f'// TG: t.me/opc_ivanovo — {sub_tg} подп.',
        html
    )
    return html


def update_task_statuses(html):
    yesterday = (TODAY - datetime.timedelta(days=1)).isoformat()

    def replace_status(m):
        if m.group(1) <= yesterday:
            return m.group(0).replace('status:"в работе"', 'status:"выполнено"')
        return m.group(0)

    return re.sub(r'due:"(\d{4}-\d{2}-\d{2})"[^}]*status:"в работе"',
                  replace_status, html)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log(f"\n{'='*60}")
    log(f"🚀 OnePriceCoffee Dashboard Updater — {TODAY}")
    log(f"{'='*60}\n")

    html = read_html()
    cur_sub_vk, cur_sub_tg = get_current_subs(html)
    log(f"Текущие подписчики: VK={cur_sub_vk}, TG={cur_sub_tg}\n")

    # ── 1. Выручка ──────────────────────────────────────────────────────────
    sheets_data = fetch_sheets_revenue()
    revenue_added = 0
    if sheets_data:
        records, max_id, sum_rev, sum_cust = extract_existing_revenue(html)
        new_rev = build_new_revenue_entries(sheets_data, records, max_id, sum_rev, sum_cust)
        if new_rev:
            html, revenue_added = update_revenue_in_html(html, new_rev)
            log(f"  ✅ Выручка: добавлено {revenue_added} записей")
        else:
            log("  ℹ️  Выручка: нет новых данных")
    log("")

    # ── 2. VK ───────────────────────────────────────────────────────────────
    last_vk = get_last_post_date(html, "vk")
    log(f"Последний VK-пост: {last_vk}")
    vk_posts, new_sub_vk = fetch_vk_posts(last_vk)
    sub_vk = new_sub_vk if new_sub_vk else cur_sub_vk
    log("")

    # ── 3. Telegram ─────────────────────────────────────────────────────────
    last_tg = get_last_post_date(html, "telegram")
    log(f"Последний TG-пост: {last_tg}")
    tg_posts, new_sub_tg = fetch_tg_posts(last_tg)
    sub_tg = new_sub_tg if new_sub_tg else cur_sub_tg
    log("")

    # ── 4. Вставка постов ───────────────────────────────────────────────────
    html, vk_added, tg_added = inject_posts(html, vk_posts, tg_posts, sub_vk, sub_tg)
    html = update_sub_comment(html, sub_vk, sub_tg)
    html = update_task_statuses(html)

    # ── 5. Сохранение ───────────────────────────────────────────────────────
    write_html(html)
    log(f"\n✅ Готово: VK +{vk_added} | TG +{tg_added} | Выручка +{revenue_added}")
    log(f"   Подписчики: VK={sub_vk} | TG={sub_tg}")

    return {"vk_added": vk_added, "tg_added": tg_added,
            "revenue_added": revenue_added, "sub_vk": sub_vk, "sub_tg": sub_tg}


if __name__ == "__main__":
    result = main()
    summary = (
        f"VK: +{result['vk_added']} постов ({result['sub_vk']} подп.) | "
        f"TG: +{result['tg_added']} постов ({result['sub_tg']} подп.) | "
        f"Выручка: +{result['revenue_added']} записей"
    )
    print(f"\nSUMMARY: {summary}")
    with open("update_summary.txt", "w") as f:
        f.write(summary)
