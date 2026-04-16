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
SHEETS_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Gx7-FIccn0qLkH7aGKzpDSu6Ixq2xh_HTiSZR2yoiBA"
    "/export?format=csv&gid=1752437281"
)
VK_GROUP = "onepricecoffee_ivanovo"
TG_CHANNEL = "opc_ivanovo"
HTML_FILE = "index.html"

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

# ── Google Sheets → выручка ───────────────────────────────────────────────────
def fetch_sheets_revenue():
    """
    Возвращает dict:
      {
        "lenina":   {"revenue": 367162, "customers": 1188, "avg_check": 309},
        "serebr":   {"revenue": 563312, "customers": 1749, "avg_check": 322},
      }
    или None при ошибке.
    """
    log("📊 Загрузка Google Sheets...")
    raw = fetch(SHEETS_CSV_URL)
    if not raw:
        log("  [ERR] Не удалось загрузить Sheets.")
        return None

    reader = csv.reader(io.StringIO(raw))
    rows = list(reader)

    result = {}
    for row in rows:
        if not row:
            continue
        name = row[0].strip()

        # Иваново — пр. Ленина
        if "Иваново ленина" in name or ("Иваново" in name and "ленина" in name.lower()):
            try:
                rev_str = row[10].strip().replace(" ", "").replace("\xa0", "")
                chk_str = row[15].strip().replace(" ", "").replace("\xa0", "")
                avg_str = row[18].strip().replace(" ", "").replace("\xa0", "")
                rev = int(float(rev_str)) if rev_str else 0
                chk = int(float(chk_str)) if chk_str else 0
                avg = int(float(avg_str)) if avg_str else 309
                result["lenina"] = {"revenue": rev, "customers": chk, "avg_check": avg}
                log(f"  Ленина: выручка={rev:,}, чеки={chk}, ср.чек={avg}")
            except Exception as e:
                log(f"  [WARN] Парсинг Ленина: {e} | row={row[:20]}")

        # Иваново — ТЦ Серебряный город
        if "Серебряный город" in name and "Иваново" in name:
            try:
                rev_str = row[10].strip().replace(" ", "").replace("\xa0", "")
                chk_str = row[15].strip().replace(" ", "").replace("\xa0", "")
                avg_str = row[18].strip().replace(" ", "").replace("\xa0", "")
                rev = int(float(rev_str)) if rev_str else 0
                chk = int(float(chk_str)) if chk_str else 0
                avg = int(float(avg_str)) if avg_str else 322
                result["serebr"] = {"revenue": rev, "customers": chk, "avg_check": avg}
                log(f"  Серебряный: выручка={rev:,}, чеки={chk}, ср.чек={avg}")
            except Exception as e:
                log(f"  [WARN] Парсинг Серебряный: {e} | row={row[:20]}")

    if "lenina" not in result or "serebr" not in result:
        log(f"  [WARN] Нашли только: {list(result.keys())}")
    return result if result else None


def extract_existing_revenue(html):
    """
    Читает массив revenue из HTML.
    Возвращает (list_of_dicts, last_id, max_date_per_location).
    """
    m = re.search(r'let revenue\s*=\s*\[(.*?)\];', html, re.DOTALL)
    if not m:
        return [], 30, {}
    block = m.group(1)
    entries = re.findall(
        r'\{id:(\d+),\s*date:"([^"]+)",\s*location:"([^"]+)",\s*'
        r'revenue:(\d+),\s*customers:(\d+),\s*avgCheck:(\d+)\}',
        block
    )
    records = []
    max_id = 0
    max_date = {}
    sum_rev = {}
    sum_cust = {}
    for id_, date, loc, rev, cust, avg in entries:
        loc_key = "lenina" if "Ленина" in loc else "serebr"
        records.append({
            "id": int(id_), "date": date, "location": loc,
            "revenue": int(rev), "customers": int(cust), "avg_check": int(avg),
            "loc_key": loc_key
        })
        max_id = max(max_id, int(id_))
        if loc_key not in max_date or date > max_date[loc_key]:
            max_date[loc_key] = date
        sum_rev[loc_key] = sum_rev.get(loc_key, 0) + int(rev)
        sum_cust[loc_key] = sum_cust.get(loc_key, 0) + int(cust)

    return records, max_id, max_date, sum_rev, sum_cust


def build_new_revenue_entries(sheets, existing_records, max_id, known_sum_rev, known_sum_cust):
    """
    Вычисляет новые дни на основе разницы между Sheets и тем, что уже есть.
    Возвращает список новых записей.
    """
    new_entries = []
    next_id = max_id + 1

    for loc_key, loc_name in [("lenina", "Проспект Ленина"), ("serebr", "ТЦ Серебряный Город")]:
        if loc_key not in sheets:
            continue
        total_rev = sheets[loc_key]["revenue"]
        total_cust = sheets[loc_key]["customers"]
        avg_check = sheets[loc_key]["avg_check"]

        already_rev = known_sum_rev.get(loc_key, 0)
        already_cust = known_sum_cust.get(loc_key, 0)

        delta_rev = total_rev - already_rev
        delta_cust = total_cust - already_cust

        if delta_rev <= 0:
            log(f"  {loc_name}: нет новых данных (delta={delta_rev})")
            continue

        # Определяем даты, которые уже есть
        existing_dates = set(
            r["date"] for r in existing_records if r["loc_key"] == loc_key
        )

        # Находим последнюю известную дату
        last_date_str = max(existing_dates) if existing_dates else "2026-04-15"
        last_date = datetime.date.fromisoformat(last_date_str)

        # Генерируем отсутствующие дни до вчера (данные за сегодня ещё неполные)
        yesterday = TODAY - datetime.timedelta(days=1)
        missing_dates = []
        d = last_date + datetime.timedelta(days=1)
        while d <= yesterday:
            if d.isoformat() not in existing_dates:
                missing_dates.append(d)
            d += datetime.timedelta(days=1)

        if not missing_dates:
            log(f"  {loc_name}: нет пропущенных дат")
            continue

        log(f"  {loc_name}: добавляю {len(missing_dates)} дней, delta_rev={delta_rev:,}")

        # Распределяем пропорционально — смотрим на аналогичные дни недели в истории
        weekday_rev = {}
        weekday_cust = {}
        weekday_count = {}
        for r in existing_records:
            if r["loc_key"] != loc_key:
                continue
            d = datetime.date.fromisoformat(r["date"])
            wd = d.weekday()
            weekday_rev[wd] = weekday_rev.get(wd, 0) + r["revenue"]
            weekday_cust[wd] = weekday_cust.get(wd, 0) + r["customers"]
            weekday_count[wd] = weekday_count.get(wd, 0) + 1

        # Средний вес по дням недели
        weights = {}
        for wd in range(7):
            if wd in weekday_count and weekday_count[wd] > 0:
                weights[wd] = weekday_rev[wd] / weekday_count[wd]
            else:
                weights[wd] = delta_rev / len(missing_dates)

        total_weight = sum(weights[d.weekday()] for d in missing_dates)
        if total_weight == 0:
            total_weight = len(missing_dates)

        rev_left = delta_rev
        cust_left = delta_cust

        for i, d in enumerate(missing_dates):
            wd = d.weekday()
            w = weights[wd] / total_weight
            if i < len(missing_dates) - 1:
                day_rev = round(delta_rev * w)
                day_cust = round(delta_cust * w)
            else:
                day_rev = rev_left
                day_cust = cust_left

            day_rev = max(day_rev, 0)
            day_cust = max(day_cust, 1)
            day_avg = round(day_rev / day_cust) if day_cust else avg_check

            rev_left -= day_rev
            cust_left -= day_cust

            new_entries.append({
                "id": next_id,
                "date": d.isoformat(),
                "location": loc_name,
                "revenue": day_rev,
                "customers": day_cust,
                "avg_check": day_avg,
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

    # Вставляем перед закрывающим ];
    html = re.sub(r'(\];)\s*\n// Контент-план', insert + r'\n\1\n// Контент-план', html, count=1)
    return html, len(new_entries)

# ── VK ────────────────────────────────────────────────────────────────────────
def fetch_vk_posts(last_vk_date):
    """
    Парсит публичную страницу VK.
    Возвращает (список новых постов, число подписчиков).
    """
    log("📘 Получение данных VK...")
    url = f"https://vk.com/{VK_GROUP}"
    html = fetch(url)
    if not html:
        log("  [ERR] VK недоступен")
        return [], None

    # Подписчики
    sub_vk = None
    m = re.search(r'(\d[\d\s]+)\s*(?:подписчик|участник)', html, re.IGNORECASE)
    if m:
        sub_vk = int(m.group(1).replace(" ", "").replace("\xa0", ""))
        log(f"  VK подписчики: {sub_vk}")

    # Посты — ищем блоки с датой и текстом
    posts = []
    # Паттерн для поста: ищем post_id и дату
    post_blocks = re.findall(
        r'data-post-id="[^"]*_(\d+)".*?'
        r'class="[^"]*post_date[^"]*"[^>]*>.*?'
        r'<time[^>]*datetime="([^"]+)"[^>]*>.*?'
        r'</time>.*?'
        r'(?:class="[^"]*post_text[^"]*"[^>]*>(.*?)</div>)?',
        html, re.DOTALL
    )

    # Альтернативный более простой парсинг — через datetime атрибут
    dates_found = re.findall(r'datetime="(\d{4}-\d{2}-\d{2})', html)
    texts_found = re.findall(r'class="wall_post_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)

    seen_dates = set()
    for date_str in dates_found:
        if date_str <= last_vk_date or date_str in seen_dates:
            continue
        seen_dates.add(date_str)

        # Определяем тип поста (упрощённо — photo если есть photo, video если video)
        post_type = "photo"  # default

        # Текст поста — берём первый доступный для этой даты
        topic = "Новый пост"
        for t in texts_found:
            clean = re.sub(r'<[^>]+>', '', t).strip()[:80]
            clean = re.sub(r'\s+', ' ', clean)
            if clean:
                topic = clean
                texts_found.remove(t)
                break

        posts.append({
            "date": date_str,
            "type": post_type,
            "topic": topic,
            "likes": 0,
            "comments": 0,
            "reposts": 0,
        })

    posts.sort(key=lambda x: x["date"], reverse=True)
    log(f"  VK новых постов: {len(posts)}")
    return posts, sub_vk


def fetch_tg_posts(last_tg_date):
    """
    Парсит t.me/s/opc_ivanovo.
    Возвращает (список новых постов, число подписчиков).
    """
    log("✈️  Получение данных Telegram...")
    url = f"https://t.me/s/{TG_CHANNEL}"
    html = fetch(url)
    if not html:
        log("  [ERR] Telegram недоступен")
        return [], None

    # Подписчики
    sub_tg = None
    m = re.search(r'(\d[\d\s]+)\s*(?:subscriber|подписчик)', html, re.IGNORECASE)
    if m:
        sub_tg = int(m.group(1).replace(" ", "").replace("\xa0", ""))
        log(f"  TG подписчики: {sub_tg}")

    # Посты: <time datetime="YYYY-MM-DDTHH:MM:SS+00:00">
    post_blocks = re.findall(
        r'<div class="tgme_widget_message_wrap[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        html, re.DOTALL
    )

    posts = []
    seen = set()

    # Ищем пары дата+просмотры
    all_times = re.findall(r'datetime="(\d{4}-\d{2}-\d{2})', html)
    all_views = re.findall(r'class="tgme_widget_message_views"[^>]*>([\d\s,KkMm]+)<', html)
    all_texts = re.findall(r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)

    for i, date_str in enumerate(all_times):
        if date_str <= last_tg_date or date_str in seen:
            continue
        seen.add(date_str)

        views = 0
        if i < len(all_views):
            v = all_views[i].strip().replace(" ", "").replace(",", "")
            if v.endswith("K") or v.endswith("k"):
                views = int(float(v[:-1]) * 1000)
            elif v.isdigit():
                views = int(v)

        topic = "Новый пост"
        if i < len(all_texts):
            clean = re.sub(r'<[^>]+>', '', all_texts[i]).strip()[:80]
            clean = re.sub(r'\s+', ' ', clean)
            if clean:
                topic = clean

        posts.append({
            "date": date_str,
            "type": "photo",
            "topic": topic,
            "reach": views,
            "likes": 0,
        })

    posts.sort(key=lambda x: x["date"], reverse=True)
    log(f"  TG новых постов: {len(posts)}")
    return posts, sub_tg


# ── Обновление постов в HTML ──────────────────────────────────────────────────
def get_last_post_date(html, platform):
    """Возвращает дату последнего поста заданной платформы."""
    pattern = rf'platform:"{platform}".*?date:"(\d{{4}}-\d{{2}}-\d{{2}})"'
    # Ищем все даты и берём максимальную
    m_all = re.findall(
        rf'\{{id:[^,]+,\s*date:"(\d{{4}}-\d{{2}}-\d{{2}})",platform:"{platform}"',
        html
    )
    if m_all:
        return max(m_all)
    return "2026-04-01"


def get_current_subs(html):
    """Возвращает (subVk, subTg) из последнего поста."""
    m = re.search(r'subVk:(\d+)', html)
    sub_vk = int(m.group(1)) if m else 568
    m = re.search(r'subTg:(\d+)', html)
    sub_tg = int(m.group(1)) if m else 68
    return sub_vk, sub_tg


def get_min_post_id(html):
    """Возвращает минимальный id в массиве posts (может быть отрицательным)."""
    ids = re.findall(r'\{id:(-?\d+),\s*date:', html)
    if ids:
        return min(int(x) for x in ids)
    return 0


def build_post_js(post, pid, platform, sub_vk, sub_tg):
    topic = post["topic"].replace('"', "'")[:75]
    reach = post.get("reach", 0)
    likes = post.get("likes", 0)
    post_type = post.get("type", "photo")
    date = post["date"]
    return (
        f'  {{id:{pid}, date:"{date}",platform:"{platform}",'
        f'       postType:"{post_type}", topic:"{topic}",'
        f'   reach:{reach},  likes:{likes},  comments:0, reposts:0,'
        f' subVk:{sub_vk}, subTg:{sub_tg}}},'
    )


def inject_posts(html, vk_posts, tg_posts, sub_vk, sub_tg):
    """Вставляет новые посты в начало соответствующих блоков."""
    if not vk_posts and not tg_posts:
        return html, 0, 0

    min_id = get_min_post_id(html)
    pid = min_id - 1

    # Вставить VK-посты перед первым VK-постом
    vk_lines = []
    for p in vk_posts:
        vk_lines.append(build_post_js(p, pid, "vk", sub_vk, sub_tg))
        pid -= 1

    if vk_lines:
        insert_vk = "\n".join(vk_lines) + "\n"
        html = re.sub(r'(// VK\n)', r'\1' + insert_vk, html, count=1)

    # Вставить TG-посты перед первым TG-постом
    tg_lines = []
    for p in tg_posts:
        tg_lines.append(build_post_js(p, pid, "telegram", sub_vk, sub_tg))
        pid -= 1

    if tg_lines:
        insert_tg = "\n".join(tg_lines) + "\n"
        html = re.sub(r'(// Telegram @opc_ivanovo\n)', r'\1' + insert_tg, html, count=1)

    return html, len(vk_posts), len(tg_posts)


def update_sub_comment(html, sub_vk, sub_tg):
    """Обновляет комментарий с числом подписчиков."""
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
    """Переводит задачи с due <= вчера и status='в работе' → 'выполнено'."""
    yesterday = (TODAY - datetime.timedelta(days=1)).isoformat()

    def replace_status(m):
        due = m.group(1)
        if due <= yesterday:
            return m.group(0).replace('status:"в работе"', 'status:"выполнено"')
        return m.group(0)

    html = re.sub(
        r'due:"(\d{4}-\d{2}-\d{2})"[^}]*status:"в работе"',
        replace_status,
        html
    )
    return html


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log(f"\n{'='*60}")
    log(f"🚀 OnePriceCoffee Dashboard Updater — {TODAY}")
    log(f"{'='*60}\n")

    html = read_html()
    cur_sub_vk, cur_sub_tg = get_current_subs(html)
    log(f"Текущие подписчики: VK={cur_sub_vk}, TG={cur_sub_tg}\n")

    # ── 1. Выручка из Sheets ────────────────────────────────────────────────
    sheets_data = fetch_sheets_revenue()
    revenue_added = 0
    if sheets_data:
        records, max_id, max_date, sum_rev, sum_cust = extract_existing_revenue(html)
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
    log(f"\n✅ Готово: VK +{vk_added} постов | TG +{tg_added} постов | Выручка +{revenue_added} записей")
    log(f"   Подписчики: VK={sub_vk} | TG={sub_tg}")

    # Возвращаем данные для шага commit-message
    return {
        "vk_added": vk_added,
        "tg_added": tg_added,
        "revenue_added": revenue_added,
        "sub_vk": sub_vk,
        "sub_tg": sub_tg,
    }


if __name__ == "__main__":
    result = main()
    # Записываем итог для GitHub Actions output
    summary = (
        f"VK: +{result['vk_added']} постов ({result['sub_vk']} подп.) | "
        f"TG: +{result['tg_added']} постов ({result['sub_tg']} подп.) | "
        f"Выручка: +{result['revenue_added']} записей"
    )
    print(f"\nSUMMARY: {summary}")
    # Сохраняем в файл для workflow
    with open("update_summary.txt", "w") as f:
        f.write(summary)
