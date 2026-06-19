"""
Парсер отзывов Яндекс Карт через Playwright (headless Chrome).
Запускается из GitHub Actions еженедельно.
Обновляет reviewsData и yandexBreakdown в index.html.
"""
import json, re, sys, datetime

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("[WARN] playwright не установлен")
    sys.exit(0)

ORG_URL = "https://yandex.ru/maps/org/onepricecoffee/236703503674/reviews/?sort=bynearestfirst"

def parse_stars(review_el):
    """Считает закрашенные звёзды в элементе отзыва."""
    try:
        stars = review_el.query_selector_all('.business-rating-badge-view__star._full')
        return len(stars)
    except:
        return None

def fetch_reviews():
    reviews = []
    total_rating = None
    total_count = None
    breakdown = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            locale="ru-RU"
        )
        page.goto(ORG_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)

        # Общий рейтинг
        try:
            rating_el = page.query_selector('.business-summary-rating-badge-view__rating')
            if rating_el:
                total_rating = float(rating_el.inner_text().strip().replace(',', '.'))
        except:
            pass

        # Количество отзывов
        try:
            count_el = page.query_selector('.business-header-rating-view__text')
            if count_el:
                nums = re.findall(r'\d+', count_el.inner_text())
                if nums:
                    total_count = int(nums[-1])
        except:
            pass

        # Распределение по звёздам
        try:
            bars = page.query_selector_all('.business-rating-distribution-view__item')
            for bar in bars:
                star_el = bar.query_selector('.business-rating-distribution-view__stars')
                cnt_el  = bar.query_selector('.business-rating-distribution-view__count')
                if star_el and cnt_el:
                    star_num = len(re.findall(r'★|✦|_full', star_el.inner_text() + (star_el.get_attribute('class') or '')))
                    cnt = re.search(r'\d+', cnt_el.inner_text())
                    if cnt:
                        breakdown[str(star_num)] = int(cnt.group())
        except:
            pass

        # Скроллим и собираем отзывы
        for _ in range(8):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)
            more = page.query_selector('.business-reviews-view__more button, .show-more-button')
            if more:
                more.click()
                page.wait_for_timeout(2000)

        # Парсим отзывы
        review_els = page.query_selector_all('.business-reviews-view__review')
        for el in review_els:
            try:
                # Автор
                author_el = el.query_selector('.business-review-view__author-name')
                author = author_el.inner_text().strip() if author_el else 'Аноним'

                # Дата
                date_el = el.query_selector('.business-review-view__date')
                date_str = ''
                if date_el:
                    raw = date_el.inner_text().strip()
                    # Конвертируем "19 июня 2026" → "19.06.2026"
                    months = {'января':'01','февраля':'02','марта':'03','апреля':'04',
                              'мая':'05','июня':'06','июля':'07','августа':'08',
                              'сентября':'09','октября':'10','ноября':'11','декабря':'12'}
                    parts = raw.split()
                    if len(parts) >= 2:
                        day = parts[0].zfill(2)
                        mon = months.get(parts[1].lower(), '00')
                        yr  = parts[2] if len(parts) > 2 else str(datetime.date.today().year)
                        date_str = f"{day}.{mon}.{yr}"

                # Звёзды
                full_stars = el.query_selector_all('[class*="star"][class*="full"], [class*="_full"]')
                stars = len(full_stars) if full_stars else None

                # Текст
                text_el = el.query_selector('.business-review-view__body-text')
                text = ''
                if text_el:
                    # Раскрываем "ещё"
                    more_btn = text_el.query_selector('button')
                    if more_btn:
                        try: more_btn.click(); page.wait_for_timeout(300)
                        except: pass
                    text = text_el.inner_text().strip()

                if author and text:
                    reviews.append({
                        'source': 'yandex',
                        'author': author,
                        'stars': stars,
                        'date': date_str,
                        'text': text
                    })
            except Exception as e:
                print(f"  [WARN] Ошибка парсинга отзыва: {e}")

        browser.close()

    print(f"  Яндекс: рейтинг={total_rating}, отзывов={total_count}, собрано={len(reviews)}")
    return total_rating, total_count, breakdown, reviews


def update_yandex_in_html(html, total_rating, total_count, breakdown, reviews):
    """Обновляет yandexBreakdown, yandexRating и reviewsData в index.html."""

    # 1. Рейтинг на главной и на странице отзывов
    if total_rating:
        html = re.sub(r'id="rating-yandex-val">[^<]+<', f'id="rating-yandex-val">{total_rating}<', html)
        html = re.sub(r'id="rating-yandex-count">[^<]+<', f'id="rating-yandex-count">{total_count or ""}<', html)

    # 2. Breakdown
    if breakdown:
        html = re.sub(
            r'const yandexBreakdown\s*=\s*\{[^}]*\};',
            f'const yandexBreakdown = {json.dumps(breakdown)};',
            html
        )

    # 3. reviewsData — обновляем полностью
    if reviews:
        def fmt(r):
            stars_str = str(r['stars']) if r['stars'] is not None else 'null'
            text = r['text'].replace('\\', '\\\\').replace('`', "'").replace('${', '\\${')
            return (
                f"  {{ source:'yandex', author:{json.dumps(r['author'], ensure_ascii=False)}, "
                f"stars:{stars_str}, date:{json.dumps(r['date'])}, "
                f"text:{json.dumps(text, ensure_ascii=False)} }}"
            )
        entries_str = ',\n'.join(fmt(r) for r in reviews)
        new_data = f'const reviewsData = [\n{entries_str}\n];'
        html = re.sub(
            r'const reviewsData = \[.*?\];',
            new_data,
            html,
            flags=re.DOTALL
        )

    return html


if __name__ == '__main__':
    total_rating, total_count, breakdown, reviews = fetch_reviews()

    if not reviews:
        print("Нет данных — index.html не изменён")
        sys.exit(0)

    with open('index.html', encoding='utf-8') as f:
        html = f.read()

    html = update_yandex_in_html(html, total_rating, total_count, breakdown, reviews)

    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  Обновлено в index.html: {len(reviews)} отзывов Яндекс")
