"""
Парсер отзывов Яндекс Карт через Playwright с обходом bot-detection.
Запускается из GitHub Actions еженедельно по воскресеньям.
"""
import json, re, sys, datetime, os

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("[WARN] playwright не установлен")
    sys.exit(0)

ORG_URL = "https://yandex.ru/maps/org/onepricecoffee/236703503674/reviews/?sort=bynearestfirst"

MONTHS = {
    'января':'01','февраля':'02','марта':'03','апреля':'04',
    'мая':'05','июня':'06','июля':'07','августа':'08',
    'сентября':'09','октября':'10','ноября':'11','декабря':'12'
}

def parse_date(raw):
    parts = raw.strip().split()
    if len(parts) >= 2:
        day = parts[0].zfill(2)
        mon = MONTHS.get(parts[1].lower(), '00')
        yr  = parts[2] if len(parts) > 2 else str(datetime.date.today().year)
        return f"{day}.{mon}.{yr}"
    return raw

def fetch_reviews():
    reviews = []
    total_rating = None
    total_count = None
    breakdown = {}

    with sync_playwright() as p:
        # Используем Firefox — он менее заметен для Яндекса
        browser = p.firefox.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        # Сначала зайдём на главную чтобы получить куки
        page.goto("https://yandex.ru/", timeout=20000)
        page.wait_for_timeout(2000)

        # Теперь переходим на страницу отзывов
        page.goto(ORG_URL, timeout=30000)
        page.wait_for_timeout(5000)

        # Проверяем что загрузилось
        title = page.title()
        print(f"  Page title: {title}")

        body_text = page.inner_text('body')[:200]
        print(f"  Body sample: {body_text[:100]}")

        # Captcha check
        captcha = page.query_selector('[class*="captcha"], [class*="CheckboxCaptcha"]')
        if captcha:
            print("  [WARN] Яндекс показал капчу — парсинг невозможен")
            browser.close()
            return None, None, {}, []

        # Общий рейтинг
        for sel in ['.business-summary-rating-badge-view__rating',
                    '.orgpage-header-view__rating .business-rating-badge-view__rating-value',
                    '[class*="rating-badge"]']:
            el = page.query_selector(sel)
            if el:
                try:
                    total_rating = float(el.inner_text().strip().replace(',', '.'))
                    print(f"  Рейтинг: {total_rating}")
                    break
                except: pass

        # Количество отзывов
        for sel in ['.business-header-rating-view__text._clickable',
                    '.business-header-rating-view__text',
                    '[class*="reviews-count"]']:
            el = page.query_selector(sel)
            if el:
                nums = re.findall(r'\d+', el.inner_text())
                if nums:
                    total_count = int(nums[-1])
                    print(f"  Всего отзывов: {total_count}")
                    break

        # Скроллим для загрузки отзывов
        last_count = 0
        for attempt in range(10):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            cur = len(page.query_selector_all('.business-reviews-view__review'))
            print(f"  Attempt {attempt+1}: {cur} reviews loaded")
            if cur == last_count:
                break
            last_count = cur
            # Нажимаем "Показать ещё" если есть
            more = page.query_selector('.business-reviews-view__more-link, .show-more')
            if more:
                try: more.click(); page.wait_for_timeout(2000)
                except: pass

        # Парсим отзывы
        review_els = page.query_selector_all('.business-reviews-view__review')
        print(f"  Всего элементов отзывов: {len(review_els)}")

        for el in review_els:
            try:
                author_el = el.query_selector('.business-review-view__author-name')
                author = author_el.inner_text().strip() if author_el else 'Аноним'

                date_el = el.query_selector('.business-review-view__date')
                date_str = parse_date(date_el.inner_text()) if date_el else ''

                # Звёзды — считаем full-stars
                full_stars = el.query_selector_all('[class*="star"][class*="full"]')
                if not full_stars:
                    # Fallback: aria-label
                    badge = el.query_selector('[class*="rating-badge"]')
                    if badge:
                        aria = badge.get_attribute('aria-label') or ''
                        nums = re.findall(r'(\d)', aria)
                        stars = int(nums[0]) if nums else None
                    else:
                        stars = None
                else:
                    stars = len(full_stars)

                # Текст отзыва
                text_el = el.query_selector('.business-review-view__body-text')
                if text_el:
                    more_btn = text_el.query_selector('button, [class*="more"]')
                    if more_btn:
                        try: more_btn.click(); page.wait_for_timeout(300)
                        except: pass
                    text = text_el.inner_text().strip()
                else:
                    text = ''

                if text:
                    reviews.append({
                        'source': 'yandex',
                        'author': author,
                        'stars': stars,
                        'date': date_str,
                        'text': text
                    })
            except Exception as e:
                print(f"  [WARN] {e}")

        browser.close()

    print(f"  Итого собрано: {len(reviews)} отзывов")
    return total_rating, total_count, breakdown, reviews


def update_yandex_in_html(html, total_rating, total_count, breakdown, reviews):
    if total_rating:
        html = re.sub(r'id="rating-yandex-val">[^<]+<', f'id="rating-yandex-val">{total_rating}<', html)
    if total_count:
        html = re.sub(r'id="rating-yandex-count">[^<]+<', f'id="rating-yandex-count">{total_count}<', html)
        html = re.sub(r'id="rating-yandex-reviews">[^<]+<', f'id="rating-yandex-reviews">{total_count}<', html)

    if breakdown:
        html = re.sub(
            r'const yandexBreakdown\s*=\s*\{[^}]*\};',
            f'const yandexBreakdown = {json.dumps(breakdown)};',
            html
        )

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
        html = re.sub(r'const reviewsData = \[.*?\];', new_data, html, flags=re.DOTALL)

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

    print(f"index.html обновлён: {len(reviews)} отзывов Яндекс")
