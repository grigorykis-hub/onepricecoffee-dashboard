# Архитектура проекта

## Общая схема

```
┌─────────────────────────────────────────────┐
│               GitHub Repository              │
│                                             │
│  index.html  ←──── update_dashboard.py      │
│      │                    │                 │
│      │              GitHub Actions          │
│      │              (0 8 * * * UTC)         │
└──────┼────────────────────┼─────────────────┘
       │                    │
       ▼                    ▼
  GitHub Pages         Внешние API
  (публичный URL)      ┌──────────────┐
                       │ VK API       │ посты, подписчики,
                       │              │ рейтинг, отзывы
                       ├──────────────┤
                       │ Telegram     │ посты, подписчики
                       │ (scraping)   │
                       ├──────────────┤
                       │ Google Sheets│ выручка по дням
                       │ (CSV export) │
                       └──────────────┘
```

---

## index.html — структура

Один файл содержит всё: HTML-разметку, CSS-стили и JavaScript.  
Данные (посты, выручка, отзывы) хранятся прямо в JS-массивах внутри файла — скрипт обновляет их при каждом запуске.

### Разделы HTML (страницы)

```html
<div id="page-home">     <!-- Главная -->
<div id="page-smm">      <!-- СММ -->
<div id="page-events">   <!-- Мероприятия -->
<div id="page-revenue">  <!-- Трафик и Выручка -->
<div id="page-target">   <!-- Таргет VK -->
<div id="page-reviews">  <!-- Отзывы -->
<div id="page-tasks">    <!-- Задачи -->
```

### Навигация

```javascript
function navigate(page) { ... }  // показывает нужную страницу
```

### Ключевые JS-массивы (данные)

```javascript
// Посты VK — обновляется update_dashboard.py
const vkPosts = [ { id, date, text, likes, views, link } ];
let currentSubVk = 660;  // текущее число подписчиков

// Посты Telegram
const tgPosts = [ { id, date, text, views } ];
let currentSubTg = 81;

// Выручка по дням (обе точки)
const revenueData = [
  { id, date, location, revenue, customers, avg_check }
];

// Отзывы Яндекс Карт (захардкожены)
const reviewsData = [ { source, author, stars, date, text } ];

// VK рейтинг (обновляется автоматически)
const vkRatingData = { mark: 3.7, review_cnt: 3 };
const vkBreakdown  = { "5": 2, "4": 0, "3": 0, "2": 0, "1": 1 };

// Отзывы из VK обсуждений (обновляются автоматически)
const vkBoardReviews = [ ... ];
```

### Маппинг месячных файлов ОПВ (в update_dashboard.py)

```python
OPV_FILE_MAP = {
    "2026-01": "1nzsZA0OvD7NEl0gBq6qCUxI7v9kfqbfRjPjXlXODFlY",
    "2026-02": "1pvpBJ1F6PEgtXvHZCyvJvDHjoe3wBqNQcveAYsLpR6c",
    "2026-03": "1jIn-HXDhce080LSNyfQmM5zYdbLF3FUgdDg11cfq9Xc",
    "2026-04": "1Gx7-FIccn0qLkH7aGKzpDSu6Ixq2xh_HTiSZR2yoiBA",
    "2026-05": "1woZ5udfV-RkEFAmBApo4h1_-fOavvJWcfCWI7jCPskk",
    # добавлять каждый месяц
}
GID_LENINA = "649208657"   # лист "Иваново Ленина"
GID_SEREBR = "2039636677"  # лист "Иваново ТЦ Сер. город"
```

---

## update_dashboard.py — структура

```python
# ── Конфиг ────────────────────────────────────────
OPV_FILE_MAP = { ... }        # маппинг месяц → Google Sheets ID
VK_GROUP_SCREEN = "onepricecoffee_ivanovo"
TG_CHANNEL     = "opc_ivanovo"
VK_REVIEWS_TOPIC_ID = 55892727
VK_BOARD_GROUP_ID   = 236450024

# ── Утилиты ───────────────────────────────────────
def fetch(url)                # HTTP GET с таймаутом
def vk_api(method, params)    # VK API вызов с токеном
def log(msg)                  # логирование + запись в update_summary.txt

# ── Источники данных ──────────────────────────────
def fetch_vk_posts()          # VK wall.get → новые посты
def fetch_tg_posts()          # Telegram scraping → посты + подписчики
def fetch_sheets_revenue()    # Google Sheets CSV → данные по дням
def fetch_vk_rating()         # VK groups.getById → рейтинг
def fetch_vk_board_reviews()  # VK board.getComments → отзывы
def get_sheets_urls()         # определяет текущий файл ОПВ по месяцу

# ── Парсинг ───────────────────────────────────────
def parse_daily_sheet(csv, location)  # разбирает CSV лист выручки

# ── Обновление HTML ───────────────────────────────
def update_vk_in_html(html, posts, sub)
def update_tg_in_html(html, posts, sub)
def update_revenue_in_html(html, lenina, serebr)
def update_vk_reviews_in_html(html, rating, board_reviews)

# ── Точка входа ───────────────────────────────────
def main()
```

---

## GitHub Actions — update.yml

```yaml
on:
  schedule:
    - cron: '0 8 * * *'   # 11:00 МСК ежедневно
  workflow_dispatch:       # ручной запуск через GitHub UI

env:
  VK_TOKEN: ${{ secrets.VK_TOKEN }}

steps:
  1. actions/checkout@v4
  2. actions/setup-python@v5 (3.11)
  3. python update_dashboard.py
  4. git add index.html && git commit && git push  # если были изменения
```

---

## Страница Таргет VK — принцип работы

1. Пользователь скачивает отчёт с [ads.vk.com](https://ads.vk.com)
2. Загружает `.xlsx` файл на страницу дашборда
3. SheetJS парсит файл прямо в браузере (без сервера)
4. Показываются KPI-карточки с цветовой индикацией по порогам:
   - CTR: >1.2% 🟢 / 0.7–1.2% 🔵 / 0.3–0.7% 🟡 / <0.3% 🔴
   - CPF: ≤50₽ 🟢 / 50–100₽ 🟡 / >100₽ 🔴
   - Частота: ≤3 🟢 / 3–4 🟡 / >5 🔴
