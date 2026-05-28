# Переменные окружения и токены

## GitHub Secrets (Settings → Secrets → Actions)

| Переменная | Где взять | Для чего |
|------------|-----------|----------|
| `VK_TOKEN` | [vk.com/dev](https://vk.com/dev) → Standalone-приложение → access_token с правами `wall`, `groups`, `offline` | VK API: посты, подписчики, рейтинг, отзывы |

> **Примечание:** Telegram и Google Sheets работают без токенов — через публичный scraping и CSV-экспорт.

---

## VK Token — как получить/обновить

1. Зайди на [vkhost.github.io](https://vkhost.github.io/)
2. Выбери разрешения: `wall`, `groups`, `offline`
3. Нажми «Получить токен» → авторизуйся через VK
4. Скопируй `access_token` из URL
5. В GitHub: репозиторий → Settings → Secrets and variables → Actions → `VK_TOKEN` → Update

> Токен с `offline` не истекает. Без `offline` — протухает через несколько часов.

---

## VK — идентификаторы групп

| Параметр | Значение | Описание |
|----------|----------|----------|
| `VK_GROUP_SCREEN` | `onepricecoffee_ivanovo` | screen name для wall.get |
| `VK_BOARD_GROUP_ID` | `236450024` | ID группы для board API |
| `VK_REVIEWS_TOPIC_ID` | `55892727` | ID топика «ОТЗЫВЫ» |
| `VK_ADS_ACCOUNT_ID` | `30421754` | ID рекламного кабинета (ООО «КАПУЧИНО») |

---

## Google Sheets — идентификаторы

| Параметр | Значение |
|----------|----------|
| Папка ОПВ (Drive) | `1xjKIklKYNYXf9pgzOrh1FNguzVpHOK1a` |
| GID листа Ленина | `649208657` |
| GID листа Серебряного | `2039636677` |

### Файлы по месяцам (добавлять ежемесячно в `update_dashboard.py`):

| Месяц | File ID |
|-------|---------|
| Январь 2026 | `1nzsZA0OvD7NEl0gBq6qCUxI7v9kfqbfRjPjXlXODFlY` |
| Февраль 2026 | `1pvpBJ1F6PEgtXvHZCyvJvDHjoe3wBqNQcveAYsLpR6c` |
| Март 2026 | `1jIn-HXDhce080LSNyfQmM5zYdbLF3FUgdDg11cfq9Xc` |
| Апрель 2026 | `1Gx7-FIccn0qLkH7aGKzpDSu6Ixq2xh_HTiSZR2yoiBA` |
| Май 2026 | `1woZ5udfV-RkEFAmBApo4h1_-fOavvJWcfCWI7jCPskk` |

> Каждый месяц: открой новый файл «ММ.ГГ ОПВ» по ссылке (Поделиться → Все по ссылке), возьми ID из URL и добавь в `OPV_FILE_MAP` в `update_dashboard.py`.

---

## Telegram

| Параметр | Значение |
|----------|----------|
| Канал | `@opc_ivanovo` |
| URL | `https://t.me/opc_ivanovo` |

Токен не нужен — используется публичный Telegram preview.

---

## GitHub Pages

| Параметр | Значение |
|----------|----------|
| Репозиторий | `grigorykis-hub/onepricecoffee-dashboard` |
| Ветка | `main` |
| Папка | `/` (корень) |
| URL | `https://grigorykis-hub.github.io/onepricecoffee-dashboard/` |
| Git remote | `https://git-agent-proxy.perplexity.ai/grigorykis-hub/onepricecoffee-dashboard.git` |
