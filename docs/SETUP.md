# Инструкция по переносу в другую среду

## Быстрый старт (любая среда)

```bash
git clone https://github.com/grigorykis-hub/onepricecoffee-dashboard.git
cd onepricecoffee-dashboard
```

Всё. Открой `index.html` в браузере — дашборд работает без установки зависимостей.

---

## Перенос в Cursor

1. **Клонировать репозиторий:**
   ```bash
   git clone https://github.com/grigorykis-hub/onepricecoffee-dashboard.git
   ```

2. **Открыть папку в Cursor:** File → Open Folder → выбрать `onepricecoffee-dashboard`

3. **Открыть дашборд:** правой кнопкой на `index.html` → Open with Live Server (или просто открыть в браузере)

4. **Для запуска скрипта обновления локально:**
   ```bash
   pip install requests  # если нужен requests (скрипт использует urllib из stdlib)
   VK_TOKEN=ваш_токен python update_dashboard.py
   ```

5. **Контекст для Cursor AI:**  
   Открой чат Cursor и вставь содержимое `docs/ARCHITECTURE.md` — это даст AI полный контекст о структуре проекта.

---

## Перенос в Claude Code (claude.ai/code)

1. **Загрузить файлы проекта:**
   - Загрузи `index.html`, `update_dashboard.py`, `.github/workflows/update.yml`
   - Или дай Claude ссылку на репозиторий: `https://github.com/grigorykis-hub/onepricecoffee-dashboard`

2. **Стартовый промпт для Claude:**
   ```
   Это дашборд для сети кофеен OnePriceCoffee (Иваново).
   
   Структура проекта:
   - index.html — весь фронтенд, данные хранятся в JS-массивах
   - update_dashboard.py — скрипт автообновления данных (VK, Telegram, Google Sheets)
   - .github/workflows/update.yml — cron-задача GitHub Actions (ежедневно 11:00 МСК)
   
   Полная архитектура: [вставь содержимое ARCHITECTURE.md]
   Переменные окружения: [вставь содержимое SECRETS.md]
   ```

3. **Важные особенности кода:**
   - Данные обновляются через `regex replace` в `index.html` — не через БД
   - `edit` инструмент ломается на JS-коде с `\B` в регулярках — использовать Python-скрипт для вставки JS
   - SheetJS подключён через CDN только для страницы Таргет VK

---

## Перенос в VS Code

1. Клонировать репо (см. выше)
2. Установить расширения:
   - **Live Server** — для просмотра index.html
   - **Python** — для работы со скриптом
3. Создать `.env` файл:
   ```
   VK_TOKEN=ваш_токен_вк
   ```
4. Запуск скрипта:
   ```bash
   export $(cat .env | xargs) && python update_dashboard.py
   ```

---

## Добавление нового месяца ОПВ

Каждый месяц нужно добавить новый файл выручки:

1. Открой файл `ММ.ГГ ОПВ` в Google Drive
2. Нажми «Поделиться» → «Все, у кого есть ссылка» → «Просматривающий»
3. Скопируй ID из URL: `https://docs.google.com/spreadsheets/d/**ВОТ_ЭТО**/edit`
4. Добавь в `update_dashboard.py`:
   ```python
   OPV_FILE_MAP = {
       ...
       "2026-06": "НОВЫЙ_FILE_ID",  # ← добавить
   }
   ```
5. Сохрани и запушь:
   ```bash
   git add update_dashboard.py
   git commit -m "Добавлен файл ОПВ за июнь 2026"
   git push
   ```

---

## Добавление новой кофейни

1. В `update_dashboard.py` добавить новый URL листа Google Sheets в `get_sheets_urls()`
2. В `index.html` добавить новую локацию в `revenueData` и обновить фильтры на странице «Трафик и Выручка»

---

## Ручной запуск обновления

Через GitHub UI:  
Репозиторий → Actions → «Обновление дашборда» → «Run workflow» → Run

Через CLI:
```bash
gh workflow run "Обновление дашборда" --repo grigorykis-hub/onepricecoffee-dashboard
```

---

## Отладка GitHub Actions

```bash
# Посмотреть последние запуски
gh run list --repo grigorykis-hub/onepricecoffee-dashboard --limit 5

# Посмотреть лог конкретного запуска
gh run view RUN_ID --repo grigorykis-hub/onepricecoffee-dashboard --log
```

---

## Частые проблемы

| Проблема | Причина | Решение |
|----------|---------|---------|
| Данные не обновляются | Первый запуск cron — нужна первая ручная активация | Запустить workflow вручную через GitHub UI |
| VK токен не работает | Токен протух или нет прав `offline` | Перевыпустить токен на vkhost.github.io |
| Выручка не загружается | Новый файл ОПВ не в маппинге | Добавить ID файла в OPV_FILE_MAP |
| Выручка не загружается 2 | Файл Google Sheets закрыт | Открыть доступ «по ссылке» |
| `edit` tool сломал JS | Спецсимволы в JS-коде | Использовать Python heredoc для вставки |
