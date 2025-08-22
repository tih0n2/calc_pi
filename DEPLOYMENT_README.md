# 🚀 Развертывание Дашборда Аналитики

## ⚠️ Безопасность

**ВАЖНО:** Код готов к публикации в публичном репозитории. Все чувствительные данные вынесены в переменные окружения.

## 🔧 Настройка переменных окружения

### Для локального запуска:

1. Скопируйте `env.example` в `.env`:
```bash
cp env.example .env
```

2. Заполните реальными значениями:
```bash
DB_HOST=your_database_host
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_SSLMODE=prefer
```

### Для Streamlit Cloud:

1. В настройках приложения добавьте переменные окружения:
   - `DB_HOST`
   - `DB_PORT`
   - `DB_NAME`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_SSLMODE`

## 📦 Зависимости

Создайте `requirements.txt`:
```
streamlit
pandas
plotly
psycopg2-binary
openpyxl
python-dotenv
requests
```

## 🚀 Запуск

```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск дашборда
streamlit run dashboard.py
```

## 🔒 Что НЕ попадет в репозиторий

- `.env` - переменные окружения
- `*.log` - логи
- `__pycache__/` - кэш Python
- `.DS_Store` - системные файлы
- `*.csv`, `*.xlsx` - экспортированные данные
