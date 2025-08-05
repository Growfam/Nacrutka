# Telegram SMM Bot 🤖

Автоматична система накрутки для Telegram каналів з використанням Nakrutka API.

## 🚀 Можливості

- ✅ Автоматичний моніторинг нових постів в каналах
- ✅ Накрутка переглядів, реакцій та репостів
- ✅ Рандомізація кількості (±40%)
- ✅ Drip-feed розподіл по порціях
- ✅ Підтримка різних типів реакцій
- ✅ Детальна статистика та логування
- ✅ Telegram бот для управління

## 📋 Вимоги

- Python 3.11+
- PostgreSQL (через Supabase)
- Telegram Bot Token
- Nakrutka API Key
- Railway для хостингу

## 🛠️ Встановлення

### 1. Клонування репозиторію

```bash
git clone https://github.com/yourusername/telegram-smm-bot.git
cd telegram-smm-bot
```

### 2. Створення віртуального середовища

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# або
venv\Scripts\activate  # Windows
```

### 3. Встановлення залежностей

```bash
pip install -r requirements.txt
```

### 4. Налаштування змінних середовища

Створіть файл `.env` на основі `.env.example`:

```bash
cp .env.example .env
```

Заповніть необхідні змінні:

```env
# Database
DATABASE_URL=postgresql://postgres:password@db.project.supabase.co:5432/postgres

# Supabase
SUPABASE_URL=https://project.supabase.co
SUPABASE_KEY=your-service-role-key

# APIs
NAKRUTKA_API_KEY=your-api-key
TELEGRAM_BOT_TOKEN=your-bot-token

# Settings
CHECK_INTERVAL=30
ADMIN_TELEGRAM_ID=your-telegram-id
```

## 🗄️ База даних

База даних вже налаштована в Supabase з усіма необхідними таблицями:

- `channels` - Канали для моніторингу
- `channel_settings` - Налаштування накрутки
- `posts` - Оброблені пости
- `orders` - Замовлення
- `order_portions` - Порції для drip-feed
- І інші...

Детальна документація БД знаходиться в файлі `Повна документація БД Telegram Bot для автоматичної накрутки.md`

## 🚀 Запуск

### Локальний запуск

```bash
python main.py
```

### Запуск через Docker

```bash
docker build -t telegram-smm-bot .
docker run --env-file .env telegram-smm-bot
```

## 📱 Команди Telegram бота

- `/start` - Початок роботи
- `/status` - Статус бота та активні замовлення
- `/stats` - Статистика по каналах

## ⚙️ Налаштування каналу

Канал `mark_crypto_inside` вже налаштований з наступними параметрами:

- **Перегляди**: 4600 (без рандомізації)
- **Реакції**: 66 ±40%
- **Репости**: 23 ±40%

### Розподіл реакцій:
- Позитивний мікс (ID: 3911) - 48 штук
- Блискавка ⚡️ (ID: 3839) - 11 штук
- Кит 🐳 (ID: 3872) - 7 штук

## 🔄 Логіка роботи

1. **Моніторинг** - кожні 30 секунд перевірка нових постів
2. **Обробка** - створення замовлень з рандомізацією
3. **Розподіл** - 5 порцій для кожного типу накрутки:
   - 70% за перші 3-5 годин
   - 30% протягом дня
4. **Відправка** - через Nakrutka API з drip-feed
5. **Контроль** - моніторинг статусів виконання

## 🚂 Деплой на Railway

1. Створіть проект на Railway
2. Підключіть GitHub репозиторій
3. Додайте всі змінні середовища
4. Railway автоматично задеплоїть через Dockerfile

## 📊 Моніторинг

### Логи
Всі логи зберігаються в БД та виводяться в консоль.

### Статистика
```sql
-- Переглянути активні канали
SELECT * FROM channel_monitoring;

-- Витрати за сьогодні
SELECT * FROM orders WHERE DATE(created_at) = CURRENT_DATE;
```

## 🧪 Тестування

```bash
# Запустити всі тести
pytest

# Тести з детальним виводом
pytest -v

# Конкретний файл
pytest tests/test_database.py
```

## 🛡️ Безпека

- API ключі зберігаються в змінних середовища
- База даних має обмежений доступ
- Telegram бот команди доступні тільки адміну
- Всі помилки логуються без sensitive даних

## 📝 Структура проекту

```
telegram-smm-bot/
├── src/
│   ├── config.py           # Конфігурація
│   ├── database/           # Робота з БД
│   ├── services/           # Основні сервіси
│   └── utils/              # Допоміжні функції
├── tests/                  # Тести
├── main.py                # Точка входу
├── requirements.txt       # Залежності
├── Dockerfile            # Docker конфігурація
└── README.md            # Документація
```

## ❓ FAQ

**Q: Як додати новий канал?**
A: Додайте запис в таблиці `channels` та `channel_settings` через SQL.

**Q: Як змінити кількість накрутки?**
A: Оновіть значення в таблиці `channel_settings`.

**Q: Чому не працює моніторинг?**
A: Перевірте чи бот є адміном в каналі або канал публічний.

## 📞 Підтримка

При виникненні проблем:
1. Перевірте логи в БД
2. Перегляньте статус через `/status`
3. Переконайтесь що всі API ключі правильні

## 📄 Ліцензія

MIT License - детальніше в файлі LICENSE