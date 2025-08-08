# 📚 Повна документація БД Telegram Bot для автоматичної накрутки

## 🏗️ Архітектура системи

### Компоненти:
- **PostgreSQL (Supabase)** - база даних
- **Python Bot** - основна логіка
- **Railway** - хостинг 24/7
- **Nakrutka API** - сервіс накрутки
- **Telegram Bot API** - моніторинг каналів

## 📊 Структура бази даних

### 1. Таблиця `channels` - Канали для моніторингу
```sql
CREATE TABLE channels (
    id SERIAL PRIMARY KEY,
    channel_username VARCHAR(255) UNIQUE NOT NULL,  -- username без @
    channel_id BIGINT UNIQUE,                       -- Telegram ID каналу
    is_active BOOLEAN DEFAULT true,                 -- Чи активний моніторинг
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Таблиця `channel_settings` - Налаштування накрутки
```sql
CREATE TABLE channel_settings (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
    views_target INTEGER DEFAULT 0,              -- Цільова к-сть переглядів
    reactions_target INTEGER DEFAULT 0,          -- Цільова к-сть реакцій
    reposts_target INTEGER DEFAULT 0,            -- Цільова к-сть репостів
    reaction_types JSONB DEFAULT '["❤️", "🔥", "👍"]'::jsonb,
    views_service_id INTEGER,                    -- ID сервісу з Nakrutka
    reactions_service_id INTEGER,
    reposts_service_id INTEGER,
    randomize_reactions BOOLEAN DEFAULT true,    -- Рандомізувати реакції
    randomize_reposts BOOLEAN DEFAULT true,      -- Рандомізувати репости
    randomize_percent INTEGER DEFAULT 40,        -- Відсоток рандомізації ±
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3. Таблиця `posts` - Оброблені пости
```sql
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
    post_id INTEGER NOT NULL,                    -- Telegram message ID
    post_url VARCHAR(500),                       -- Посилання на пост
    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,                      -- Коли почали обробку
    status VARCHAR(20) DEFAULT 'new' 
        CHECK (status IN ('new', 'processing', 'completed', 'failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, post_id)                  -- Унікальність по каналу
);
```

### 4. Таблиця `orders` - Замовлення в Nakrutka
```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
    nakrutka_order_id VARCHAR(100),              -- ID від Nakrutka API
    service_type VARCHAR(20) 
        CHECK (service_type IN ('views', 'reactions', 'reposts')),
    service_id INTEGER,                          -- ID сервісу Nakrutka
    total_quantity INTEGER,                      -- Загальна кількість
    status VARCHAR(20) DEFAULT 'pending' 
        CHECK (status IN ('pending', 'in_progress', 'completed', 'cancelled')),
    start_delay_minutes INTEGER DEFAULT 0,       -- Затримка старту
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

### 5. Таблиця `order_portions` - Порції для drip-feed
```sql
CREATE TABLE order_portions (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    portion_number INTEGER NOT NULL,             -- Номер порції (1-5)
    quantity_per_run INTEGER NOT NULL,           -- К-сть за один запуск
    runs INTEGER NOT NULL,                       -- К-сть повторів
    interval_minutes INTEGER NOT NULL,           -- Інтервал між повторами
    nakrutka_portion_id VARCHAR(100),            -- ID порції від API
    status VARCHAR(20) DEFAULT 'waiting' 
        CHECK (status IN ('waiting', 'running', 'completed')),
    scheduled_at TIMESTAMP,                      -- Запланований час
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

### 6. Таблиця `services` - Довідник сервісів Nakrutka
```sql
CREATE TABLE services (
    id SERIAL PRIMARY KEY,
    nakrutka_id INTEGER UNIQUE NOT NULL,         -- ID з Nakrutka
    service_name VARCHAR(500) NOT NULL,          -- Назва сервісу
    service_type VARCHAR(20) 
        CHECK (service_type IN ('views', 'reactions', 'reposts', 'subscribers')),
    price_per_1000 DECIMAL(10, 4),              -- Ціна за 1000
    min_quantity INTEGER,                        -- Мін. замовлення
    max_quantity INTEGER,                        -- Макс. замовлення
    is_active BOOLEAN DEFAULT true,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 7. Таблиця `portion_templates` - Шаблони розподілу порцій
```sql
CREATE TABLE portion_templates (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
    service_type VARCHAR(20) 
        CHECK (service_type IN ('views', 'reactions', 'reposts')),
    portion_number INTEGER,                      -- Номер порції (1-5)
    quantity_percent DECIMAL(5,2),              -- % від загальної к-сті
    runs_formula VARCHAR(50),                    -- Формула для runs
    interval_minutes INTEGER,                    -- Інтервал між повторами
    start_delay_minutes INTEGER,                 -- Затримка старту порції
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 8. Таблиця `channel_reaction_services` - Розподіл реакцій
```sql
CREATE TABLE channel_reaction_services (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
    service_id INTEGER,                          -- ID сервісу реакції
    emoji VARCHAR(10),                           -- Емодзі або 'mix'
    target_quantity INTEGER,                     -- Цільова кількість
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 9. Таблиця `api_keys` - Ключі API
```sql
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL 
        CHECK (service_name IN ('nakrutka', 'telegram_bot')),
    api_key TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 10. Таблиця `logs` - Системні логи
```sql
CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(10) CHECK (level IN ('info', 'warning', 'error')),
    message TEXT,
    context JSONB,                               -- Додаткова інформація
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔧 Функції БД

### 1. `calculate_random_quantity` - Розрахунок з рандомізацією
```sql
CREATE OR REPLACE FUNCTION calculate_random_quantity(
    base_quantity INTEGER,
    randomize_percent INTEGER
) RETURNS INTEGER
```
Повертає випадкову кількість в межах ±randomize_percent від base_quantity

### 2. `calculate_portion_details` - Розрахунок параметрів порцій
```sql
CREATE OR REPLACE FUNCTION calculate_portion_details(
    p_channel_id INTEGER,
    p_service_type VARCHAR,
    p_total_quantity INTEGER
) RETURNS TABLE (...)
```
Розраховує детальні параметри для кожної порції

### 3. `update_updated_at_column` - Автооновлення updated_at
```sql
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER
```
Тригерна функція для автоматичного оновлення поля updated_at

## 📈 Views (представлення)

### `channel_monitoring` - Огляд стану каналів
```sql
CREATE VIEW channel_monitoring AS
SELECT 
    channel_username,
    is_active,
    views_target,
    reactions_target,
    reposts_target,
    randomize_percent,
    total_posts,
    active_posts
FROM ...
```

## 🔑 Індекси для оптимізації

```sql
-- Основні індекси
CREATE INDEX idx_posts_channel_id ON posts(channel_id);
CREATE INDEX idx_posts_status ON posts(status);
CREATE INDEX idx_orders_post_id ON orders(post_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_portions_order_id ON order_portions(order_id);
CREATE INDEX idx_logs_created_at ON logs(created_at);
CREATE INDEX idx_portion_templates_channel ON portion_templates(channel_id);
CREATE INDEX idx_portion_templates_type ON portion_templates(service_type);
CREATE INDEX idx_channel_reaction_services_channel ON channel_reaction_services(channel_id);
```

## 🔄 Тригери

```sql
-- Автооновлення updated_at
CREATE TRIGGER update_channels_updated_at 
    BEFORE UPDATE ON channels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_channel_settings_updated_at 
    BEFORE UPDATE ON channel_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_services_updated_at 
    BEFORE UPDATE ON services
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

## 📋 Логіка роботи системи

### 1. Моніторинг нових постів
1. Бот перевіряє активні канали кожні 30 секунд
2. Знаходить нові пости (яких немає в таблиці `posts`)
3. Створює запис зі статусом 'new'

### 2. Обробка нового поста
1. Змінює статус на 'processing'
2. Читає налаштування з `channel_settings`
3. Застосовує рандомізацію для реакцій/репостів
4. Створює 3 замовлення в `orders` (views, reactions, reposts)

### 3. Створення порцій
1. Для кожного замовлення читає `portion_templates`
2. Розраховує кількості через `calculate_portion_details`
3. Створює 5 записів в `order_portions` для кожного типу

### 4. Відправка на Nakrutka API
1. Формує запити згідно API Nakrutka
2. Використовує drip-feed параметри з порцій
3. Зберігає ID замовлень від API

### 5. Моніторинг виконання
1. Періодично перевіряє статус через API
2. Оновлює статуси в БД
3. Логує важливі події

## 🎯 Приклад роботи з каналом mark_crypto_inside

### Налаштування:
- **Перегляди**: 4600 (без рандомізації)
- **Реакції**: 66 ±40% (від 40 до 92)
- **Репости**: 23 ±40% (від 14 до 32)

### Розподіл по порціях:
**Перегляди (70% за 3-5 год, 30% протягом дня):**
- Порції 1-4: швидкий старт
- Порція 5: повільний дотік (затримка 2г 36хв)

**Реакції (затримка старту 0-7 хв):**
- Розподіл: 3911(48), 3839(11), 3872(7)
- Пропорційно до загальної кількості

**Репости (затримка старту 5-19 хв):**
- Всі порції стартують через 5 хв
- Порція 5 через 45 хв

## 🛠️ Корисні SQL запити

### Статистика по каналу:
```sql
SELECT * FROM channel_monitoring;
```

### Симуляція нового поста:
```sql
SELECT * FROM simulate_new_post(1);
```

### Поточні активні замовлення:
```sql
SELECT * FROM orders 
WHERE status IN ('pending', 'in_progress') 
ORDER BY created_at DESC;
```

### Витрати за період:
```sql
SELECT 
    service_type,
    SUM(total_quantity) as total_ordered,
    SUM(total_quantity * price_per_1000 / 1000) as cost_usd
FROM orders o
JOIN services s ON s.nakrutka_id = o.service_id
WHERE DATE(o.created_at) = CURRENT_DATE
GROUP BY service_type;
```

## 🔐 Безпека

1. API ключі зберігаються в окремій таблиці
2. Використовуються CHECK constraints для валідації
3. Foreign keys забезпечують цілісність даних
4. Унікальні індекси запобігають дублюванню

## 📝 Важливі примітки

1. **Рандомізація** працює тільки для реакцій і репостів
2. **Drip-feed** реалізований через порції (5 для кожного типу)
3. **Затримки старту** різні для кожного типу накрутки
4. **Пропорції реакцій** зберігаються при рандомізації
5. **Моніторинг** відбувається кожні 30 секунд