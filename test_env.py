# test_env.py
import os
from dotenv import load_dotenv

load_dotenv()

print("🔍 Checking environment variables:\n")

required_vars = [
    'DATABASE_URL',
    'SUPABASE_URL',
    'SUPABASE_KEY',
    'NAKRUTKA_API_KEY',
    'TELEGRAM_BOT_TOKEN'
]

for var in required_vars:
    value = os.getenv(var)
    if value:
        # Приховуємо частину значення для безпеки
        if len(value) > 10:
            masked = value[:5] + '...' + value[-5:]
        else:
            masked = '***'
        print(f"✅ {var}: {masked}")
    else:
        print(f"❌ {var}: NOT FOUND")

print("\n📊 Optional variables:")
print(f"CHECK_INTERVAL: {os.getenv('CHECK_INTERVAL', 'default: 30')}")
print(f"ENVIRONMENT: {os.getenv('ENVIRONMENT', 'default: development')}")