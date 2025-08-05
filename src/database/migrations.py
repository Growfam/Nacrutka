"""
Database migrations and schema management
"""
import asyncio
from typing import List, Dict, Any
from datetime import datetime

from src.database.connection import DatabaseConnection
from src.utils.logger import get_logger
from src.config import settings

logger = get_logger(__name__)


class DatabaseMigrations:
    """Handle database migrations and schema updates"""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    async def check_database_exists(self) -> bool:
        """Check if database structure exists"""
        try:
            # Check if channels table exists
            result = await self.db.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'channels'
                )
            """)
            return result
        except Exception:
            return False

    async def get_current_version(self) -> int:
        """Get current database version"""
        try:
            # Check if migrations table exists
            exists = await self.db.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '_migrations'
                )
            """)

            if not exists:
                return 0

            # Get latest version
            version = await self.db.fetchval("""
                SELECT COALESCE(MAX(version), 0) 
                FROM _migrations 
                WHERE status = 'completed'
            """)
            return version or 0

        except Exception as e:
            logger.error(f"Failed to get database version: {e}")
            return 0

    async def create_migrations_table(self):
        """Create migrations tracking table"""
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id SERIAL PRIMARY KEY,
                version INTEGER UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'completed',
                error TEXT
            )
        """)

    async def apply_migration(self, version: int, name: str, sql: str):
        """Apply single migration"""
        try:
            # Start transaction
            async with self.db.transaction() as conn:
                # Execute migration
                await conn.execute(sql)

                # Record migration
                await conn.execute("""
                    INSERT INTO _migrations (version, name, status)
                    VALUES ($1, $2, 'completed')
                """, version, name)

            logger.info(f"Applied migration {version}: {name}")

        except Exception as e:
            # Record failed migration
            await self.db.execute("""
                INSERT INTO _migrations (version, name, status, error)
                VALUES ($1, $2, 'failed', $3)
                ON CONFLICT (version) 
                DO UPDATE SET status = 'failed', error = $3
            """, version, name, str(e))

            logger.error(f"Migration {version} failed: {e}")
            raise

    async def run_migrations(self):
        """Run all pending migrations"""
        # Ensure migrations table exists
        await self.create_migrations_table()

        # Get current version
        current_version = await self.get_current_version()
        logger.info(f"Current database version: {current_version}")

        # Get migrations to apply
        migrations = self.get_migrations()
        pending = [m for m in migrations if m['version'] > current_version]

        if not pending:
            logger.info("Database is up to date")
            return

        logger.info(f"Applying {len(pending)} migrations...")

        # Apply migrations in order
        for migration in sorted(pending, key=lambda x: x['version']):
            await self.apply_migration(
                migration['version'],
                migration['name'],
                migration['sql']
            )

        logger.info("All migrations completed successfully")

    def get_migrations(self) -> List[Dict[str, Any]]:
        """Get all migration definitions"""
        return [
            {
                'version': 1,
                'name': 'initial_schema',
                'sql': self._get_initial_schema()
            },
            {
                'version': 2,
                'name': 'add_indexes',
                'sql': self._get_indexes_migration()
            },
            {
                'version': 3,
                'name': 'add_functions',
                'sql': self._get_functions_migration()
            },
            {
                'version': 4,
                'name': 'add_views',
                'sql': self._get_views_migration()
            },
            {
                'version': 5,
                'name': 'add_constraints',
                'sql': self._get_constraints_migration()
            }
        ]

    def _get_initial_schema(self) -> str:
        """Get initial database schema"""
        return """
            -- Channels table
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                channel_username VARCHAR(255) UNIQUE NOT NULL,
                channel_id BIGINT UNIQUE,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Channel settings
            CREATE TABLE IF NOT EXISTS channel_settings (
                id SERIAL PRIMARY KEY,
                channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
                views_target INTEGER DEFAULT 0,
                reactions_target INTEGER DEFAULT 0,
                reposts_target INTEGER DEFAULT 0,
                reaction_types JSONB DEFAULT '["❤️", "🔥", "👍"]'::jsonb,
                views_service_id INTEGER,
                reactions_service_id INTEGER,
                reposts_service_id INTEGER,
                randomize_reactions BOOLEAN DEFAULT true,
                randomize_reposts BOOLEAN DEFAULT true,
                randomize_percent INTEGER DEFAULT 40,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Posts table
            CREATE TABLE IF NOT EXISTS posts (
                id SERIAL PRIMARY KEY,
                channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
                post_id INTEGER NOT NULL,
                post_url VARCHAR(500),
                published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                status VARCHAR(20) DEFAULT 'new' 
                    CHECK (status IN ('new', 'processing', 'completed', 'failed')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, post_id)
            );

            -- Orders table
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                nakrutka_order_id VARCHAR(100),
                service_type VARCHAR(20) 
                    CHECK (service_type IN ('views', 'reactions', 'reposts')),
                service_id INTEGER,
                total_quantity INTEGER,
                status VARCHAR(20) DEFAULT 'pending' 
                    CHECK (status IN ('pending', 'in_progress', 'completed', 'cancelled')),
                start_delay_minutes INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            );

            -- Order portions table
            CREATE TABLE IF NOT EXISTS order_portions (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
                portion_number INTEGER NOT NULL,
                quantity_per_run INTEGER NOT NULL,
                runs INTEGER NOT NULL,
                interval_minutes INTEGER NOT NULL,
                nakrutka_portion_id VARCHAR(100),
                status VARCHAR(20) DEFAULT 'waiting' 
                    CHECK (status IN ('waiting', 'running', 'completed')),
                scheduled_at TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            );

            -- Services table
            CREATE TABLE IF NOT EXISTS services (
                id SERIAL PRIMARY KEY,
                nakrutka_id INTEGER UNIQUE NOT NULL,
                service_name VARCHAR(500) NOT NULL,
                service_type VARCHAR(20) 
                    CHECK (service_type IN ('views', 'reactions', 'reposts', 'subscribers')),
                price_per_1000 DECIMAL(10, 4),
                min_quantity INTEGER,
                max_quantity INTEGER,
                is_active BOOLEAN DEFAULT true,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Portion templates table
            CREATE TABLE IF NOT EXISTS portion_templates (
                id SERIAL PRIMARY KEY,
                channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
                service_type VARCHAR(20) 
                    CHECK (service_type IN ('views', 'reactions', 'reposts')),
                portion_number INTEGER,
                quantity_percent DECIMAL(5,2),
                runs_formula VARCHAR(50),
                interval_minutes INTEGER,
                start_delay_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Channel reaction services table
            CREATE TABLE IF NOT EXISTS channel_reaction_services (
                id SERIAL PRIMARY KEY,
                channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
                service_id INTEGER,
                emoji VARCHAR(10),
                target_quantity INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- API keys table
            CREATE TABLE IF NOT EXISTS api_keys (
                id SERIAL PRIMARY KEY,
                service_name VARCHAR(50) NOT NULL 
                    CHECK (service_name IN ('nakrutka', 'telegram_bot')),
                api_key TEXT NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Logs table
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                level VARCHAR(10) CHECK (level IN ('info', 'warning', 'error')),
                message TEXT,
                context JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

    def _get_indexes_migration(self) -> str:
        """Get indexes migration"""
        return """
            -- Posts indexes
            CREATE INDEX IF NOT EXISTS idx_posts_channel_id ON posts(channel_id);
            CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
            CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);

            -- Orders indexes
            CREATE INDEX IF NOT EXISTS idx_orders_post_id ON orders(post_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_orders_service_type ON orders(service_type);
            CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);

            -- Order portions indexes
            CREATE INDEX IF NOT EXISTS idx_order_portions_order_id ON order_portions(order_id);
            CREATE INDEX IF NOT EXISTS idx_order_portions_status ON order_portions(status);
            CREATE INDEX IF NOT EXISTS idx_order_portions_scheduled_at ON order_portions(scheduled_at);

            -- Portion templates indexes
            CREATE INDEX IF NOT EXISTS idx_portion_templates_channel ON portion_templates(channel_id);
            CREATE INDEX IF NOT EXISTS idx_portion_templates_type ON portion_templates(service_type);

            -- Channel reaction services indexes
            CREATE INDEX IF NOT EXISTS idx_channel_reaction_services_channel ON channel_reaction_services(channel_id);

            -- Services indexes
            CREATE INDEX IF NOT EXISTS idx_services_type ON services(service_type);
            CREATE INDEX IF NOT EXISTS idx_services_active ON services(is_active);

            -- Logs indexes
            CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);
            CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
        """

    def _get_functions_migration(self) -> str:
        """Get functions migration"""
        return """
            -- Function to calculate random quantity
            CREATE OR REPLACE FUNCTION calculate_random_quantity(
                base_quantity INTEGER,
                randomize_percent INTEGER
            ) RETURNS INTEGER AS $$
            DECLARE
                min_quantity INTEGER;
                max_quantity INTEGER;
            BEGIN
                min_quantity := base_quantity - (base_quantity * randomize_percent / 100);
                max_quantity := base_quantity + (base_quantity * randomize_percent / 100);
                
                RETURN floor(random() * (max_quantity - min_quantity + 1) + min_quantity);
            END;
            $$ LANGUAGE plpgsql;

            -- Function to calculate portion details
            CREATE OR REPLACE FUNCTION calculate_portion_details(
                p_channel_id INTEGER,
                p_service_type VARCHAR,
                p_total_quantity INTEGER
            ) RETURNS TABLE (
                portion_number INTEGER,
                quantity_per_run INTEGER,
                runs INTEGER,
                total_in_portion INTEGER,
                interval_minutes INTEGER,
                start_delay_minutes INTEGER
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    pt.portion_number,
                    CASE 
                        WHEN pt.runs_formula LIKE 'quantity/%' THEN 
                            CAST(REPLACE(pt.runs_formula, 'quantity/', '') AS INTEGER)
                        ELSE 1
                    END as quantity_per_run,
                    CASE 
                        WHEN pt.runs_formula LIKE 'quantity/%' THEN 
                            CAST(ROUND(p_total_quantity * pt.quantity_percent / 100.0 / 
                                 CAST(REPLACE(pt.runs_formula, 'quantity/', '') AS INTEGER)) AS INTEGER)
                        ELSE 
                            CAST(ROUND(p_total_quantity * pt.quantity_percent / 100.0) AS INTEGER)
                    END as runs,
                    CAST(ROUND(p_total_quantity * pt.quantity_percent / 100.0) AS INTEGER) as total_in_portion,
                    pt.interval_minutes,
                    pt.start_delay_minutes
                FROM portion_templates pt
                WHERE pt.channel_id = p_channel_id 
                AND pt.service_type = p_service_type
                ORDER BY pt.portion_number;
            END;
            $$ LANGUAGE plpgsql;

            -- Function to update updated_at timestamp
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """

    def _get_views_migration(self) -> str:
        """Get views migration"""
        return """
            -- Channel monitoring view
            CREATE OR REPLACE VIEW channel_monitoring AS
            SELECT 
                c.id,
                c.channel_username,
                c.channel_id,
                c.is_active,
                cs.views_target,
                cs.reactions_target,
                cs.reposts_target,
                cs.randomize_percent,
                COUNT(DISTINCT p.id) as total_posts,
                COUNT(DISTINCT CASE WHEN p.status = 'processing' THEN p.id END) as active_posts,
                COUNT(DISTINCT CASE WHEN p.created_at > NOW() - INTERVAL '24 hours' THEN p.id END) as posts_24h
            FROM channels c
            LEFT JOIN channel_settings cs ON cs.channel_id = c.id
            LEFT JOIN posts p ON p.channel_id = c.id
            GROUP BY c.id, c.channel_username, c.channel_id, c.is_active, 
                     cs.views_target, cs.reactions_target, cs.reposts_target,
                     cs.randomize_percent;

            -- Order statistics view
            CREATE OR REPLACE VIEW order_statistics AS
            SELECT 
                DATE(o.created_at) as date,
                o.service_type,
                COUNT(o.id) as order_count,
                SUM(o.total_quantity) as total_quantity,
                SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost,
                AVG(o.total_quantity) as avg_quantity
            FROM orders o
            LEFT JOIN services s ON s.nakrutka_id = o.service_id
            GROUP BY DATE(o.created_at), o.service_type;
        """

    def _get_constraints_migration(self) -> str:
        """Get constraints and triggers migration"""
        return """
            -- Add triggers for updated_at
            DROP TRIGGER IF EXISTS update_channels_updated_at ON channels;
            CREATE TRIGGER update_channels_updated_at 
                BEFORE UPDATE ON channels
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

            DROP TRIGGER IF EXISTS update_channel_settings_updated_at ON channel_settings;
            CREATE TRIGGER update_channel_settings_updated_at 
                BEFORE UPDATE ON channel_settings
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

            DROP TRIGGER IF EXISTS update_services_updated_at ON services;
            CREATE TRIGGER update_services_updated_at 
                BEFORE UPDATE ON services
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

            -- Add unique constraints
            ALTER TABLE channel_settings 
                ADD CONSTRAINT unique_channel_settings 
                UNIQUE (channel_id);

            ALTER TABLE portion_templates 
                ADD CONSTRAINT unique_portion_template 
                UNIQUE (channel_id, service_type, portion_number);

            -- Add check constraints
            ALTER TABLE channel_settings
                ADD CONSTRAINT check_randomize_percent 
                CHECK (randomize_percent >= 0 AND randomize_percent <= 100);

            ALTER TABLE portion_templates
                ADD CONSTRAINT check_quantity_percent 
                CHECK (quantity_percent > 0 AND quantity_percent <= 100);
        """

    async def verify_database_integrity(self) -> Dict[str, Any]:
        """Verify database integrity after migrations"""
        results = {
            'tables': {},
            'functions': {},
            'views': {},
            'indexes': {},
            'triggers': {}
        }

        # Check tables
        table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        tables = await self.db.fetch(table_query)
        for table in tables:
            table_name = table['table_name']
            count = await self.db.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            results['tables'][table_name] = {'exists': True, 'row_count': count}

        # Check functions
        function_query = """
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_schema = 'public' 
            AND routine_type = 'FUNCTION'
        """
        functions = await self.db.fetch(function_query)
        for func in functions:
            results['functions'][func['routine_name']] = True

        # Check views
        view_query = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
        """
        views = await self.db.fetch(view_query)
        for view in views:
            results['views'][view['table_name']] = True

        # Check indexes
        index_query = """
            SELECT indexname, tablename 
            FROM pg_indexes 
            WHERE schemaname = 'public'
        """
        indexes = await self.db.fetch(index_query)
        for idx in indexes:
            table = idx['tablename']
            if table not in results['indexes']:
                results['indexes'][table] = []
            results['indexes'][table].append(idx['indexname'])

        # Check triggers
        trigger_query = """
            SELECT trigger_name, event_object_table 
            FROM information_schema.triggers 
            WHERE trigger_schema = 'public'
        """
        triggers = await self.db.fetch(trigger_query)
        for trigger in triggers:
            table = trigger['event_object_table']
            if table not in results['triggers']:
                results['triggers'][table] = []
            results['triggers'][table].append(trigger['trigger_name'])

        return results


async def initialize_database(db: DatabaseConnection):
    """Initialize database with migrations"""
    migrations = DatabaseMigrations(db)

    # Check if database needs initialization
    if not await migrations.check_database_exists():
        logger.info("Database not initialized, running migrations...")
        await migrations.run_migrations()
    else:
        # Check for pending migrations
        current_version = await migrations.get_current_version()
        all_migrations = migrations.get_migrations()
        latest_version = max(m['version'] for m in all_migrations)

        if current_version < latest_version:
            logger.info(f"Database needs update from v{current_version} to v{latest_version}")
            await migrations.run_migrations()
        else:
            logger.info("Database schema is up to date")

    # Verify integrity
    logger.info("Verifying database integrity...")
    integrity = await migrations.verify_database_integrity()

    logger.info(f"Database has {len(integrity['tables'])} tables")
    logger.info(f"Database has {len(integrity['functions'])} functions")
    logger.info(f"Database has {len(integrity['views'])} views")

    return integrity