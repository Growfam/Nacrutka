"""
Bot command handlers for admin interface
"""
from typing import Optional, Dict, Any, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
import json

from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.post_repo import post_repo
from src.database.repositories.order_repo import order_repo
from src.database.models import ServiceType
from src.core.post_processor import post_processor
from src.services.order_manager import order_manager
from src.services.twiboost_client import twiboost_client
from src.scheduler.tasks import task_scheduler
from src.bot.telegram_monitor import telegram_monitor
from src.utils.logger import get_logger
from src.config import settings


logger = get_logger(__name__)


class BotHandlers:
    """Admin bot command handlers"""

    def __init__(self, application: Application):
        self.app = application
        self.setup_handlers()

    def setup_handlers(self):
        """Register all command handlers"""
        # Admin check decorator
        def admin_only(func):
            async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
                user_id = update.effective_user.id
                if settings.admin_telegram_id and user_id != settings.admin_telegram_id:
                    await update.message.reply_text("❌ Access denied. Admin only.")
                    return
                return await func(update, context)
            return wrapper

        # Commands
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("stats", admin_only(self.stats_command)))
        self.app.add_handler(CommandHandler("channels", admin_only(self.channels_command)))
        self.app.add_handler(CommandHandler("add_channel", admin_only(self.add_channel_command)))
        self.app.add_handler(CommandHandler("remove_channel", admin_only(self.remove_channel_command)))
        self.app.add_handler(CommandHandler("settings", admin_only(self.settings_command)))
        self.app.add_handler(CommandHandler("orders", admin_only(self.orders_command)))
        self.app.add_handler(CommandHandler("balance", admin_only(self.balance_command)))
        self.app.add_handler(CommandHandler("tasks", admin_only(self.tasks_command)))
        self.app.add_handler(CommandHandler("trigger", admin_only(self.trigger_task_command)))
        self.app.add_handler(CommandHandler("services", admin_only(self.services_command)))

        # Callback queries
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))

        logger.info("Bot handlers registered")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user

        welcome_text = (
            f"👋 Welcome {user.first_name}!\n\n"
            f"🤖 *Telegram SMM Bot*\n"
            f"Automated promotion service for Telegram channels\n\n"
        )

        if settings.admin_telegram_id and user.id == settings.admin_telegram_id:
            welcome_text += (
                "✅ *Admin access granted*\n\n"
                "Available commands:\n"
                "/help - Show all commands\n"
                "/stats - View statistics\n"
                "/channels - Manage channels\n"
                "/orders - View recent orders\n"
                "/balance - Check balance\n"
                "/tasks - View scheduled tasks"
            )
        else:
            welcome_text += "ℹ️ This bot is for administrative use only."

        await update.message.reply_text(
            welcome_text,
            parse_mode=ParseMode.MARKDOWN
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        help_text = """
📋 *Available Commands:*

*Channel Management:*
/channels - List active channels
/add\_channel `<channel_id>` - Add new channel
/remove\_channel `<channel_id>` - Remove channel
/settings `<channel_id>` - Channel settings

*Monitoring:*
/stats - System statistics
/orders - Recent orders
/balance - Twiboost balance

*System:*
/tasks - Scheduled tasks
/trigger `<task_id>` - Manually trigger task
/services - Available Twiboost services

*Examples:*
`/add_channel -1001234567890`
`/settings -1001234567890`
`/trigger process_posts`
        """

        await update.message.reply_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN
        )

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        try:
            # Get statistics
            post_stats = await post_processor.get_processing_stats()
            order_stats = await order_manager.get_execution_stats()

            stats_text = f"""
📊 *System Statistics*

*Posts (24h):*
• Total: {post_stats.get('total', 0)}
• New: {post_stats.get('new', 0)}
• Completed: {post_stats.get('completed', 0)}
• Failed: {post_stats.get('failed', 0)}
• Success Rate: {post_stats.get('success_rate', 0):.1f}%

*Orders (24h):*
• Total: {order_stats.get('total_orders', 0)}
• Pending: {order_stats.get('by_status', {}).get('pending', {}).get('count', 0)}
• In Progress: {order_stats.get('by_status', {}).get('in_progress', {}).get('count', 0)}
• Completed: {order_stats.get('by_status', {}).get('completed', {}).get('count', 0)}
• Failed: {order_stats.get('by_status', {}).get('failed', {}).get('count', 0)}

*By Service:*
• Views: {order_stats.get('by_service', {}).get('views', {}).get('count', 0)}
• Reactions: {order_stats.get('by_service', {}).get('reactions', {}).get('count', 0)}
• Reposts: {order_stats.get('by_service', {}).get('reposts', {}).get('count', 0)}

*Session:*
• Posts Processed: {post_processor.processed_count}
• Orders Executed: {order_manager.execution_count}
• Active Orders: {len(order_manager.active_orders)}
• Errors: {post_processor.error_count + order_manager.error_count}
            """

            await update.message.reply_text(
                stats_text,
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Stats command error: {e}")
            await update.message.reply_text("❌ Failed to get statistics")

    async def channels_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /channels command"""
        try:
            channels = await channel_repo.get_active_channels()

            if not channels:
                await update.message.reply_text("📭 No active channels")
                return

            text = "*📢 Active Channels:*\n\n"

            for channel in channels:
                # Get recent posts count
                recent_posts = await post_repo.get_recent_posts(channel.id, hours=24)

                text += (
                    f"• *{channel.title}*\n"
                    f"  ID: `{channel.id}`\n"
                    f"  Username: @{channel.username or 'N/A'}\n"
                    f"  Posts (24h): {len(recent_posts)}\n\n"
                )

            # Add inline buttons for management
            keyboard = [
                [
                    InlineKeyboardButton("➕ Add Channel", callback_data="add_channel"),
                    InlineKeyboardButton("⚙️ Settings", callback_data="channel_settings")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )

        except Exception as e:
            logger.error(f"Channels command error: {e}")
            await update.message.reply_text("❌ Failed to get channels")

    async def add_channel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add_channel command"""
        if not context.args:
            await update.message.reply_text(
                "Usage: /add\\_channel `<channel_id>`\n"
                "Example: /add\\_channel `-1001234567890`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])

            # Add channel
            channel = await telegram_monitor.add_channel(channel_id)

            if channel:
                await update.message.reply_text(
                    f"✅ Channel added successfully!\n\n"
                    f"*{channel.title}*\n"
                    f"ID: `{channel.id}`\n"
                    f"Username: @{channel.username or 'N/A'}\n\n"
                    f"Default settings created. Use /settings to configure.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    "❌ Failed to add channel.\n"
                    "Make sure the bot is admin in the channel."
                )

        except ValueError:
            await update.message.reply_text("❌ Invalid channel ID")
        except Exception as e:
            logger.error(f"Add channel error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def remove_channel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /remove_channel command"""
        if not context.args:
            await update.message.reply_text(
                "Usage: /remove\\_channel `<channel_id>`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])

            await telegram_monitor.remove_channel(channel_id)

            await update.message.reply_text(
                f"✅ Channel {channel_id} removed from monitoring"
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid channel ID")
        except Exception as e:
            logger.error(f"Remove channel error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /settings command"""
        if not context.args:
            await update.message.reply_text(
                "Usage: /settings `<channel_id>`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])

            # Get channel and settings
            channel = await channel_repo.get_channel(channel_id)
            if not channel:
                await update.message.reply_text("❌ Channel not found")
                return

            settings_list = await channel_repo.get_channel_settings(channel_id)

            text = f"*⚙️ Settings for {channel.title}*\n\n"

            for setting in settings_list:
                text += f"*{setting.service_type.upper()}:*\n"
                text += f"• Base Quantity: {setting.base_quantity}\n"
                text += f"• Randomization: ±{setting.randomization_percent}%\n"

                if setting.service_type == ServiceType.VIEWS:
                    text += f"• Portions: {setting.portions_count}\n"
                    text += f"• Fast Delivery: {setting.fast_delivery_percent}%\n"
                elif setting.service_type == ServiceType.REACTIONS:
                    if setting.reaction_distribution:
                        text += f"• Distribution: {json.dumps(setting.reaction_distribution)}\n"
                    text += f"• Drops/Run: {setting.drops_per_run}\n"
                    text += f"• Interval: {setting.run_interval} min\n"
                elif setting.service_type == ServiceType.REPOSTS:
                    text += f"• Delay: {setting.repost_delay_minutes} min\n"
                    text += f"• Drops/Run: {setting.drops_per_run}\n"
                    text += f"• Interval: {setting.run_interval} min\n"

                text += "\n"

            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid channel ID")
        except Exception as e:
            logger.error(f"Settings command error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def orders_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /orders command"""
        try:
            # Get recent orders
            active_orders = await order_manager.active_orders

            text = "*📦 Recent Orders:*\n\n"

            if not active_orders:
                text += "No active orders"
            else:
                for order_id, twiboost_id in list(active_orders.items())[:10]:
                    order = await order_repo.get_order(order_id)
                    if order:
                        text += (
                            f"• Order #{order.id}\n"
                            f"  Type: {order.service_type}\n"
                            f"  Quantity: {order.actual_quantity}\n"
                            f"  Status: {order.status}\n"
                            f"  Twiboost: {twiboost_id}\n\n"
                        )

            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Orders command error: {e}")
            await update.message.reply_text("❌ Failed to get orders")

    async def balance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /balance command"""
        try:
            balance = await twiboost_client.get_balance()

            await update.message.reply_text(
                f"💰 *Twiboost Balance:*\n\n"
                f"Balance: {balance['balance']} {balance['currency']}",
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Balance command error: {e}")
            await update.message.reply_text("❌ Failed to get balance")

    async def tasks_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /tasks command"""
        try:
            jobs = task_scheduler.get_job_info()

            text = "*⏰ Scheduled Tasks:*\n\n"

            for job in jobs:
                stats = job.get('stats', {})
                text += (
                    f"• *{job['name']}*\n"
                    f"  ID: `{job['id']}`\n"
                    f"  Next Run: {job['next_run']}\n"
                    f"  Runs: {stats.get('runs', 0)}, Errors: {stats.get('errors', 0)}\n\n"
                )

            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Tasks command error: {e}")
            await update.message.reply_text("❌ Failed to get tasks")

    async def trigger_task_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /trigger command"""
        if not context.args:
            await update.message.reply_text(
                "Usage: /trigger `<task_id>`\n"
                "Example: /trigger `process_posts`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            task_id = context.args[0]

            success = await task_scheduler.trigger_job(task_id)

            if success:
                await update.message.reply_text(f"✅ Task `{task_id}` triggered")
            else:
                await update.message.reply_text(f"❌ Task `{task_id}` not found")

        except Exception as e:
            logger.error(f"Trigger task error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def services_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /services command"""
        try:
            services = await twiboost_client.get_services()

            # Filter relevant services
            relevant = [s for s in services if s['type'] in ['view', 'reaction', 'repost']]

            text = f"*🛠 Available Services ({len(relevant)}):*\n\n"

            for service in relevant[:20]:  # Limit to 20
                text += (
                    f"• ID: {service['service']}\n"
                    f"  Name: {service['name']}\n"
                    f"  Type: {service['type']}\n"
                    f"  Rate: {service['rate']}/1000\n\n"
                )

            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Services command error: {e}")
            await update.message.reply_text("❌ Failed to get services")

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries from inline buttons"""
        query = update.callback_query
        await query.answer()

        if query.data == "add_channel":
            await query.message.reply_text(
                "To add a channel, use:\n"
                "/add\\_channel `<channel_id>`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif query.data == "channel_settings":
            await query.message.reply_text(
                "To view channel settings, use:\n"
                "/settings `<channel_id>`",
                parse_mode=ParseMode.MARKDOWN
            )


# Bot handlers will be initialized in main.py