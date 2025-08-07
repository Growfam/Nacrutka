"""
Bot command handlers for admin interface with dual API support
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
from src.database.repositories.service_repo import service_repo
from src.database.models import ServiceType, APIProvider
from src.core.post_processor import post_processor
from src.services.order_manager import order_manager
from src.services.twiboost_client import twiboost_client
from src.services.nakrutochka_client import nakrutochka_client
from src.scheduler.tasks import task_scheduler
from src.bot.telegram_monitor import telegram_monitor
from src.utils.logger import get_logger
from src.config import settings


logger = get_logger(__name__)


class BotHandlers:
    """Admin bot command handlers with dual API support"""

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

        # Basic Commands
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("stats", admin_only(self.stats_command)))

        # Channel Management
        self.app.add_handler(CommandHandler("channels", admin_only(self.channels_command)))
        self.app.add_handler(CommandHandler("add_channel", admin_only(self.add_channel_command)))
        self.app.add_handler(CommandHandler("remove_channel", admin_only(self.remove_channel_command)))
        self.app.add_handler(CommandHandler("settings", admin_only(self.settings_command)))

        # Configuration
        self.app.add_handler(CommandHandler("configure", admin_only(self.configure_command)))
        self.app.add_handler(CommandHandler("set_views", admin_only(self.set_views_command)))
        self.app.add_handler(CommandHandler("set_reactions", admin_only(self.set_reactions_command)))
        self.app.add_handler(CommandHandler("set_reposts", admin_only(self.set_reposts_command)))

        # API Management (NEW)
        self.app.add_handler(CommandHandler("api_status", admin_only(self.api_status_command)))
        self.app.add_handler(CommandHandler("api_balance", admin_only(self.api_balance_command)))
        self.app.add_handler(CommandHandler("set_api", admin_only(self.set_api_command)))

        # Service Management
        self.app.add_handler(CommandHandler("services", admin_only(self.services_command)))
        self.app.add_handler(CommandHandler("sync_services", admin_only(self.sync_services_command)))
        self.app.add_handler(CommandHandler("sync_nakrutochka", admin_only(self.sync_nakrutochka_command)))
        self.app.add_handler(CommandHandler("check_db", admin_only(self.check_db_command)))

        # Order Management
        self.app.add_handler(CommandHandler("orders", admin_only(self.orders_command)))
        self.app.add_handler(CommandHandler("balance", admin_only(self.balance_command)))

        # System Commands
        self.app.add_handler(CommandHandler("tasks", admin_only(self.tasks_command)))
        self.app.add_handler(CommandHandler("trigger", admin_only(self.trigger_task_command)))

        # Callback queries
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))

        logger.info("Bot handlers registered with dual API support")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user

        welcome_text = (
            f"👋 Welcome {user.first_name}!\n\n"
            f"🤖 *Telegram SMM Bot*\n"
            f"Automated promotion service with dual API support\n\n"
            f"🔧 *Active APIs:*\n"
            f"• Twiboost - Views\n"
            f"• Nakrutochka - Reactions & Reposts\n\n"
        )

        if settings.admin_telegram_id and user.id == settings.admin_telegram_id:
            welcome_text += (
                "✅ *Admin access granted*\n\n"
                "Available commands:\n"
                "/help - Show all commands\n"
                "/stats - View statistics\n"
                "/api\\_status - API statistics\n"
                "/api\\_balance - Check balances\n"
                "/channels - Manage channels\n"
                "/configure - Configure channel settings\n"
                "/services - View available services\n"
                "/orders - View recent orders\n"
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
/add\\_channel `<channel_id>` - Add new channel
/remove\\_channel `<channel_id>` - Remove channel
/settings `<channel_id>` - View channel settings

*Configuration:*
/configure `<channel_id>` - Interactive configuration
/set\\_views `<channel_id> <quantity>` - Set views amount
/set\\_reactions `<channel_id> <quantity>` - Set reactions amount
/set\\_reposts `<channel_id> <quantity>` - Set reposts amount

*API Management:*
/api\\_status - View API statistics
/api\\_balance - Check API balances
/set\\_api `<channel_id> <service> <api>` - Set API for service
/sync\\_nakrutochka - Sync Nakrutochka services

*Services:*
/services - View available services
/sync\\_services - Force sync all services
/check\\_db - Check services in database

*Monitoring:*
/stats - System statistics
/orders - Recent orders
/balance - API balances

*System:*
/tasks - Scheduled tasks
/trigger `<task_id>` - Manually trigger task

*Examples:*
`/add_channel -1001234567890`
`/set_views -1001234567890 1000`
`/set_api -1001234567890 reactions nakrutochka`
        """

        await update.message.reply_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN
        )

    async def api_status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /api_status command - show API statistics"""
        try:
            # Get statistics by API
            api_stats = await order_repo.get_statistics_by_api()

            # Get service counts
            service_counts = await service_repo.get_all_active_services_count()

            # Get balances
            balances = await order_manager.get_api_balances()

            text = "*📊 API Status*\n\n"

            # Twiboost
            text += "*🔵 Twiboost:*\n"
            text += f"• Balance: {balances['twiboost'].get('balance', 0)} {balances['twiboost'].get('currency', 'USD')}\n"
            text += f"• Services: Views({service_counts.get('twiboost', {}).get('views', 0)})\n"
            if 'twiboost' in api_stats:
                stats = api_stats['twiboost']
                text += f"• Orders: {stats['total']} total, {stats['completed']} completed\n"
                text += f"• Success rate: {(stats['completed']/stats['total']*100 if stats['total'] > 0 else 0):.1f}%\n"
                if stats['avg_completion_time']:
                    text += f"• Avg time: {stats['avg_completion_time']:.1f}s\n"
            text += "\n"

            # Nakrutochka
            text += "*🟢 Nakrutochka:*\n"
            text += f"• Balance: {balances['nakrutochka'].get('balance', 0)} {balances['nakrutochka'].get('currency', 'RUB')}\n"
            text += f"• Services: Reactions({service_counts.get('nakrutochka', {}).get('reactions', 0)}), "
            text += f"Reposts({service_counts.get('nakrutochka', {}).get('reposts', 0)})\n"
            if 'nakrutochka' in api_stats:
                stats = api_stats['nakrutochka']
                text += f"• Orders: {stats['total']} total, {stats['completed']} completed\n"
                text += f"• Success rate: {(stats['completed']/stats['total']*100 if stats['total'] > 0 else 0):.1f}%\n"
                if stats['avg_completion_time']:
                    text += f"• Avg time: {stats['avg_completion_time']:.1f}s\n"

            text += f"\n*Configuration:*\n"
            text += f"• Fallback enabled: {'✅' if settings.enable_api_fallback else '❌'}\n"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"API status command error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def api_balance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /api_balance command - show balances from both APIs"""
        try:
            balances = await order_manager.get_api_balances()

            text = "*💰 API Balances:*\n\n"

            # Twiboost
            if 'error' not in balances.get('twiboost', {}):
                text += f"*🔵 Twiboost:* {balances['twiboost']['balance']} {balances['twiboost']['currency']}\n"
            else:
                text += f"*🔵 Twiboost:* ❌ Error\n"

            # Nakrutochka
            if 'error' not in balances.get('nakrutochka', {}):
                text += f"*🟢 Nakrutochka:* {balances['nakrutochka']['balance']} {balances['nakrutochka']['currency']}\n"
            else:
                text += f"*🟢 Nakrutochka:* ❌ Error\n"

            # Calculate total in USD (approximate)
            total_usd = 0
            if 'error' not in balances.get('twiboost', {}):
                total_usd += float(balances['twiboost'].get('balance', 0))
            if 'error' not in balances.get('nakrutochka', {}):
                # Assuming RUB, convert to USD (approximate rate)
                total_usd += float(balances['nakrutochka'].get('balance', 0)) / 90

            text += f"\n*💵 Total (approx):* ${total_usd:.2f} USD"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"API balance command error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def set_api_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /set_api command - set preferred API for service type"""
        if len(context.args) < 3:
            await update.message.reply_text(
                "Usage: /set\\_api `<channel_id> <service_type> <api>`\n"
                "Example: /set\\_api `-1001234567890 reactions nakrutochka`\n\n"
                "*Service types:* views, reactions, reposts\n"
                "*APIs:* twiboost, nakrutochka",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])
            service_type = context.args[1].lower()
            api = context.args[2].lower()

            if service_type not in ["views", "reactions", "reposts"]:
                await update.message.reply_text("❌ Invalid service type")
                return

            if api not in ["twiboost", "nakrutochka"]:
                await update.message.reply_text("❌ Invalid API")
                return

            # Update preferences
            await channel_repo.update_api_preference(channel_id, service_type, api)

            await update.message.reply_text(
                f"✅ API preference updated!\n\n"
                f"*Channel:* `{channel_id}`\n"
                f"*Service:* {service_type}\n"
                f"*API:* {api}",
                parse_mode=ParseMode.MARKDOWN
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid channel ID")
        except Exception as e:
            logger.error(f"Set API error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def sync_nakrutochka_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /sync_nakrutochka command - force sync Nakrutochka services"""
        try:
            await update.message.reply_text("🔄 Syncing Nakrutochka services...")

            # Get services from API
            services = await nakrutochka_client.get_services(force_refresh=True)

            # Save to database
            synced_count = await service_repo.sync_nakrutochka_services(services)

            # Get counts by type
            reaction_services = await service_repo.get_nakrutochka_services_by_type("reactions")
            repost_services = await service_repo.get_nakrutochka_services_by_type("reposts")

            text = f"✅ *Nakrutochka services synced!*\n\n"
            text += f"Total from API: {len(services)}\n"
            text += f"Synced to DB: {synced_count}\n\n"
            text += f"*By type:*\n"
            text += f"• Reactions: {len(reaction_services)}\n"

            # Show sample reactions
            if reaction_services[:3]:
                text += "  _Examples:_\n"
                for srv in reaction_services[:3]:
                    text += f"  - {srv.name[:40]}\n"

            text += f"• Reposts: {len(repost_services)}\n"

            if repost_services[:3]:
                text += "  _Examples:_\n"
                for srv in repost_services[:3]:
                    text += f"  - {srv.name[:40]}\n"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Sync Nakrutochka error: {e}")
            await update.message.reply_text(f"❌ Sync failed: {str(e)}")

    async def configure_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /configure command - interactive configuration"""
        if not context.args:
            channels = await channel_repo.get_active_channels()

            if not channels:
                await update.message.reply_text("📭 No active channels. Add one with /add\\_channel")
                return

            text = "📝 *Select channel to configure:*\n\n"
            for channel in channels:
                text += f"• {channel.title}\n  `/configure {channel.id}`\n\n"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
            return

        try:
            channel_id = int(context.args[0])
            channel = await channel_repo.get_channel(channel_id)

            if not channel:
                await update.message.reply_text("❌ Channel not found")
                return

            # Create inline keyboard for configuration
            keyboard = [
                [
                    InlineKeyboardButton("👁 Views", callback_data=f"config_views_{channel_id}"),
                    InlineKeyboardButton("❤️ Reactions", callback_data=f"config_reactions_{channel_id}")
                ],
                [
                    InlineKeyboardButton("🔄 Reposts", callback_data=f"config_reposts_{channel_id}"),
                    InlineKeyboardButton("🔧 API Settings", callback_data=f"config_api_{channel_id}")
                ],
                [
                    InlineKeyboardButton("📊 Show Current", callback_data=f"show_settings_{channel_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"⚙️ *Configure {channel.title}*\n\n"
                f"Select service to configure:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid channel ID")
        except Exception as e:
            logger.error(f"Configure command error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def set_views_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /set_views command"""
        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /set\\_views `<channel_id> <quantity>`\n"
                "Example: /set\\_views `-1001234567890 1000`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])
            quantity = int(context.args[1])

            # Update settings
            await channel_repo.create_settings({
                "channel_id": channel_id,
                "service_type": ServiceType.VIEWS,
                "base_quantity": quantity,
                "randomization_percent": 0,
                "portions_count": 5,
                "fast_delivery_percent": 70
            })

            await update.message.reply_text(
                f"✅ Views settings updated!\n"
                f"Channel ID: `{channel_id}`\n"
                f"Quantity: {quantity}\n"
                f"Portions: 5 (70% fast delivery)\n"
                f"API: Twiboost",
                parse_mode=ParseMode.MARKDOWN
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid values")
        except Exception as e:
            logger.error(f"Set views error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def set_reactions_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /set_reactions command"""
        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /set\\_reactions `<channel_id> <quantity> [distribution]`\n"
                "Example: /set\\_reactions `-1001234567890 100 👍:45,❤️:30,🔥:25`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])
            quantity = int(context.args[1])

            # Parse distribution if provided
            distribution = {"👍": 45, "❤️": 30, "🔥": 25}  # Default
            if len(context.args) > 2:
                dist_str = context.args[2]
                distribution = {}
                for pair in dist_str.split(','):
                    emoji, percent = pair.split(':')
                    distribution[emoji] = int(percent)

            # Update settings
            await channel_repo.create_settings({
                "channel_id": channel_id,
                "service_type": ServiceType.REACTIONS,
                "base_quantity": quantity,
                "randomization_percent": 40,
                "reaction_distribution": distribution,
                "portions_count": 1,
                "drops_per_run": 5,
                "run_interval": 23
            })

            dist_text = ", ".join([f"{e}: {p}%" for e, p in distribution.items()])

            await update.message.reply_text(
                f"✅ Reactions settings updated!\n"
                f"Channel ID: `{channel_id}`\n"
                f"Quantity: {quantity} (±40% random)\n"
                f"Distribution: {dist_text}\n"
                f"Drip-feed: 5 per run, 23 min interval\n"
                f"API: Nakrutochka",
                parse_mode=ParseMode.MARKDOWN
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid values")
        except Exception as e:
            logger.error(f"Set reactions error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def set_reposts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /set_reposts command"""
        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /set\\_reposts `<channel_id> <quantity>`\n"
                "Example: /set\\_reposts `-1001234567890 50`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        try:
            channel_id = int(context.args[0])
            quantity = int(context.args[1])

            # Update settings
            await channel_repo.create_settings({
                "channel_id": channel_id,
                "service_type": ServiceType.REPOSTS,
                "base_quantity": quantity,
                "randomization_percent": 40,
                "portions_count": 1,
                "repost_delay_minutes": 5,
                "drops_per_run": 3,
                "run_interval": 34
            })

            await update.message.reply_text(
                f"✅ Reposts settings updated!\n"
                f"Channel ID: `{channel_id}`\n"
                f"Quantity: {quantity} (±40% random)\n"
                f"Delay: 5 minutes\n"
                f"Drip-feed: 3 per run, 34 min interval\n"
                f"API: Nakrutochka",
                parse_mode=ParseMode.MARKDOWN
            )

        except ValueError:
            await update.message.reply_text("❌ Invalid values")
        except Exception as e:
            logger.error(f"Set reposts error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def sync_services_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /sync_services command - force sync from both APIs"""
        try:
            await update.message.reply_text("🔄 Syncing services from both APIs...")

            # Sync Twiboost
            twiboost_services = await twiboost_client.get_services(force_refresh=True)
            twiboost_synced = await service_repo.sync_twiboost_services(twiboost_services)

            # Sync Nakrutochka
            nakrutochka_services = await nakrutochka_client.get_services(force_refresh=True)
            nakrutochka_synced = await service_repo.sync_nakrutochka_services(nakrutochka_services)

            # Get counts
            service_counts = await service_repo.get_all_active_services_count()

            text = f"✅ *Services synced successfully!*\n\n"
            text += f"*Twiboost:*\n"
            text += f"• Total from API: {len(twiboost_services)}\n"
            text += f"• Synced: {twiboost_synced}\n"
            text += f"• Views: {service_counts.get('twiboost', {}).get('views', 0)}\n\n"

            text += f"*Nakrutochka:*\n"
            text += f"• Total from API: {len(nakrutochka_services)}\n"
            text += f"• Synced: {nakrutochka_synced}\n"
            text += f"• Reactions: {service_counts.get('nakrutochka', {}).get('reactions', 0)}\n"
            text += f"• Reposts: {service_counts.get('nakrutochka', {}).get('reposts', 0)}"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Sync services error: {e}")
            await update.message.reply_text(f"❌ Sync failed: {str(e)}")

    async def check_db_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /check_db command - check services in database"""
        try:
            # Get service counts
            service_counts = await service_repo.get_all_active_services_count()

            # Get samples
            view_services = await service_repo.get_services_by_type("views")
            reaction_services = await service_repo.get_nakrutochka_services_by_type("reactions")
            repost_services = await service_repo.get_nakrutochka_services_by_type("reposts")

            text = "*📊 Services in Database:*\n\n"

            # Twiboost
            text += "*🔵 Twiboost:*\n"
            if view_services:
                text += f"• Views: {len(view_services)} services\n"
                for s in view_services[:3]:
                    text += f"  - {s.name[:40]}\n  Rate: {s.rate}/1000\n"
                if len(view_services) > 3:
                    text += f"  _...and {len(view_services)-3} more_\n"
            else:
                text += "• No services found\n"
            text += "\n"

            # Nakrutochka
            text += "*🟢 Nakrutochka:*\n"
            if reaction_services:
                text += f"• Reactions: {len(reaction_services)} services\n"
                for s in reaction_services[:3]:
                    text += f"  - {s.name[:40]}\n  Rate: {s.rate}/1000\n"
                if len(reaction_services) > 3:
                    text += f"  _...and {len(reaction_services)-3} more_\n"

            if repost_services:
                text += f"• Reposts: {len(repost_services)} services\n"
                for s in repost_services[:3]:
                    text += f"  - {s.name[:40]}\n  Rate: {s.rate}/1000\n"
                if len(repost_services) > 3:
                    text += f"  _...and {len(repost_services)-3} more_\n"

            if not (view_services or reaction_services or repost_services):
                text += "❌ No services found in database!\n"
                text += "Run /sync\\_services to import from APIs"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Check DB error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        try:
            # Get statistics
            post_stats = await post_processor.get_processing_stats()
            order_stats = await order_manager.get_execution_stats()
            api_stats = order_stats.get("by_api", {})

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

*By API:*
• Twiboost: {api_stats.get('twiboost', {}).get('total', 0)} orders
• Nakrutochka: {api_stats.get('nakrutochka', {}).get('total', 0)} orders

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

                # Get API preferences
                api_prefs = await channel_repo.get_api_preferences(channel.id)

                text += (
                    f"• *{channel.title}*\n"
                    f"  ID: `{channel.id}`\n"
                    f"  Username: @{channel.username or 'N/A'}\n"
                    f"  Posts (24h): {len(recent_posts)}\n"
                    f"  APIs: V-{api_prefs.get('views', 'T')[0]}, "
                    f"Re-{api_prefs.get('reactions', 'N')[0]}, "
                    f"Rp-{api_prefs.get('reposts', 'N')[0]}\n\n"
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
                    f"Default settings created:\n"
                    f"• Views: Twiboost\n"
                    f"• Reactions: Nakrutochka\n"
                    f"• Reposts: Nakrutochka\n\n"
                    f"Use /configure to adjust.",
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
            api_prefs = await channel_repo.get_api_preferences(channel_id)

            text = f"*⚙️ Settings for {channel.title}*\n\n"

            for setting in settings_list:
                text += f"*{setting.service_type.upper()}:*\n"
                text += f"• API: {api_prefs.get(setting.service_type, 'default')}\n"
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
            active_orders = order_manager.active_orders

            text = "*📦 Recent Orders:*\n\n"

            if not active_orders:
                text += "No active orders"
            else:
                for order_id, (api_provider, external_id) in list(active_orders.items())[:10]:
                    order = await order_repo.get_order(order_id)
                    if order:
                        text += (
                            f"• Order #{order.id}\n"
                            f"  Type: {order.service_type}\n"
                            f"  Quantity: {order.actual_quantity}\n"
                            f"  Status: {order.status}\n"
                            f"  API: {api_provider}\n"
                            f"  External ID: {external_id}\n\n"
                        )

            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN
            )

        except Exception as e:
            logger.error(f"Orders command error: {e}")
            await update.message.reply_text("❌ Failed to get orders")

    async def balance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /balance command - show all API balances"""
        try:
            balances = await order_manager.get_api_balances()

            text = "*💰 API Balances:*\n\n"
            text += f"*Twiboost:* {balances['twiboost'].get('balance', 0)} {balances['twiboost'].get('currency', 'USD')}\n"
            text += f"*Nakrutochka:* {balances['nakrutochka'].get('balance', 0)} {balances['nakrutochka'].get('currency', 'RUB')}"

            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Balance command error: {e}")
            await update.message.reply_text("❌ Failed to get balances")

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
            # Get services from database
            view_services = await service_repo.get_services_by_type("views")
            reaction_services = await service_repo.get_nakrutochka_services_by_type("reactions")
            repost_services = await service_repo.get_nakrutochka_services_by_type("reposts")

            if not (view_services or reaction_services or repost_services):
                await update.message.reply_text(
                    "❌ No services in database!\n"
                    "Run /sync\\_services to import from APIs",
                    parse_mode=ParseMode.MARKDOWN
                )
                return

            text = "*🛠 Available Services:*\n\n"

            if view_services:
                text += f"*👁 Views (Twiboost) - {len(view_services)}:*\n"
                for service in view_services[:3]:
                    text += f"• ID {service.service_id}: {service.name[:40]}\n  Rate: {service.rate}/1000\n"
                text += "\n"

            if reaction_services:
                text += f"*❤️ Reactions (Nakrutochka) - {len(reaction_services)}:*\n"
                for service in reaction_services[:3]:
                    text += f"• ID {service.service_id}: {service.name[:40]}\n  Rate: {service.rate}/1000\n"
                text += "\n"

            if repost_services:
                text += f"*🔄 Reposts (Nakrutochka) - {len(repost_services)}:*\n"
                for service in repost_services[:3]:
                    text += f"• ID {service.service_id}: {service.name[:40]}\n  Rate: {service.rate}/1000\n"

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

        data = query.data

        if data == "add_channel":
            await query.message.reply_text(
                "To add a channel, use:\n"
                "/add\\_channel `<channel_id>`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif data == "channel_settings":
            await query.message.reply_text(
                "To view channel settings, use:\n"
                "/settings `<channel_id>`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif data.startswith("config_"):
            parts = data.split("_")
            service = parts[1]
            channel_id = parts[2] if len(parts) > 2 else None

            if service == "views":
                await query.message.reply_text(
                    f"Configure views for channel {channel_id}:\n"
                    f"/set\\_views `{channel_id} <quantity>`",
                    parse_mode=ParseMode.MARKDOWN
                )
            elif service == "reactions":
                await query.message.reply_text(
                    f"Configure reactions for channel {channel_id}:\n"
                    f"/set\\_reactions `{channel_id} <quantity> [distribution]`",
                    parse_mode=ParseMode.MARKDOWN
                )
            elif service == "reposts":
                await query.message.reply_text(
                    f"Configure reposts for channel {channel_id}:\n"
                    f"/set\\_reposts `{channel_id} <quantity>`",
                    parse_mode=ParseMode.MARKDOWN
                )
            elif service == "api":
                await query.message.reply_text(
                    f"Configure API for channel {channel_id}:\n"
                    f"/set\\_api `{channel_id} <service> <api>`\n\n"
                    f"Example: /set\\_api `{channel_id} reactions nakrutochka`",
                    parse_mode=ParseMode.MARKDOWN
                )
        elif data.startswith("show_settings_"):
            channel_id = int(data.split("_")[2])
            # Show current settings
            settings_list = await channel_repo.get_channel_settings(channel_id)
            api_prefs = await channel_repo.get_api_preferences(channel_id)

            text = "*Current Settings:*\n\n"
            for setting in settings_list:
                text += f"*{setting.service_type}:*\n"
                text += f"• Quantity: {setting.base_quantity}\n"
                text += f"• API: {api_prefs.get(setting.service_type, 'default')}\n\n"

            await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# Bot handlers will be initialized in main.py