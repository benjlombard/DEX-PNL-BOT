import logging
import os
import re
from dotenv import load_dotenv
import asyncio
import sys

# Try to import telegram modules with error handling
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.constants import ParseMode
    from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
except ImportError as e:
    print(f"Error importing telegram modules: {e}")
    print("Please run: pip install python-telegram-bot==20.8")
    sys.exit(1)

# Import your report modules
try:
    from ETH_PNL import WalletReport 
    from BNB_PNL import BNBReport
    from SOLANA_PNL import SOLReport
except ImportError as e:
    print(f"Error importing report modules: {e}")
    print("Make sure all PNL modules are in the same directory")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

def is_valid_evm_address(address):
    """Check if the given string is a valid EVM address."""
    return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address))

def is_valid_solana_address(address):
    """Basic check for Solana address format."""
    return 32 <= len(address) <= 44 and address.isalnum()

class DEXPNLBot:
    def __init__(self):
        load_dotenv()
        self.token = os.getenv('TELEGRAM_TOKEN')
        
        if not self.token:
            raise ValueError("TELEGRAM_TOKEN not found in .env file")
        
        # Initialize application
        self.application = None
        self.user_data = {}  # Simple in-memory storage for user states
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        keyboard = [
            [
                InlineKeyboardButton("🔵 Ethereum", callback_data='eth'),
                InlineKeyboardButton("🟡 Binance", callback_data='bnb'),
                InlineKeyboardButton("🟣 Solana", callback_data='sol')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = (
            "🤖 **Welcome to DEX PNL Bot!**\n\n"
            "This bot analyzes your trading performance on decentralized exchanges.\n\n"
            "📊 **Features:**\n"
            "• Profit/Loss analysis\n"
            "• Win/Loss ratios\n"
            "• Trading statistics\n"
            "• Excel reports\n\n"
            "**Choose your blockchain:**"
        )
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button presses"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        chain = query.data
        
        # Store user's chain selection
        self.user_data[user_id] = {'chain': chain}
        
        chain_names = {
            'eth': '🔵 Ethereum',
            'bnb': '🟡 Binance Smart Chain',
            'sol': '🟣 Solana'
        }
        
        chain_name = chain_names.get(chain, chain.upper())
        
        await query.edit_message_text(
            text=f"✅ **Selected: {chain_name}**\n\n"
                 f"Now please send me your wallet address for {chain_name}:",
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle wallet address input"""
        user_id = update.message.from_user.id
        wallet_address = update.message.text.strip()
        
        # Check if user has selected a chain
        if user_id not in self.user_data:
            await update.message.reply_text(
                "⚠️ Please start by selecting a blockchain using /start"
            )
            return
        
        chain = self.user_data[user_id].get('chain')
        if not chain:
            await update.message.reply_text(
                "⚠️ Please select a blockchain first using /start"
            )
            return
        
        # Validate wallet address
        if chain in ['eth', 'bnb']:
            if not is_valid_evm_address(wallet_address):
                await update.message.reply_text(
                    "❌ Invalid EVM address format.\n"
                    "Please provide a valid address (e.g., 0x1234...)"
                )
                return
        elif chain == 'sol':
            if not is_valid_solana_address(wallet_address):
                await update.message.reply_text(
                    "❌ Invalid Solana address format.\n"
                    "Please provide a valid Solana address."
                )
                return
        
        # Send processing message
        chain_names = {
            'eth': 'Ethereum',
            'bnb': 'Binance Smart Chain', 
            'sol': 'Solana'
        }
        chain_name = chain_names.get(chain, chain.upper())
        
        processing_msg = await update.message.reply_text(
            f"⏳ **Generating {chain_name} PNL Report**\n\n"
            f"Wallet: `{wallet_address}`\n"
            f"Please wait, this may take 1-2 minutes...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Generate report asynchronously
        try:
            await self.generate_report(update, wallet_address, chain, processing_msg)
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            await processing_msg.edit_text(
                f"❌ **Error generating report**\n\n"
                f"Error: `{str(e)}`\n\n"
                f"Please try again or contact support.",
                parse_mode=ParseMode.MARKDOWN
            )

    async def generate_report(self, update: Update, wallet_address: str, chain: str, processing_msg):
        """Generate and send PNL report"""
        try:
            # Select appropriate report class
            if chain == 'eth':
                report_class = WalletReport
            elif chain == 'bnb':
                report_class = BNBReport
            elif chain == 'sol':
                report_class = SOLReport
            else:
                raise ValueError(f"Unsupported chain: {chain}")
            
            # Create report instance
            report = report_class(wallet_address)
            
            # Generate report (run in thread to avoid blocking)
            await asyncio.get_event_loop().run_in_executor(
                None, report.generate_report
            )
            
            # Check if report file exists
            report_file = os.path.join("reports", f"{wallet_address}.xlsx")
            if not os.path.exists(report_file):
                raise FileNotFoundError("Report file was not generated")
            
            # Send the report file
            chain_names = {
                'eth': 'Ethereum',
                'bnb': 'Binance_Smart_Chain',
                'sol': 'Solana'
            }
            chain_name = chain_names.get(chain, chain.upper())
            
            with open(report_file, 'rb') as file:
                await update.message.reply_document(
                    document=file,
                    filename=f"{chain_name}_PNL_{wallet_address[:8]}.xlsx",
                    caption=f"📊 **{chain_name} PNL Report**\n\n"
                           f"Wallet: `{wallet_address}`\n"
                           f"Period: Last 30 days",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            # Delete processing message
            try:
                await processing_msg.delete()
            except:
                pass
            
            logger.info(f"Report sent successfully for {wallet_address} on {chain}")
            
        except Exception as e:
            logger.error(f"Error in generate_report: {e}")
            await processing_msg.edit_text(
                f"❌ **Failed to generate report**\n\n"
                f"Wallet: `{wallet_address}`\n"
                f"Error: `{str(e)}`\n\n"
                f"Please check your wallet address and try again.",
                parse_mode=ParseMode.MARKDOWN
            )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        help_text = (
            "🤖 **DEX PNL Bot Help**\n\n"
            "**Commands:**\n"
            "• /start - Start the bot and select blockchain\n"
            "• /help - Show this help message\n\n"
            "**Supported Chains:**\n"
            "• 🔵 Ethereum\n"
            "• 🟡 Binance Smart Chain\n"
            "• 🟣 Solana\n\n"
            "**How to use:**\n"
            "1. Send /start\n"
            "2. Select your blockchain\n"
            "3. Send your wallet address\n"
            "4. Receive your PNL report\n\n"
            "**Note:** Reports analyze the last 30 days of trading activity."
        )
        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors"""
        logger.error("Exception while handling update:", exc_info=context.error)

    def run(self):
        """Run the bot"""
        # Create application
        self.application = Application.builder().token(self.token).build()
        
        # Add handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.application.add_error_handler(self.error_handler)
        
        print("🤖 DEX PNL Bot starting...")
        print("✅ Bot is ready!")
        print("📱 Send /start to your bot to begin")
        
        # Start polling
        self.application.run_polling(drop_pending_updates=True)

def main():
    """Main function"""
    try:
        bot = DEXPNLBot()
        bot.run()
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()