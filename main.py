from dotenv import load_dotenv
import os
import logging
import asyncio
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters
from ETH_PNL import WalletReport  # Make sure the ETH_pnl.py is in the same directory or properly referenced
from BNB_PNL import BNBReport
from SOLANA_PNL import SOLReport
# Import BNBReport class

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Suppress httpx info logs
logging.getLogger("httpx").setLevel(logging.WARNING)

# List of allowed usernames
ALLOWED_USERS = {'henrytirla'}  # Replace with actual usernames

def is_valid_evm_address(address):
    """Check if the given string is a valid EVM address."""
    return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address))

class BotHandler:
    def __init__(self):
        load_dotenv()
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.application = Application.builder().token(self.token).build()
        self.add_handlers()

    def add_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CallbackQueryHandler(self.button))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("adduser", self.add_user_command))  # Add handler for adding users
        self.application.add_handler(CommandHandler("listusers", self.list_users_command))  # Handler to list users
        self.application.add_handler(CommandHandler("removeuser", self.remove_user_command))  # Handler to remove users
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_wallet_address))

    def user_allowed(self, username):
        return username.lower() in ALLOWED_USERS

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Sends a message with two inline buttons attached."""
        username = update.message.from_user.username
        logger.info(f'Button pressed by user {username}')
        #

        # if not self.user_allowed(username):
        #     await update.message.reply_text(
        #         "Subscription Starts at $99/Month. Contact [henrytirla](https://t.me/henrytirla) to subscribe",
        #         parse_mode=ParseMode.MARKDOWN
        #     )
        #     return

        keyboard = [
            [InlineKeyboardButton("ETH", callback_data='eth_pnl'), InlineKeyboardButton("BNB", callback_data='bnb_pnl'),InlineKeyboardButton("SOL", callback_data='sol_pnl')],
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Please choose the chain for which you want to get PNL:", reply_markup=reply_markup)

    async def button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Parses the CallbackQuery and asks for the wallet address."""
        query = update.callback_query
        username = query.from_user.username
        #
        # if not self.user_allowed(username):
        #     await query.answer("You are not authorized to use this bot.", show_alert=True)
        #     return

        await query.answer()
        context.user_data['chain'] = query.data

        await query.edit_message_text(text=f"Selected option: {query.data}. Please enter the wallet address:")

    async def handle_wallet_address(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handles the wallet address input and generates the report."""
        username = update.message.from_user.username
        #
        # if not self.user_allowed(username):
        #     await update.message.reply_text(
        #         "Subscription Starts at $99/Month. Contact [henrytirla](https://t.me/henrytirla) to subscribe",
        #         parse_mode=ParseMode.MARKDOWN
        #     )
        #     return

        wallet_address = update.message.text
        chain = context.user_data.get('chain')

        if not is_valid_evm_address(wallet_address)and chain != 'sol_pnl':
            await update.message.reply_text("Please enter a valid EVM wallet address.")
            return

        if not chain:
            await update.message.reply_text("Please choose a chain first by using /start command.")
            return

        await update.message.reply_text("Generating report, please wait... You can send another address it will scan subsequently")

        # Run the report generation concurrently
        asyncio.create_task(self.generate_and_send_report(update, context, chain, wallet_address))

    async def generate_and_send_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE, chain: str, wallet_address: str):
        """Generates the report and sends it to the user."""
        if chain == 'eth_pnl':
            report = WalletReport(wallet_address)
        elif chain == 'bnb_pnl':
            report = BNBReport(wallet_address)
        elif chain == 'sol_pnl':
            report = SOLReport(wallet_address)
        else:
            await update.message.reply_text("Invalid chain selected.")
            return

        await asyncio.to_thread(report.generate_report)

        output_file_path = os.path.join("reports", f"{wallet_address}.xlsx")

        # Send the generated report back to the user
        with open(output_file_path, 'rb') as file:
            await update.message.reply_document(document=file, filename=f"{wallet_address}.xlsx")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Displays info on how to use the bot."""
        username = update.message.from_user.username
        if not self.user_allowed(username):
            await update.message.reply_text("To use this bot subscribe.")
            return

        await update.message.reply_text("Use /start to test this bot.")

    async def add_user_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Adds a new user to the allowed users list."""
        username = update.message.from_user.username
        if username != "henrytirla":
            await update.message.reply_text("You are not authorized to add users.")
            return

        try:
            new_username = context.args[0]
            ALLOWED_USERS.add(new_username)
            await update.message.reply_text(f"User {new_username} added to the allowed users list.")
            logger.info(f'User {new_username} added by {username}')
        except IndexError:
            await update.message.reply_text("Please provide a valid username.")

    async def list_users_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Lists all allowed users."""
        username = update.message.from_user.username
        if not self.user_allowed(username):
            await update.message.reply_text("You are not authorized to use this bot.")
            return

        users_list = "\n".join(ALLOWED_USERS)
        await update.message.reply_text(f"Allowed users:\n{users_list}")

    async def remove_user_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Removes a user from the allowed users list."""
        username = update.message.from_user.username
        if username != "henrytirla":
            await update.message.reply_text("You are not authorized to remove users.")
            return

        try:
            remove_username = context.args[0]
            if remove_username in ALLOWED_USERS:
                ALLOWED_USERS.remove(remove_username)
                await update.message.reply_text(f"User {remove_username} removed from the allowed users list.")
                logger.info(f'User {remove_username} removed by {username}')
            else:
                await update.message.reply_text(f"User {remove_username} not found in the allowed users list.")
        except IndexError:
            await update.message.reply_text("Please provide a valid username.")

    def run(self):
        """Run the bot."""
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    bot_handler = BotHandler()
    bot_handler.run()
