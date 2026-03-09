import random
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes

TOKEN = "7597740350:AAH1eqNGlHTbjkE7UUp80A9x6oqhINdz4Hs"


anekdots = [
    "Приходит {w1} к {w2} и говорит: 'Слушай, {w3}, давай больше так не будем!'",
    "— {w1}, ты зачем {w2} на работу принёс?\n— Так {w3} же сказал: 'Работай с тем, что любишь!'",
    "Сидят {w1}, {w2} и {w3}. Молчат. Потом {w1} говорит: 'Ну и странный же сегодня интернет…'",
    "Учитель спрашивает:\n— {w1}, сколько будет {w2} + {w3}?\n— А мы сегодня без калькулятора работаем?",
    "{w1} звонит {w2}:\n— Ты где?\n— Я с {w3} разбираюсь.\n— Опять интернет сломал?"
]


def make_anekdot(words):
    while len(words) < 3:
        words.append("что-то")

    template = random.choice(anekdots)

    return template.format(
        w1=words[0],
        w2=words[1],
        w3=words[2]
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    text = update.message.text
    words = text.split()

    joke = make_anekdot(words)

    await update.message.reply_text(joke)


def main():

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot started")

    app.run_polling()


if __name__ == "__main__":
    main()