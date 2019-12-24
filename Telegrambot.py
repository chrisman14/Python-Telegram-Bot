from telegram.ext import Updater, InlineQueryHandler, CommandHandler
import requests
import re

def get_url():
    contents = requests.get('https://random.dog/woof.json').json()    
    url = contents['url']
    return url

def bop(bot, update):
    url = get_url()
    chat_id = update.message.chat_id
    bot.send_photo(chat_id=chat_id, photo=url)


def start_send(bot, update):
    url = "Welcome to Custome Bot Owner Chrisman"
    chat_id = update.message.chat_id
    bot.send_message(chat_id=chat_id, text=url)

def charts(bot, update):
    url = "https://quickchart.io/chart?c={type:'pie',data:{labels:['January','February', 'March','April', 'May'], datasets:[{data:[50,60,70,180,190]}]}}"
    chat_id = update.message.chat_id
    bot.send_photo(chat_id=chat_id, photo=url)

def main():
    updater = Updater('934856262:AAFHNnlgwIS9JMw1QdLfIpyqpGEqyNB4jww')
    dp = updater.dispatcher
    dp.add_handler(CommandHandler('bop',bop))
    dp.add_handler(CommandHandler('showChart',charts))
    dp.add_handler(CommandHandler('start',start_send))
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()