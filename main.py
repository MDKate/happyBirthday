# Импорт библиотек
import pandas as pd
import numpy as np
from datetime import datetime
import os
import pathlib
import aioschedule
import asyncio

from aiogram import Bot, Dispatcher, executor, types

botMes = Bot(open(os.path.abspath('token.txt')).read())
bot = Dispatcher(botMes)


birthday = pd.read_excel(os.path.abspath('УИДР.xlsx'))
gratters = pd.read_excel(os.path.abspath('Поздравления.xlsx'))



async def on_startup(_):
    print('start')
    birthday = pd.read_excel(os.path.abspath('УИДР.xlsx'))
    gratters = pd.read_excel(os.path.abspath('Поздравления.xlsx'))
    asyncio.create_task(schedulerS(birthday, gratters))
#
#
async def job():
    birthday = pd.read_excel(os.path.abspath('УИДР.xlsx'))
    gratters = pd.read_excel(os.path.abspath('Поздравления.xlsx'))
    # print('aaaaaaaaaaa')
    birthday1 = birthday[birthday['Дата рождения'] == str(datetime.today().strftime('%d.%m.%Y'))].reset_index(drop=True)
    # print(birthday1)
    # print(len(birthday1))
    date = str(birthday1['Дата рождения'][0])[0:10] + ' 17:15'
    formatted_date = datetime.strptime(date, "%Y-%m-%d %H:%M")
    if str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')) == str(formatted_date):
        for i in range(0, len(birthday1)):
            NamePerson = birthday1['Имя'][i] + ' ' + birthday1['Отчество'][i]
            generateText = gratters.sample()['Текст']
            generateText = generateText.values[0]
            # print(str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')), str(formatted_date))
            # print('**********')
            await botMes.send_message(open(os.path.abspath('chat.txt')).read(), 'Всем доброго дня! \n'
                                                                                f'Сегодня наш именинник {NamePerson} \n'
                                                                                f'{generateText}')

@bot.message_handler(commands=['update_table'])  # обновить в базе таблицу vks
async def start(message: types.message):
    #Отправляем сообщение
    await botMes.send_message(message.from_user.id, "Отправьте в чат файл. Названия столбцов не должны меняться. "
                                                    "Не должно быть лишних пометок.")

@bot.message_handler(content_types=types.ContentType.DOCUMENT)  # Обрабатываем документ
async def doc_message(message: types.Message):
    document = message.document
    if ".xlsx" in document.file_name:
        docName = document.file_name.partition('.')[0]
        try:
            os.remove(os.path.abspath(docName + '.xlsx'))
        except:
            pass
        await document.download(destination_file=f'{document.file_name}')
        await botMes.send_message(message.from_user.id, "Таблица обновлена!")
        return gratters, birthday
    else:
        await botMes.send_message(message.from_user.id, "Отправьте файл в формате xlsx!")

@bot.message_handler(commands=['download'])  # Обрабатываем документ
async def doc_message(message: types.Message):
    await botMes.send_document(message.chat.id, open(os.path.abspath('УИДР.xlsx'), 'rb'))
    await botMes.send_document(message.chat.id, open(os.path.abspath('Поздравления.xlsx'), 'rb'))


async def schedulerS(birthday, gratters):
    # print(1)
    aioschedule.every(1).second.do(job)
    # aioschedule.every(1).minutes.do(job)
    # print(2)
    while True:
        # print(3)
        await aioschedule.run_pending()
        await asyncio.sleep(1)


if __name__ == '__main__':
    # Бесконечно запускаем бот и игнорим ошибки
    while True:
        try:
            # print(1)
            # schedulerS.start_process()
            executor.start_polling(bot, on_startup=on_startup)    #start_polling(bot, on_startup=on_startup, timeout=2)
            # print(2)
            # asyncio.run(on_startup(birthday, gratters))
        except:
            pass

