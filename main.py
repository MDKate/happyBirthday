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
    # print(str((birthday['Дата рождения'][79]).strftime('%d.%m'))[0:5])
    # print(str((birthday['Дата рождения'][79]).strftime('%d.%m'))[0:5] == str(datetime.today().strftime('%d.%m'))[0:5])
    birthday1 = pd.DataFrame(columns=birthday.columns)
    for i in range(len(birthday)):
        # print(datetime.strftime(birthday['Дата рождения'][i], '%d.%m'), str(datetime.today().strftime('%d.%m'))[0:5])
        if datetime.strftime(birthday['Дата рождения'][i], '%d.%m') == str(datetime.today().strftime('%d.%m'))[0:5]:
            # print(birthday.iloc[i].values)
            birthday1.loc[len(birthday1.index)] = birthday.iloc[i]


    date = str(datetime.today().strftime("%Y-%m-%d")) + '5:00'
    # print(birthday1)
    formatted_date = datetime.strptime(date, "%Y-%m-%d %H:%M")
    if str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')) == str(formatted_date):
        for x in range(0, len(birthday1)):
            # print(str(birthday1.values[0]))
            NamePerson = str(birthday1.iloc[x].values[1]) + ' ' + str(birthday1.iloc[x].values[2])
            generateText = gratters.sample()['Текст']
            generateText = generateText.values[0]
            await botMes.send_message(open(os.path.abspath('chat.txt')).read(), 'Всем доброго дня! \n'
                                                                                f'Сегодня наш именинник {NamePerson} \n'
                                                                                f'{generateText}')



    # print(birthday1)
    # birthday1 = birthday[birthday['Дата рождения'] == str(datetime.today().strftime('%d.%m.%Y'))].reset_index(drop=True)
    # # print(birthday1)
    # # print(str(birthday1['Дата рождения']))
    # date = str(birthday1['Дата рождения'][0])[0:10] + ' 11:27'
    # formatted_date = datetime.strptime(date, "%Y-%m-%d %H:%M")
    # if str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')) == str(formatted_date):
    #     for i in range(0, len(birthday1)):
    #         NamePerson = birthday1['Имя'][i] + ' ' + birthday1['Отчество'][i]
    #         generateText = gratters.sample()['Текст']
    #         generateText = generateText.values[0]
    #         await botMes.send_message(open(os.path.abspath('chat.txt')).read(), 'Всем доброго дня! \n'
    #                                                                             f'Сегодня наш именинник {NamePerson} \n'
    #                                                                             f'{generateText}')

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

