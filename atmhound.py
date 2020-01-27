import datetime
import logging
import pymongo
import telegram
import random
from bson import SON
from telegram.ext import CommandHandler, MessageHandler, Filters
from telegram.ext import Updater
from telegram import Location, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import schedule
import time


# El bot se llama ATM Hound. La informacion de los cajeros se encuentra en una base MongoDB. El script
# csvToMongoDB.py descarga el csv y genera la colección en la base de datos. Mantiene la misma informacion que el csv
# excepto que se crea un array 'loc' que contiene las coordenadas del cajero como 'long' y 'lat'. El bot inicia su
# flujo con el comando /start en el que solicita la ubicación actual del usuario, luego puede manejar la
# funcionalidad de mostrar los cajeros más cercanos con los comandos /banelco y /link. en el archivo config.txt se
# tiene todas las claves y tokens para referencia.

def startbot():
    start_handler = CommandHandler('start', start)
    banelco_handler = CommandHandler('Banelco', banelco)
    link_handler = CommandHandler('Link', link)
    end_handler = CommandHandler('Terminar', end_conversation)
    red_handler = CommandHandler('Seleccionar_Red', preguntar_red)

    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(link_handler)
    dispatcher.add_handler(banelco_handler)
    dispatcher.add_handler(end_handler)
    dispatcher.add_handler(red_handler)
    dispatcher.add_handler(MessageHandler(Filters.location, location))


def start(update, context):
    reply_markup = telegram.ReplyKeyboardMarkup(
        [[telegram.KeyboardButton('Compartir Ubicacion', request_location=True)]])
    context.bot.send_message(chat_id=update.effective_chat.id, text="Comparta su ubicación", reply_markup=reply_markup)


def end_conversation(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text='Adios!', reply_markup=ReplyKeyboardRemove())


def preguntar_red(update, context):
    reply_markup = telegram.ReplyKeyboardMarkup(
        [[telegram.KeyboardButton('/Banelco')], [telegram.KeyboardButton('/Link')]])
    context.bot.send_message(chat_id=update.effective_chat.id, text="Que red de cajeros quiere rastrear?",
                             reply_markup=reply_markup)


def preguntar_ok(update, context):
    reply_markup = telegram.ReplyKeyboardMarkup(
        [[telegram.KeyboardButton('/Terminar')], [telegram.KeyboardButton('/Seleccionar_red')]])
    context.bot.send_message(chat_id=update.effective_chat.id, text="Algo más?",
                             reply_markup=reply_markup)


def location(update, context):
    global user_location
    user_location = update.message.location
    preguntar_red(update, context)


def banelco(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text='Olfateando...', reply_markup=ReplyKeyboardRemove())
    db = my_client.db_cajero
    collection = db.segment
    # obtiene los cajeros cercanos
    obj = mongo_query_find_atm(collection, 'BANELCO', user_location['longitude'], user_location['latitude'])
    # muestra los cajeros cercanos y actualiza el campo 'extracciones' en alguno de los cajeros dada la probabilidad de
    # ser visitado
    mostrar_actualizar_cajeros(update, context, obj, collection)
    preguntar_ok(update, context)


def link(update, context):
    # misma funcionalidad que banelco
    context.bot.send_message(chat_id=update.effective_chat.id, text='Olfateando...', reply_markup=ReplyKeyboardRemove())
    db = my_client.db_cajero
    collection = db.segment
    obj = mongo_query_find_atm(collection, 'LINK', user_location['longitude'], user_location['latitude'])
    mostrar_actualizar_cajeros(update, context, obj, collection)
    preguntar_ok(update, context)


def show_map(update, context, cajero_markers):
    # forma la url para llamar a la api de google para mostrar mapas con la ubicación del usuario en rojo y en verde
    # la de los cajeros cercanos
    url_center = 'https://maps.googleapis.com/maps/api/staticmap?center=' + str(user_location['latitude']) + ',' + \
                 str(user_location['longitude']) + '&zoom=15&size=400x400&markers=size:tiny%7Ccolor:red%7C' + str(
        user_location['latitude']) + ',' \
                 + str(user_location['longitude'])
    markers = ''
    for x in cajero_markers:
        markers += '&markers=size:mid%7Ccolor:green%7Clabel:C%7C' + str(x[1]) + ',' + str(x[0])

    map_url = url_center + markers + '&key=APIKEY'
    # envia la imagen al chat
    context.bot.send_photo(chat_id=update.effective_chat.id, photo=map_url)


def mostrar_actualizar_cajeros(update, context, obj, collection):
    # valida que la consulta haya traido al menos un cajero
    if obj.count() > 0:
        cajero_markers = []
        generated_number = random.random()
        probabilities_of_extraction = [0.7, 0.9, 1]
        flag = 0
        i = 0
        for x in obj:
            # muestra los cajeros encontrados
            cajero_markers.append(x['loc'])
            text = '{} - {}'.format(x['banco'], x['ubicacion'])
            context.bot.send_message(chat_id=update.effective_chat.id, text=text)
            # calcula la probabilidad de que se visite el 1° (70%), 2°(20%) o 3° (10%) cajero y aumenta el contador
            # de extracciones en la coleccion de la base de datos
            if generated_number < probabilities_of_extraction[i] and flag == 0:
                collection.update_one({'_id': x['_id']}, {'$inc': {'extractions': 1}})
                flag = 1
            ++i
        show_map(update, context, cajero_markers)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id, text='No hay ningún cajero a menos de 5 cuadras')


def mongo_query_find_atm(collection, red, long, lat):
    RADIANS_TO_METERS_CONSTANT = 0.1 / 111.2
    query = {"red": red, "extracciones": {"$lt": 1000},
             "loc": SON([("$near", [long, lat]), ("$maxDistance", 5 * RADIANS_TO_METERS_CONSTANT)])}
    obj = collection.find(query).limit(3)
    return obj


def mongo_query_reset_extractions(collection):
    collection.update_many({}, {'$set': {'extracciones': 0}})


# actualizo los registros de extracciones con un job para el scheduler
def update_extractions_job(extraction_collection):
    mongo_query_reset_extractions(extraction_collection)


updater = Updater(token='BOT_TOKEN', use_context=True)
dispatcher = updater.dispatcher

my_client = pymongo.MongoClient(CLIENT_KEYS)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

startbot()

scheduler_collection = my_client.db_cajero.segment
# resetear el contador de extracciones de lunes a viernes a las 9 am
schedule.every().monday.at("09:00").do(update_extractions_job, extraction_collection=scheduler_collection)
schedule.every().tuesday.at("09:00").do(update_extractions_job, extraction_collection=scheduler_collection)
schedule.every().wednesday.at("09:00").do(update_extractions_job, extraction_collection=scheduler_collection)
schedule.every().thursday.at("09:00").do(update_extractions_job, extraction_collection=scheduler_collection)
schedule.every().friday.at("09:00").do(update_extractions_job, extraction_collection=scheduler_collection)

updater.start_polling()

while True:
    schedule.run_pending()
    time.sleep(1)
