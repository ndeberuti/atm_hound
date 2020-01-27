import csv
import io
import os
import urllib.request
from urllib.request import urlretrieve
import pymongo
from pymongo import MongoClient, GEO2D
from urllib.request import urlopen


def scrap_csv():
    urllib.request.urlretrieve('http://cdn.buenosaires.gob.ar/datosabiertos/datasets/cajeros-automaticos/cajeros-automaticos.csv', "cajeros-automaticos.csv")
    print('Descargando csv')
    csv_file = open('cajeros-automaticos.csv','r', encoding="utf8")
    reader = csv.DictReader(csv_file)
    print('Conectandose al cliente Mongo')
    my_client = pymongo.MongoClient(CLIENT_KEYS)
    my_db = my_client["db_cajero"]
    # mycol = mydb["customers"]
    # mydict = {"name": "John", "address": "Highway 37"}
    # x = mycol.insert_one(mydict)
    # print(myclient.list_database_names())
    # db = myclient.tabla_cajero
    print('Generando coleccion con el csv')
    my_db.segment.drop()
    header = ["id", "banco", "red", "ubicacion",
              "localidad", "terminales", "no_vidente", "dolares", "calle",
              "altura", "calle2", "barrio", "comuna", "codigo_postal", "codigo_postal_argentino"]
    for each in reader:
        row = {}
        for field in header:
            row[field] = each[field]
        row['loc'] = [float(each['long']),float(each['lat'])]
        row['hits'] = 0
        my_db.segment.insert_one(row)

    my_db.segment.create_index([("loc", GEO2D)])
    print('Terminado!')


scrap_csv()
