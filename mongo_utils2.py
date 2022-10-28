import json

import pymongo
from bson.son import SON
from pymongo import MongoClient

MONGO_HOST = "127.0.0.1:27017"
FILE_MOCK = "MOCK_DATA.json"


def import_json(database, collection):
    client = MongoClient(MONGO_HOST)
    db = client[database]
    with open(FILE_MOCK, encoding="UTF-8") as f:
        file_data = json.load(f)
        db.get_collection(collection).insert_many(file_data)
    client.close()


def add_user(client, data):
    db = client['upsa1']  ## Se pone el nombre de la base de datos que se quiera usar
    inserted_id = db.users.insert_many(data).inserted_ids

    print("ID del Alumno creado ={}".format(inserted_id))
    'return inserted_id'


def add_users(client, data):
    db = client['upsa1']  ## Se pone el nombre de la base de datos que se quiera usar
    inserted_id = db.users.insert_many(data).inserted_ids

    print("ID de los Alumnos creados ={}".format(inserted_id))
    'return inserted_id'


def cuentaquery():
    collection = "users"
    MONGO_HOST = "127.0.0.1:27017"
    client = pymongo.MongoClient(MONGO_HOST)
    db = client['upsa1']
    query = {"$and": [{"Latitude": {"$gte": 30}}, {"Altitude": {"$lte": 10}}]}

    total = db.get_collection(collection).count_documents(query)
    return total


def calculaporcentajes():
    collection = "users"
    MONGO_HOST = "127.0.0.1:27017"
    client = pymongo.MongoClient(MONGO_HOST)
    db = client['upsa1']
    query = {'gender': 'Male'}
    query2 = {'gender': 'Female'}

    totalhombres = db.get_collection(collection).count_documents(query)
    totalmujeres = db.get_collection(collection).count_documents(query2)
    'print("Hombres:"+str(totalhombres))'
    'print("Mujeres:"+str(totalmujeres))'
    totalpersonas = totalmujeres + totalhombres
    'print("Total: "+str(totalpersonas))'
    porcentajehombres = round(totalhombres / totalpersonas, 4) * 100
    print("El porcentaje de hombres es:" + str(porcentajehombres) + "%.")
    print("El porcentaje de mujeres es:" + str(100 - porcentajehombres) + "%.")


def get_user_with_query(client, query):
    """
    Executes the query
    :param client:
    :param mongo_host:
    :param query:
    :return:
    """

    db = client['upsa1']
    'results = db.users.find(query).limit(1)'
    results = db.users.find(query)
    #
    return results


def update_user(client, id, data):
    """
    Update restaurant
    :param client:
    :param mongo_host:
    :param id:
    :param data:
    :return:
    """
    db = client['upsa1']
    db.users.update_one({'_id': id}, {"$set": data})


def actualizaip_user_check(collection, new_ip, name):
    client = MongoClient(MONGO_HOST)
    db = client['upsa1']
    data = {'ip_address': new_ip}
    query = {'first_name': name}
    'db.get_collection(collection).update_one(query, {"$set": data})'
    db.get_collection(collection).update_many(query, {"$set": data})

    result = db.get_collection(collection).find(query)
    return result


def borraCollection():
    client = MongoClient(MONGO_HOST)
    db = client['upsa1']
    collection = "users"

    db.get_collection(collection).delete_many({})


def mapreduce():
    client = MongoClient(MONGO_HOST)
    db = client['upsa1']
    collection = "users"
    pipeline = [
        {"$unwind": "$gender"},
        {"$group": {"_id": "$gender", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]
    #list(db.get_collection(collection).aggregate(pipeline)))
    print((list(db.get_collection(collection).aggregate(pipeline))))

