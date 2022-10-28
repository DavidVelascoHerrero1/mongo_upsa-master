import pymongo

import mongo_utils2


def get_print_from_cursor(message, cursor):
    print(message)
    for item in cursor:
        print(item)


database = "upsa1"
collection = "users"
MONGO_HOST = "127.0.0.1:27017"
client = pymongo.MongoClient(MONGO_HOST)

'Apartado 1. Importar los datos a una base datos llamada upsa1 en la collecion users'
# mongo_utils2.import_json(database, collection)

'Apartado 2. Insertar en la base de datos upsa1'
'Apartado 2a. Un objeto'

data1 = [
    {
        "first_name": "David",
        "last_name": "Velasco",
        "email": "davidvelascoherrero1@gmail.com",
        "gender": "Male",
        "ip_address": "15.208.64.26",
        "Latitude": 43.6342238,
        "Altitude": -3.41144,
        "City": "Zamora",
        "University": "Upsa"
    }]

# mongo_utils2.add_user(client, data1)'

'Apartado 2b: 1 lista de 3 objets'
data2 = [
    {
        "first_name": "Manuel",
        "last_name": "Gomez",
        "email": "manogo@gmail.com",
        "gender": "Male",
        "ip_address": "15.208.64.26",
        "Latitude": 43.6342238,
        "Altitude": -3.41144,
        "City": "Madrid",
        "University": "Upsa"
    },
    {
        "first_name": "Lucia",
        "last_name": "Sanchez",
        "email": "lucisan@gmail.com",
        "gender": "Female",
        "ip_address": "5.208.64.76",
        "Latitude": 43.1342238,
        "Altitude": -2.41144,
        "City": "Salamanca",
        "University": "UPSA"
    },
    {
        "first_name": "Sergio",
        "last_name": "Suarez",
        "email": "sergiosua@gmail.com",
        "gender": "Male",
        "ip_address": "5.208.65.29",
        "Latitude": 43.1342238,
        "Altitude": -2.61144,
        "City": "Salamanca",
        "University": "UPSA"
    }]
#mongo_utils2.add_users(client, data2)

'Apartado 3. Hacer una query q me saque por consola el porcentaje de usuarios por género'
'Tip: db.get_collection(collection).count_documents(query)'
# mongo_utils2.calculaporcentajes()'

'Apartado 4: Actualizar la ip de un usuario nombre "Jervis" y '
'actualizarlo con "109.150.230.156/24" and comprobarlo haciendo una query.'
# queryJer = {'first_name': 'Jervis'}
# results = mongo_utils2.get_user_with_query(client, queryJer)
# get_print_from_cursor("JEEERVIS QUERY:", results)

'4b. ACTUALIZARLO Y CHECKEARLO'

# mongo_utils2.actualizaip_user_check('users', '109.150.230.156/24', 'Jervis')
# results = mongo_utils2.get_user_with_query(client, queryJer)'
# get_print_from_cursor("JEEERVIS QUERY:", results)

'Apartado 5: Buscar y sacar por consola los usuarios cuya latitud '
'se mayor o igual que 30.00 y la altitud menor que 10.00'
# query = {"$and": [{"Latitude": {"$gte": 30}}, {"Altitude": {"$lte": 10}}]}
# results = mongo_utils2.get_user_with_query(client, query)
# print("En total hay "+ str(mongo_utils2.cuentaquery()) + " registros.")
# get_print_from_cursor("RESULTADOS:", results)

'Apartado 6: Crear un metodo que borre todos los elementos de la collecion users.'
' Tip: db.get_collection(collection).delete_many(query)'
# mongo_utils2.borraCollection()

'Apartado 7: Realizar un map reduce q de la siguiente salida'
# {'_id': 'Female', 'count': 19.0}
# {'_id': 'Male', 'count': 14.0}
#mongo_utils2.mapreduce()