"""
    Challenge: integrating Python with SQLite and MongoDB:
    This code is a part of the challenge proposed for DIO, the part of NoSQL, using MongoDB,
    the idea is to create a schema using databases of both types: SQL and NoSQL.
"""

import pprint
import pymongo as py_mongo

""" 
    Connecting with the Mongo client
    IMPORTANT! if you will do download of this file,
    change the variables: <user> and <password>, them are your user and password on the mongo atlas
"""
client = py_mongo.MongoClient("mongodb+srv://<user>:<password>@mongodio.udp172n.mongodb.net/"
                              "?retryWrites=true&w=majority&appName=AtlasApp")

# Creating the database bank
db = client.bank
collection = db.bank_collection
print(db.bank_collection)

# Informing the first information at the doc
bank = {
    "client_name": "Mike",
    "cpf": "12345678911",
    "address": "street of happiness, number 126",
    "account_type": "checking account",
    "account_agency": "123",
    "account_number": "12345687",
    "account_balance": "300"
}

# "Writing" the information in the doc
client_accounts = db.posts
client_ID = client_accounts.insert_one(bank).inserted_id

pprint.pprint(db.posts.find_one())

# Using the bulk inserts
new_account = [{
    "client_name": "Victor",
    "cpf": "321456989",
    "address": "street of sadness, number 132",
    "account_type": "saving account",
    "account_agency": "321",
    "account_number": "1230009877",
    "account_balance": "160"},
    {
    "client_name": "Hugo",
    "cpf": "99988877734",
    "address": "street of peace, number 22",
    "account_type": "saving account",
    "account_agency": "676",
    "account_number": "12303846",
    "account_balance": "350"}]

# Recovering the new accounts
result = client_accounts.insert_many(new_account)
print(result.inserted_ids)

# Recovering the information using filter, on this case I'm searching for the client called "Mike"
print("\nFinal recover")
pprint.pprint(db.posts.find_one({"client_name": "Mike"}))

# Recovering all doc's on the collection
print("\nDocuments present in the posts collection")
for post in client_accounts.find():
    pprint.pprint(post)
