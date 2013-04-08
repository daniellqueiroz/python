#-------------------------------------------------------------------------------
# Name:        Exemplo operações MongoDB/Python
# Purpose:     Aprendizado
# Author:      Daniel Queiroz
# Created:     08/04/2013
#------------------------------------------------------------------------------

# http://snmaynard.com/2012/10/17/things-i-wish-i-knew-about-mongodb-a-year-ago/

import sys
import random
from pymongo import Connection


def generate_random_name():
    vName = ''.join([random.choice('abcdefghijklmnoprstuvwyxzABCDEFGHIJKLMNOPRSTUVWXYZ') for i in range(8)])
    return vName


def generate_random_number():
    vNumber = ''.join([random.choice('0123456789') for i in range(4)])
    return vNumber


def make_connection():
    try:
        vConnection = Connection("127.0.0.1", port=27017)
        return vConnection["database_teste"]
    except e:
        print("Falha ao estabelecer uma conexão ao banco de dados")
        sys.exit(1)


def insert_item(pConnection):
    i = 0
    for i in range(10):
        vCollection = {
            "produto" : generate_random_name(),
            "nome_fantasia" : generate_random_name(),
            "preço": float(generate_random_number()),
            "custo" : float(generate_random_number())
        }

        pConnection.produtos.insert(vCollection)


def list_itens(pConnection):
    vCollection = pConnection["produtos"]
    vDocuments = vCollection.find({})

    if(vDocuments.count() <= 0):
        print("Lista Vazia")
    else:
        for i in vDocuments:
            print (i)


def delete_itens(pConnection):
    vCollection = pConnection["produtos"]
    vDocuments = vCollection.find()
    for i in vDocuments:
        pConnection["produtos"].remove(i)


def delete_item_by_name(pConnection, pName):
    vCollection = pConnection["produtos"]
    vDocuments = vCollection.find({"produto" : pName})
    for i in vDocuments:
        print (i)


def create_table_index(pConnection):
    from pymongo import ASCENDING
    pConnection.produtos.create_index([("produto", ASCENDING)])


def create_map_function(pConnection):


def create_reduce_function(pConnection):


def main():

   vConnection = make_connection()
   insert_item(vConnection)
   create_table_index(vConnection)
   list_itens(vConnection)

   print("removendo produtos...")
   delete_itens(vConnection)
   list_itens(vConnection)

if __name__ == '__main__':
    main()
