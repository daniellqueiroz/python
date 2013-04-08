#-------------------------------------------------------------------------------
# Name:        Exemplo operações MongoDB/Python
# Purpose:     Aprendizado
#
# Author:      Daniel Queiroz
#
# Created:     08/04/2013
# Copyright:   (c) dqueiroz 2013
# Licence:     <your licence>
#------------------------------------------------------------------------------

import sys
import random
import datetime
from pymongo import Connection


def generate_random_name():
    vName = ''.join([random.choice('abcdefghijklmnoprstuvwyxzABCDEFGHIJKLMNOPRSTUVWXYZ0123456789') for i in range(8)])
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
            "username" : generate_random_name(),
            "firstname" : generate_random_name(),
            "surname": generate_random_name(),
            "custo" : generate_random_number()
        }

        pConnection.users.insert(vCollection)
        print("Coleção adicionada com sucesso!")


def list_itens(pConnection):
    vCollection = pConnection["users"]
    vDocuments = vCollection.find()
    for i in vDocuments:
        print (i)


#def delete_itens(_connection):

def main():
   vConnection = make_connection()
   insert_item(vConnection)
   list_itens(vConnection)


if __name__ == '__main__':
    main()
