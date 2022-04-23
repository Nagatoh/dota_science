import requests
import json
from pymongo import MongoClient
import dotenv
import os
import time
import datetime
import argparse


# pesquisar kwargs dps
# Coloca Thread dps
def get_matches_batch(min_match_id=None):
    """ Captura uma lista de partidas pro players.
    Caso seja passado um id de partida, captura apenas as partidas que sejam menores que o id passado.

    Args:
        min_match_id ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    url = 'https://api.opendota.com/api/proMatches'

    if min_match_id is not None:
        url += f'?less_than_match_id={min_match_id}'

    data = requests.get(url).json()

    return data


def save_matches(data, db_collection):
    """ Salva as listas de partidas no MongoDB.

    Args:
        matches ([type]): [description]
    """
    db_collection.insert_many(data)
    return True


def get_and_save(min_match_id=None, max_match_id=None, db_collection=None):
    """ Captura e salva as partidas."""
    data_raw = get_matches_batch(min_match_id=min_match_id)
    data = [i for i in data_raw if "match_id" in i] # verificar se possue match_id"]


    if len(data) == 0:
        print("Limite excedido de requests!")
        return False, data
    
    if max_match_id is not None:
        data = [i for i in data if i["match_id"] > max_match_id]
        if len(data) == 0:
            print("Todas as novas partidas já foram capturadas")
            return False, data

    save_matches(data, db_collection)
    min_match_id = min([i["match_id"] for i in data])
    print(len(data), "--", "--",
          datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(1)
    return True, data


def get_oldest_matches(db_collection):
    """ Retorna a partida mais antiga.

    Args:
        db_collection ([type]): [description]

    Returns:
        [type]: [description]
    """
    min_match_id = db_collection.find_one(sort=[("match_id", 1)])["match_id"]
    count = 1
    while True:
        check, _ = get_and_save(min_match_id = min_match_id,db_collection= db_collection)
        if not check:
            break
        count += 1


def get_newest_matches(db_collection):
    """ Retorna a partida mais nova.

    Args:
        db_collection ([type]): [description]

    Returns:
        [type]: [description]
    """
    try:
        max_match_id = db_collection.find_one(sort=[("match_id", -1)])["match_id"]
    except TypeError:
        max_match_id = 0

    _,data=get_and_save(max_match_id=max_match_id, db_collection=db_collection)

    try:
        min_match_id = min([i["match_id"] for i in data])
    except ValueError:
        return
    
    count = 0
    while min_match_id > max_match_id:
        check = get_and_save(min_match_id=min_match_id, db_collection=db_collection)
        if not check:
            break
        count += 1



def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--how', choices=['oldest', 'newest'])
    args = parser.parse_args()

    # Carrega o dotenv para ler as variáveis de ambiente
    dotenv.load_dotenv(dotenv.find_dotenv())
    MONGODB_IP = os.getenv("MONGODB_IP")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT"))

    mongodb_client = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database = mongodb_client["dota_raw"]
    # Preenchendo dados pela primeira vez
    # data=get_matches_batch()
    # save_matches(data,mongodb_database["pro_match_history"])
    if args.how == "oldest":
        get_oldest_matches(mongodb_database["pro_match_history"])
    if args.how == "newest":
        get_newest_matches(mongodb_database["pro_match_history"])



if __name__ == "__main__":
    main()
