# import requests
# import json



# def retrieve_uniswap_information():
#     query = """
#          query {
#               pools (orderBy: totalValueLockedETH, 
#                 orderDirection: desc,
#                 first:500) 
#                 {
#                     id
#                     totalValueLockedETH
#                     token0Price
#                     token1Price
#                     feeTier
#                     token0 {id symbol name decimals}
#                     token1 {id symbol name decimals}
#                 }
#         }
#     """

#     url = "https://gateway.thegraph.com/api/925607239f08322514396d6381dbe8f5/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
#     headers = {
#         "Content-Type": "application/json"
#     }
#     req = requests.post(url, json={'query': query}, headers=headers)
#     json_dict = json.loads(req.text)
#     return json_dict

# print(retrieve_uniswap_information())


# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3
import os
import requests
import json
import time
import logging
from datetime import datetime
import func_triangular_arb
from colorama import Fore, Style, init


# Инициализация colorama для поддержки цвета в консоли на Windows
init(autoreset=True)

# Устанавливаем формат даты для логов
log_filename = datetime.now().strftime("%Y-%m-%d_triangular_arbitrage.log")

# Настройка логирования
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]",  # Формат логов с именем функции и номером строки
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Добавляем консольный логгер с цветным выводом
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]", "%Y-%m-%d %H:%M:%S")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


""" RETRIEVE GRAPH QL MID PRICES FOR UNISWAP"""
def retrieve_uniswap_information():

    """
    Retrieve Uniswap pool information using GraphQL.

    This function sends a GraphQL query to The Graph API to retrieve a list of Uniswap pools sorted by total value locked in ETH in descending order. The query includes the following fields:
        - id: The pool's ID.
        - totalValueLockedETH: The total value of tokens locked in the pool in ETH.
        - token0Price: The price of the first token in the pool in ETH.
        - token1Price: The price of the second token in the pool in ETH.
        - feeTier: The fee tier of the pool.
        - token0: The first token in the pool.
            - id: The token's ID.
            - symbol: The token's symbol.
            - name: The token's name.
            - decimals: The token's number of decimals.
        - token1: The second token in the pool.
            - id: The token's ID.
            - symbol: The token's symbol.
            - name: The token's name.
            - decimals: The token's number of decimals.

    The function returns a JSON dictionary containing the query results.

    :return: A JSON dictionary containing the query results.
    :rtype: dict
    """
    query = """
         {
              pools (orderBy: totalValueLockedETH, 
                orderDirection: desc,
                first:1000) 
                {
                    id
                    totalValueLockedETH
                    token0Price
                    token1Price
                    feeTier
                    token0 {id symbol name decimals}
                    token1 {id symbol name decimals}
                }
        }
    """
    API_KEY_UNISWAP = os.getenv('API_kEY_UNISWAP')
    url = "https://gateway.thegraph.com/api/925607239f08322514396d6381dbe8f5/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
    headers = {
        "Content-Type": "application/json"
    }
    req = requests.post(url, json={'query': query}, headers=headers)
    json_dict = json.loads(req.text)
    return json_dict

if __name__ == "__main__":

    while True:

        pairs = retrieve_uniswap_information()["data"]["pools"]
        structured_pairs = func_triangular_arb.structure_trading_pairs(pairs, limit=500)

        # Get surface rates
        surface_rates_list = []
        for t_pair in structured_pairs:
            surface_rate = func_triangular_arb.calc_triangular_arb_surface_rate(t_pair, min_rate=1)
            if len(surface_rate) > 0:
                surface_rates_list.append(surface_rate)

        # Save to JSON file
        if len(surface_rates_list) > 0:
            logging.info(f"{Fore.LIGHTYELLOW_EX}Загружено {len(surface_rates_list)} треугольников.")
            with open("uniswap_surface_rates.json", "w") as fp:
                json.dump(surface_rates_list, fp)
                print("File saved.")

        time.sleep(60)
