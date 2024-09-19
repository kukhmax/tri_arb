import ccxt.async_support as ccxt  # Асинхронный CCXT
import os
from dotenv import load_dotenv
import json
import asyncio
import aiohttp
import logging
from aiolimiter import AsyncLimiter
import colorlog

# Настройка логирования с цветом
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(message)s'))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

load_dotenv()

API_KEY = os.getenv('API_kEY_BYBIT')
API_SECRET = os.getenv('API_SECRET_BYBIT')


STARTING_AMOUNT = {
    "USDT": 100, 
    "USDC": 100, 
    "BTC": 0.0001, 
    "ETH": 0.01, 
    "UNI": 16, 
    "BETH": 0.045
}

# Лимит на количество запросов (1200 запросов в минуту = 20 запросов в секунду)
rate_limiter = AsyncLimiter(20, 1)

# Семафор для ограничения одновременной торговли только одной связкой
# trade_semaphore = asyncio.Semaphore(1)

is_trading = False  # Глобальный флаг, указывающий, идет ли сейчас торговля
trading_lock = asyncio.Lock()  # Блокировка для управления доступом к флагу


async def fetch_with_rate_limit(client, method, *args, **kwargs):
    async with rate_limiter:
        try:
            return await getattr(client, method)(*args, **kwargs)
        except Exception as e:
            logger.error(f"Ошибка при выполнении {method}: {str(e)}")
            return None

async def get_trianbular_pairs(client):
    markets = await fetch_with_rate_limit(client, 'fetch_markets')
    markets = [x for x in markets if ':' not in x['symbol']]
    triangular_pairs_list = []
    remove_duplicates_list = []

    for pair_a in markets:
        a_base, a_quote = pair_a['base'], pair_a['quote']
        a_pair_box = [a_base, a_quote]

        for pair_b in markets:
            b_base, b_quote = pair_b['base'], pair_b['quote']
            if pair_b['symbol'] != pair_a['symbol']:
                if b_base in a_pair_box or b_quote in a_pair_box:
                    for pair_c in markets:
                        c_base, c_quote = pair_c['base'], pair_c['quote']

                        if pair_c['symbol'] != pair_a['symbol'] and pair_c['symbol'] != pair_b['symbol']:
                            combine_all = [pair_a['symbol'], pair_b['symbol'], pair_c['symbol']]
                            pair_box = [a_base, a_quote, b_base, b_quote, c_base, c_quote]

                            counts_c_base = pair_box.count(c_base)
                            counts_c_quote = pair_box.count(c_quote)

                            if counts_c_base == 2 and counts_c_quote == 2 and c_base != c_quote:
                                combined = ",".join(combine_all)
                                unique_item = ''.join(sorted(combine_all))

                                if unique_item not in remove_duplicates_list:
                                    match_dict = {
                                        "a_base": a_base,
                                        "b_base": b_base,
                                        "c_base": c_base,
                                        "a_quote": a_quote,
                                        "b_quote": b_quote,
                                        "c_quote": c_quote,
                                        "pair_a": pair_a['symbol'],
                                        "pair_b": pair_b['symbol'],
                                        "pair_c": pair_c['symbol'],
                                        "combined": combined
                                    }
                                    triangular_pairs_list.append(match_dict)
                                    logger.info(f"triangular pairs is : \n{match_dict}")
                                    remove_duplicates_list.append(unique_item)

    with open('markets.json', 'w') as f:
        structured_pairs = [x for x in triangular_pairs_list if 'EUR' not in x['combined']]
        json.dump(structured_pairs, f)

async def get_price_for_t_pair(client, t_pair):
    ticker_a = await fetch_with_rate_limit(client, 'fetch_ticker', t_pair['pair_a'])
    await asyncio.sleep(0.2)
    ticker_b = await fetch_with_rate_limit(client, 'fetch_ticker', t_pair['pair_b'])
    await asyncio.sleep(0.2)
    ticker_c = await fetch_with_rate_limit(client, 'fetch_ticker', t_pair['pair_c'])
    
    # Создаем словарь ask_bid вручную
    ask_bid = {f"pair_a_ask": ticker_a['ask'], f"pair_a_bid": ticker_a['bid'], 
               f"pair_b_ask": ticker_b['ask'], f"pair_b_bid": ticker_b['bid'], 
               f"pair_c_ask": ticker_c['ask'], f"pair_c_bid": ticker_c['bid']}
    return ask_bid

async def get_depth_from_orderbook(client, surface_arb, taker_fee):
    swap_1 = surface_arb["swap_1"]
    starting_amount = STARTING_AMOUNT.get(swap_1, 100)

    tasks = []
    contracts = [(surface_arb[f"contract_{i}"], surface_arb[f"direction_trade_{i}"]) for i in range(1, 4)]
    for contract, direction in contracts:
        # Здесь добавляем именно корутину в список tasks, а не словарь
        tasks.append(fetch_with_rate_limit(client, 'fetch_order_book', contract, limit=20))

    depths = await asyncio.gather(*tasks)

    reformatted_depths = [reformated_orderbook(depth, direction) for depth, (_, direction) in zip(depths, contracts)]

    acquired_coin_t1 = calculate_acquired_coin(starting_amount, reformatted_depths[0], taker_fee)
    acquired_coin_t2 = calculate_acquired_coin(acquired_coin_t1, reformatted_depths[1], taker_fee)
    acquired_coin_t3 = calculate_acquired_coin(acquired_coin_t2, reformatted_depths[2], taker_fee)

    profit_loss = acquired_coin_t3 - starting_amount
    real_rate_perc = (profit_loss / starting_amount) * 100 if profit_loss != 0 else 0

    if real_rate_perc > 0:
        return {
            "profit_loss": profit_loss,
            "real_rate_perc": real_rate_perc,
            **surface_arb
        }

    return {}

def calculate_acquired_coin(amount_in, orderbook, taker_fee=0.001):
    # Initialise Variables
    trading_balance = amount_in
    quantity_bought = 0
    acquired_coin = 0
    counts = 0
    for level in orderbook:

        # Extract the level price and quantity
        level_price = level[0]
        level_available_quantity = level[1]

        if trading_balance <= level_available_quantity:
            quantity_bought = trading_balance
            trading_balance = 0
            amount_bought = quantity_bought * level_price
        else:
            quantity_bought = level_available_quantity
            trading_balance -= quantity_bought
            amount_bought = quantity_bought * level_price

        # Accumulate Acquired Coin
        acquired_coin += amount_bought * (1 - taker_fee)

        # Exit Trade
        if trading_balance == 0:
            return acquired_coin

        # Exit if not enough order book levels
        counts += 1
        if counts == len(orderbook):
            return 0


# Reformat Order Book for Depth Calculation
def reformated_orderbook(prices, c_direction):
    price_list_main = []
    if c_direction == "base_to_quote":
        for p in prices["asks"]:
            ask_price = p[0]
            adj_price = 1 / ask_price if ask_price != 0 else 0
            adj_quantity = p[1] * ask_price
            price_list_main.append([adj_price, adj_quantity])
    if c_direction == "quote_to_base":
        for p in prices["bids"]:
            bid_price = p[0]
            adj_price = bid_price if bid_price != 0 else 0
            adj_quantity = p[1]
            price_list_main.append([adj_price, adj_quantity])
    return price_list_main

# Calculate Surface Rate Arbitrage Opportunity
def calc_triangular_arb_surface_rate(t_pair, prices_dict):

    # Set Variables
    starting_amount = 1
    min_surface_rate = 0
    surface_dict = {}
    contract_2 = ""
    contract_3 = ""
    direction_trade_1 = ""
    direction_trade_2 = ""
    direction_trade_3 = ""
    acquired_coin_t2 = 0
    acquired_coin_t3 = 0
    calculated = 0

    # Extract Pair Variables
    a_base = t_pair["a_base"]
    a_quote = t_pair["a_quote"]
    b_base = t_pair["b_base"]
    b_quote = t_pair["b_quote"]
    c_base = t_pair["c_base"]
    c_quote = t_pair["c_quote"]
    pair_a = t_pair["pair_a"]
    pair_b = t_pair["pair_b"]
    pair_c = t_pair["pair_c"]

    # Extract Price Information
    a_ask = prices_dict["pair_a_ask"]
    a_bid = prices_dict["pair_a_bid"]
    b_ask = prices_dict["pair_b_ask"]
    b_bid = prices_dict["pair_b_bid"]
    c_ask = prices_dict["pair_c_ask"]
    c_bid = prices_dict["pair_c_bid"]

    # Set directions and loop through
    direction_list = ["forward", "reverse"]
    for direction in direction_list:
    
        # Set additional variables for swap information
        swap_1 = 0
        swap_2 = 0
        swap_3 = 0
        swap_1_rate = 0
        swap_2_rate = 0
        swap_3_rate = 0
    
        """
            Poloniex Rules !!
            If we are swapping the coin on the left (Base) to the right (Quote) then * (1 / Ask)
            If we are swapping the coin on the right (Quote) to the left (Base) then * Bid
        """
    
        # Assume starting with a_base and swapping for a_quote
        if direction == "forward":
            swap_1 = a_base
            swap_2 = a_quote
            swap_1_rate = 1 / a_ask
            direction_trade_1 = "base_to_quote"
    
        # Assume starting with a_base and swapping for a_quote
        if direction == "reverse":
            swap_1 = a_quote
            swap_2 = a_base
            swap_1_rate = a_bid
            direction_trade_1 = "quote_to_base"
    
        # Place first trade
        contract_1 = pair_a
        acquired_coin_t1 = starting_amount * swap_1_rate
      

        """  FORWARD """
        # SCENARIO 1 Check if a_quote (acquired_coin) matches b_quote
        if direction == "forward":
            if a_quote == b_quote and calculated == 0:
                swap_2_rate = b_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_b

                # If b_base (acquired coin) matches c_base
                if b_base == c_base:
                    swap_3 = c_base
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                # If b_base (acquired coin) matches c_quote
                if b_base == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = c_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 2 Check if a_quote (acquired_coin) matches b_base
        if direction == "forward":
            if a_quote == b_base and calculated == 0:
                swap_2_rate = 1 / b_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_b

                # If b_quote (acquired coin) matches c_base
                if b_quote == c_base:
                    swap_3 = c_base
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                # If b_quote (acquired coin) matches c_quote
                if b_quote == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = c_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 3 Check if a_quote (acquired_coin) matches c_quote
        if direction == "forward":
            if a_quote == c_quote and calculated == 0:
                swap_2_rate = c_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_c

                # If c_base (acquired coin) matches b_base
                if c_base == b_base:
                    swap_3 = b_base
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                # If c_base (acquired coin) matches b_quote
                if c_base == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = b_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 4 Check if a_quote (acquired_coin) matches c_base
        if direction == "forward":
            if a_quote == c_base and calculated == 0:
                swap_2_rate = 1 / c_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_c

                # If c_quote (acquired coin) matches b_base
                if c_quote == b_base:
                    swap_3 = b_base
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                # If c_quote (acquired coin) matches b_quote
                if c_quote == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = b_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        """  REVERSE """
        # SCENARIO 1 Check if a_base (acquired_coin) matches b_quote
        if direction == "reverse":
            if a_base == b_quote and calculated == 0:
                swap_2_rate = b_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_b

                # If b_base (acquired coin) matches c_base
                if b_base == c_base:
                    swap_3 = c_base
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                # If b_base (acquired coin) matches c_quote
                if b_base == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = c_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 2 Check if a_base (acquired_coin) matches b_base
        if direction == "reverse":
            if a_base == b_base and calculated == 0:
                swap_2_rate = 1 / b_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_b

                # If b_quote (acquired coin) matches c_base
                if b_quote == c_base:
                    swap_3 = c_base
                    swap_3_rate = 1 / c_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_c

                # If b_quote (acquired coin) matches c_quote
                if b_quote == c_quote:
                    swap_3 = c_quote
                    swap_3_rate = c_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_c

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 3 Check if a_base (acquired_coin) matches c_quote
        if direction == "reverse":
            if a_base == c_quote and calculated == 0:
                swap_2_rate = c_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "quote_to_base"
                contract_2 = pair_c

                # If c_base (acquired coin) matches b_base
                if c_base == b_base:
                    swap_3 = b_base
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                # If c_base (acquired coin) matches b_quote
                if c_base == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = b_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1

        # SCENARIO 4 Check if a_base (acquired_coin) matches c_base
        if direction == "reverse":
            if a_base == c_base and calculated == 0:
                swap_2_rate = 1 / c_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2 = "base_to_quote"
                contract_2 = pair_c

                # If c_quote (acquired coin) matches b_base
                if c_quote == b_base:
                    swap_3 = b_base
                    swap_3_rate = 1 / b_ask
                    direction_trade_3 = "base_to_quote"
                    contract_3 = pair_b

                # If c_quote (acquired coin) matches b_quote
                if c_quote == b_quote:
                    swap_3 = b_quote
                    swap_3_rate = b_bid
                    direction_trade_3 = "quote_to_base"
                    contract_3 = pair_b

                acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                calculated = 1
                
        """ PROFIT LOSS OUTPUT """

        # Profit and Loss Calculations
        profit_loss = acquired_coin_t3 - starting_amount
        profit_loss_perc = (profit_loss / starting_amount) * 100 if profit_loss != 0 else 0

        # Trade Descriptions
        trade_description_1 = f"Start with {swap_1} of {starting_amount}. Swap at {swap_1_rate} for {swap_2} acquiring {acquired_coin_t1}."
        trade_description_2 = f"Swap {acquired_coin_t1} of {swap_2} at {swap_2_rate} for {swap_3} acquiring {acquired_coin_t2}."
        trade_description_3 = f"Swap {acquired_coin_t2} of {swap_3} at {swap_3_rate} for {swap_1} acquiring {acquired_coin_t3}."

        # Output Results
        if profit_loss_perc > min_surface_rate:
            surface_dict = {
                "swap_1": swap_1,
                "swap_2": swap_2,
                "swap_3": swap_3,
                "contract_1": contract_1,
                "contract_2": contract_2,
                "contract_3": contract_3,
                "direction_trade_1": direction_trade_1,
                "direction_trade_2": direction_trade_2,
                "direction_trade_3": direction_trade_3,
                "starting_amount": starting_amount,
                "acquired_coin_t1": acquired_coin_t1,
                "acquired_coin_t2": acquired_coin_t2,
                "acquired_coin_t3": acquired_coin_t3,
                "swap_1_rate": swap_1_rate,
                "swap_2_rate": swap_2_rate,
                "swap_3_rate": swap_3_rate,
                "profit_loss": profit_loss,
                "profit_loss_perc": profit_loss_perc,
                "direction": direction,
                "trade_description_1": trade_description_1,
                "trade_description_2": trade_description_2,
                "trade_description_3": trade_description_3
            }

            return surface_dict

    return surface_dict

async def check_balance(client, symbol):
    """
    Проверяет баланс по символу.
    Возвращает количество доступной монеты.
    """
    try:
        balances = await fetch_with_rate_limit(client, 'fetch_balance')
        if balances is None:
            logger.error("Не удалось получить баланс. Убедитесь, что API ключи корректны.")
            return 0
        balance = balances.get(symbol, {}).get('free', 0)
        return balance
    except Exception as e:
        logger.error(f"Ошибка при выполнении fetch_balance: {str(e)}")
        return 0

async def open_market_orders(client, surface_arb):
    logger.info("start open_market_orders")
    """
    Открывает рыночные ордера на профитной связке и выводит P&L.
    Проверяет наличие достаточного объема перед каждым ордером.
    """
    swap_1 = surface_arb["swap_1"]  # начальная монета
    starting_amount = STARTING_AMOUNT.get(swap_1, 100) # объем стартовой монеты (можно заменить динамическим)

    logger.info(f"Начинаем свопы с {starting_amount} {swap_1}")

    # Проверка баланса перед первым ордером
    balance_1 = await check_balance(client, swap_1)
    logger.info(f"Начальный баланс {swap_1}: {balance_1}")
    if balance_1 < starting_amount:
        logger.error(f"Недостаточно {swap_1}. Доступно: {balance_1}, необходимо: {starting_amount}")
        return

    # Открытие первого ордера
    contract_1 = surface_arb["contract_1"]
    direction_1 = surface_arb["direction_trade_1"]
    amount_1 = starting_amount if direction_1 == "base_to_quote" else starting_amount / surface_arb["swap_1_rate"]

    logger.info(f"Открываем первый ордер: {contract_1} на {amount_1}")
    order_1 = await client.create_order(contract_1, 'market', 'buy' if direction_1 == "base_to_quote" else 'sell', amount_1)

    # Ожидаем завершения первого ордера
    await asyncio.sleep(0.3)

    # Проверка баланса перед вторым ордером
    swap_2 = surface_arb["swap_2"]
    balance_2 = await check_balance(client, swap_2)
    if balance_2 < order_1['filled']:
        logger.error(f"Недостаточно {swap_2}. Доступно: {balance_2}, необходимо: {order_1['filled']}")
        return

    # Открытие второго ордера
    contract_2 = surface_arb["contract_2"]
    direction_2 = surface_arb["direction_trade_2"]
    amount_2 = order_1['filled'] if direction_2 == "base_to_quote" else order_1['filled'] / surface_arb["swap_2_rate"]

    logger.info(f"Открываем второй ордер: {contract_2} на {amount_2}")
    order_2 = await client.create_order(contract_2, 'market', 'buy' if direction_2 == "base_to_quote" else 'sell', amount_2)

    # Ожидаем завершения второго ордера
    await asyncio.sleep(0.3)

    # Проверка баланса перед третьим ордером
    swap_3 = surface_arb["swap_3"]
    balance_3 = await check_balance(client, swap_3)
    if balance_3 < order_2['filled']:
        logger.error(f"Недостаточно {swap_3}. Доступно: {balance_3}, необходимо: {order_2['filled']}")
        return

    # Открытие третьего ордера
    contract_3 = surface_arb["contract_3"]
    direction_3 = surface_arb["direction_trade_3"]
    amount_3 = order_2['filled'] if direction_3 == "base_to_quote" else order_2['filled'] / surface_arb["swap_3_rate"]

    logger.info(f"Открываем третий ордер: {contract_3} на {amount_3}")
    order_3 = await client.create_order(contract_3, 'market', 'buy' if direction_3 == "base_to_quote" else 'sell', amount_3)

    # Ожидаем завершения третьего ордера
    await asyncio.sleep(0.3)

    # Рассчитываем P&L
    final_amount = order_3['filled']
    profit_loss = final_amount - starting_amount
    profit_loss_perc = (profit_loss / starting_amount) * 100 if profit_loss != 0 else 0
    
    logger.info(f"P&L: {profit_loss:.2f} {swap_1}, Процент прибыли: {profit_loss_perc:.2f}%")
    logger.info(f"Финальный баланс: {final_amount:.2f} {swap_1}")

    return {
        "P&L": profit_loss,
        "final_balance": final_amount,
        "profit_loss_percentage": profit_loss_perc
    }


async def main():
    """
    Main function to run triangular arbitrage.

    This function will:

    1. Load structured pairs from 'markets.json'
    2. Process each pair in the list with `process_pair`
    3. Wait for the results of all pairs to finish
    4. Repeat the above steps every second

    If any error occurs, it will be logged with the logger.

    Finally, it will close the ccxt client.
    """
    client = ccxt.bybit({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'enableRateLimit': True,  # Включение ограничения по частоте запросов
        'options': {
            'adjustForTimeDifference': True,  # Включение синхронизации времени
    },
    })

    # await get_trianbular_pairs(client)

    try:
        with open("markets.json") as json_file:
            structured_pairs = json.load(json_file)

        while True:
            tasks = []
            for t_pair in structured_pairs:
                tasks.append(asyncio.create_task(process_pair(client, t_pair)))
                await asyncio.sleep(0.2)

            await asyncio.gather(*tasks)
            await asyncio.sleep(0.3)
    except Exception as e:
        logger.error(f"Error main function: {str(e)}")
    finally:
        await client.close()  # Ensure the client is closed

async def process_pair(client, t_pair):
    """
    Process a triangular arbitrage pair.

    This function gets the prices for a triangular pair, calculates the surface rate,
    gets the depth from the orderbook, and logs any arbitrage opportunities to a file.

    Parameters
    ----------
    client : ccxt.Exchange
        The exchange client to use.
    t_pair : dict
        The triangular pair to process. This is a dictionary with the keys
        'a_base', 'a_quote', 'b_base', 'b_quote', 'c_base', 'c_quote', 'pair_a', 'pair_b', and 'pair_c'.

    Raises
    ------
    Exception
        If there is an error processing the pair.
    """
    try:
        # Объявляем глобальную переменную в начале функции, до любого её использования
        global is_trading

        prices_dict = await get_price_for_t_pair(client, t_pair)
        surface_dict = calc_triangular_arb_surface_rate(t_pair, prices_dict)
        
        if surface_dict:
            markets = await fetch_with_rate_limit(client, 'fetch_markets')
            taker_fee = markets[0]['taker']
            real_rate_arb = await get_depth_from_orderbook(client, surface_dict, taker_fee)

            if real_rate_arb:
                async with trading_lock:
                    if is_trading:
                        return  # Если уже идет торговля, не начинаем новую
                    is_trading = True  # Устанавливаем флаг торговли

                logger.info(f"Arbitrage opportunity found for {t_pair['combined']}")
                with open('trading_logs.txt', 'a') as f:
                    f.write(f"Arbitrage Opportunity: {real_rate_arb}\n")

                # Исполняем ордера
                result = await open_market_orders(client, real_rate_arb)

                with open('trading_logs.txt', 'a') as f:
                    f.write(f"Swapping result: {result}\n\n")

                # Сбрасываем флаг после завершения торговли
                async with trading_lock:
                    is_trading = False

            else:
                logger.warning(f"Нет арбитражной возможности: {t_pair['combined']}")

    except Exception as e:
        logger.error(f"Error processing pair {t_pair['combined']}: {str(e)}")

if __name__ == '__main__':
    asyncio.run(main())









# import ccxt
# import os
# from dotenv import load_dotenv
# import json
# import time
# import logging
# import colorlog

# # # Настройка логирования с цветом
# handler = colorlog.StreamHandler()
# handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(message)s'))

# logger = colorlog.getLogger()
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)

# load_dotenv()

# API_KEY = os.getenv('API_KEY_BYBIT')
# API_SECRET = os.getenv('API_SECRET_BYBIT')

# client = ccxt.binance({
#     'apiKey': API_KEY,
#     'secret': API_SECRET,
# })

# # markets = client.fetch_markets()
# # coin_list = [x['symbol'] for x in markets]


# # Step1
# def get_trianbular_pairs(client):
#     markets = client.fetch_markets()
#     taker_fee = markets[0]['taker']

#     coin_list = [x for x in markets if ':' not in x['symbol']]
    
#     triangular_pairs_list = []
#     remove_duplicates_list = []

#     for pair_a in coin_list:
#         a_base, a_quote = pair_a['base'], pair_a['quote']
#         a_pair_box = [a_base, a_quote]

#         for pair_b in coin_list:
#             b_base, b_quote = pair_b['base'], pair_b['quote']
#             if pair_b['symbol'] != pair_a['symbol']:
#                 if b_base in a_pair_box or b_quote in a_pair_box:

#                     for pair_c in coin_list:
#                         c_base, c_quote = pair_c['base'], pair_c['quote']

#                         if pair_c['symbol'] != pair_a['symbol'] and pair_c['symbol'] != pair_b['symbol']:
#                             combine_all = [pair_a['symbol'], pair_b['symbol'], pair_c['symbol']]
#                             pair_box = [a_base, a_quote, b_base, b_quote, c_base, c_quote]

#                             # print(pair_c['symbol'
#                             counts_c_base = 0
#                             for i in pair_box:
#                                 if i == c_base:
#                                     counts_c_base += 1

#                             counts_c_quote = 0
#                             for i in pair_box:
#                                 if i == c_quote:
#                                     counts_c_quote += 1

#                             # Determining Triangular Match
#                             if counts_c_base == 2 and counts_c_quote == 2 and c_base != c_quote:
#                                 combined = pair_a['symbol'] + "," + pair_b['symbol'] + "," + pair_c['symbol']
#                                 unique_item = ''.join(sorted(combine_all))

#                                 if unique_item not in remove_duplicates_list:
#                                     match_dict = {
#                                         "a_base": a_base,
#                                         "b_base": b_base,
#                                         "c_base": c_base,
#                                         "a_quote": a_quote,
#                                         "b_quote": b_quote,
#                                         "c_quote": c_quote,
#                                         "pair_a": pair_a['symbol'],
#                                         "pair_b": pair_b['symbol'],
#                                         "pair_c": pair_c['symbol'],
#                                         "combined": combined,
#                                         "fees": taker_fee
#                                     }
#                                     triangular_pairs_list.append(match_dict)
#                                     logger.info(f"triangular pairs is : \n{match_dict['combined']}")
#                                     remove_duplicates_list.append(unique_item)

#     with open('coin_list.json', 'w') as f:
#         structured_pairs = [x for x in triangular_pairs_list if ':' not in x['combined']]
#         json.dump(structured_pairs, f)


# # Step 2
# def get_price_for_t_pair(t_pair):
#     tickers = client.fetch_tickers()
#     ask_bid = {
#             "pair_a_ask": None,
#             "pair_a_bid": None,
#             "pair_b_ask": None,
#             "pair_b_bid": None,
#             "pair_c_ask": None,
#             "pair_c_bid": None
#         }
        
#     for i, j in enumerate(t_pair['combined'].split(',')):
#         num = {0: 'a', 1: 'b', 2: 'c'}
#         for k, v in tickers.items():
#             if k == j:
#                 ask_bid[f"pair_{num[i]}_ask"] = v['ask']
#                 ask_bid[f"pair_{num[i]}_bid"] = v['bid']
#     return ask_bid
                
# # Step 3      
# # Calculate Surface Rate Arbitrage Opportunity
# def calc_triangular_arb_surface_rate(t_pair, prices_dict):

#     # Set Variables
#     starting_amount = 1
#     min_surface_rate = 0
#     surface_dict = {}
#     contract_2 = ""
#     contract_3 = ""
#     direction_trade_1 = ""
#     direction_trade_2 = ""
#     direction_trade_3 = ""
#     acquired_coin_t2 = 0
#     acquired_coin_t3 = 0
#     calculated = 0
#     fees_rate = 1 - t_pair["fees"]

#     # Extract Pair Variables
#     a_base = t_pair["a_base"]
#     a_quote = t_pair["a_quote"]
#     b_base = t_pair["b_base"]
#     b_quote = t_pair["b_quote"]
#     c_base = t_pair["c_base"]
#     c_quote = t_pair["c_quote"]
#     pair_a = t_pair["pair_a"]
#     pair_b = t_pair["pair_b"]
#     pair_c = t_pair["pair_c"]

#     # Extract Price Information
#     a_ask = prices_dict["pair_a_ask"]
#     a_bid = prices_dict["pair_a_bid"]
#     b_ask = prices_dict["pair_b_ask"]
#     b_bid = prices_dict["pair_b_bid"]
#     c_ask = prices_dict["pair_c_ask"]
#     c_bid = prices_dict["pair_c_bid"]

#     # Set directions and loop through
#     direction_list = ["forward", "reverse"]
#     for direction in direction_list:
    
#         # Set additional variables for swap information
#         swap_1 = 0
#         swap_2 = 0
#         swap_3 = 0
#         swap_1_rate = 0
#         swap_2_rate = 0
#         swap_3_rate = 0
    
#         """
#             Poloniex Rules !!
#             If we are swapping the coin on the left (Base) to the right (Quote) then * (1 / Ask)
#             If we are swapping the coin on the right (Quote) to the left (Base) then * Bid
#         """
    
#         # Assume starting with a_base and swapping for a_quote
#         if direction == "forward":
#             swap_1 = a_base
#             swap_2 = a_quote
#             swap_1_rate = 1 / a_ask
#             direction_trade_1 = "base_to_quote"
    
#         # Assume starting with a_base and swapping for a_quote
#         if direction == "reverse":
#             swap_1 = a_quote
#             swap_2 = a_base
#             swap_1_rate = a_bid
#             direction_trade_1 = "quote_to_base"
    
#         # Place first trade
#         contract_1 = pair_a
#         acquired_coin_t1 = starting_amount * swap_1_rate
      

#         """  FORWARD """
#         # SCENARIO 1 Check if a_quote (acquired_coin) matches b_quote
#         if direction == "forward":
#             if a_quote == b_quote and calculated == 0:
#                 swap_2_rate = b_bid
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "quote_to_base"
#                 contract_2 = pair_b

#                 # If b_base (acquired coin) matches c_base
#                 if b_base == c_base:
#                     swap_3 = c_base
#                     swap_3_rate = 1 / c_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_c

#                 # If b_base (acquired coin) matches c_quote
#                 if b_base == c_quote:
#                     swap_3 = c_quote
#                     swap_3_rate = c_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_c

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         # SCENARIO 2 Check if a_quote (acquired_coin) matches b_base
#         if direction == "forward":
#             if a_quote == b_base and calculated == 0:
#                 swap_2_rate = 1 / b_ask
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "base_to_quote"
#                 contract_2 = pair_b

#                 # If b_quote (acquired coin) matches c_base
#                 if b_quote == c_base:
#                     swap_3 = c_base
#                     swap_3_rate = 1 / c_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_c

#                 # If b_quote (acquired coin) matches c_quote
#                 if b_quote == c_quote:
#                     swap_3 = c_quote
#                     swap_3_rate = c_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_c

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         # SCENARIO 3 Check if a_quote (acquired_coin) matches c_quote
#         if direction == "forward":
#             if a_quote == c_quote and calculated == 0:
#                 swap_2_rate = c_bid
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "quote_to_base"
#                 contract_2 = pair_c

#                 # If c_base (acquired coin) matches b_base
#                 if c_base == b_base:
#                     swap_3 = b_base
#                     swap_3_rate = 1 / b_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_b

#                 # If c_base (acquired coin) matches b_quote
#                 if c_base == b_quote:
#                     swap_3 = b_quote
#                     swap_3_rate = b_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_b

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         # SCENARIO 4 Check if a_quote (acquired_coin) matches c_base
#         if direction == "forward":
#             if a_quote == c_base and calculated == 0:
#                 swap_2_rate = 1 / c_ask
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "base_to_quote"
#                 contract_2 = pair_c

#                 # If c_quote (acquired coin) matches b_base
#                 if c_quote == b_base:
#                     swap_3 = b_base
#                     swap_3_rate = 1 / b_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_b

#                 # If c_quote (acquired coin) matches b_quote
#                 if c_quote == b_quote:
#                     swap_3 = b_quote
#                     swap_3_rate = b_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_b

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         """  REVERSE """
#         # SCENARIO 1 Check if a_base (acquired_coin) matches b_quote
#         if direction == "reverse":
#             if a_base == b_quote and calculated == 0:
#                 swap_2_rate = b_bid
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "quote_to_base"
#                 contract_2 = pair_b

#                 # If b_base (acquired coin) matches c_base
#                 if b_base == c_base:
#                     swap_3 = c_base
#                     swap_3_rate = 1 / c_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_c

#                 # If b_base (acquired coin) matches c_quote
#                 if b_base == c_quote:
#                     swap_3 = c_quote
#                     swap_3_rate = c_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_c

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         # SCENARIO 2 Check if a_base (acquired_coin) matches b_base
#         if direction == "reverse":
#             if a_base == b_base and calculated == 0:
#                 swap_2_rate = 1 / b_ask
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "base_to_quote"
#                 contract_2 = pair_b

#                 # If b_quote (acquired coin) matches c_base
#                 if b_quote == c_base:
#                     swap_3 = c_base
#                     swap_3_rate = 1 / c_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_c

#                 # If b_quote (acquired coin) matches c_quote
#                 if b_quote == c_quote:
#                     swap_3 = c_quote
#                     swap_3_rate = c_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_c

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         # SCENARIO 3 Check if a_base (acquired_coin) matches c_quote
#         if direction == "reverse":
#             if a_base == c_quote and calculated == 0:
#                 swap_2_rate = c_bid
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "quote_to_base"
#                 contract_2 = pair_c

#                 # If c_base (acquired coin) matches b_base
#                 if c_base == b_base:
#                     swap_3 = b_base
#                     swap_3_rate = 1 / b_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_b

#                 # If c_base (acquired coin) matches b_quote
#                 if c_base == b_quote:
#                     swap_3 = b_quote
#                     swap_3_rate = b_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_b

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1

#         # SCENARIO 4 Check if a_base (acquired_coin) matches c_base
#         if direction == "reverse":
#             if a_base == c_base and calculated == 0:
#                 swap_2_rate = 1 / c_ask
#                 acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
#                 direction_trade_2 = "base_to_quote"
#                 contract_2 = pair_c

#                 # If c_quote (acquired coin) matches b_base
#                 if c_quote == b_base:
#                     swap_3 = b_base
#                     swap_3_rate = 1 / b_ask
#                     direction_trade_3 = "base_to_quote"
#                     contract_3 = pair_b

#                 # If c_quote (acquired coin) matches b_quote
#                 if c_quote == b_quote:
#                     swap_3 = b_quote
#                     swap_3_rate = b_bid
#                     direction_trade_3 = "quote_to_base"
#                     contract_3 = pair_b

#                 acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
#                 calculated = 1
                
#         """ PROFIT LOSS OUTPUT """

#         # Profit and Loss Calculations
#         profit_loss = acquired_coin_t3 - starting_amount
#         profit_loss_perc = (profit_loss / starting_amount) * 100 if profit_loss != 0 else 0

#         # Trade Descriptions
#         trade_description_1 = f"Start with {swap_1} of {starting_amount}. Swap at {swap_1_rate} for {swap_2} acquiring {acquired_coin_t1}."
#         trade_description_2 = f"Swap {acquired_coin_t1} of {swap_2} at {swap_2_rate} for {swap_3} acquiring {acquired_coin_t2}."
#         trade_description_3 = f"Swap {acquired_coin_t2} of {swap_3} at {swap_3_rate} for {swap_1} acquiring {acquired_coin_t3}."

#         # Output Results
#         if profit_loss_perc >= min_surface_rate:
#             surface_dict = {
#                 "swap_1": swap_1,
#                 "swap_2": swap_2,
#                 "swap_3": swap_3,
#                 "contract_1": contract_1,
#                 "contract_2": contract_2,
#                 "contract_3": contract_3,
#                 "direction_trade_1": direction_trade_1,
#                 "direction_trade_2": direction_trade_2,
#                 "direction_trade_3": direction_trade_3,
#                 "starting_amount": starting_amount,
#                 "acquired_coin_t1": acquired_coin_t1,
#                 "acquired_coin_t2": acquired_coin_t2,
#                 "acquired_coin_t3": acquired_coin_t3,
#                 "swap_1_rate": swap_1_rate,
#                 "swap_2_rate": swap_2_rate,
#                 "swap_3_rate": swap_3_rate,
#                 "profit_loss": profit_loss,
#                 "profit_loss_perc": profit_loss_perc,
#                 "direction": direction,
#                 "trade_description_1": trade_description_1,
#                 "trade_description_2": trade_description_2,
#                 "trade_description_3": trade_description_3
#             }

#             return surface_dict

#     return surface_dict

# def calculate_acquired_coin(amount_in, orderbook):

#     """
#         CHALLENGES
#         Full amount of starting amount can be eaten on the first level (level 0)
#         Some of the amount in can be eaten up by multiple levels
#         Some coins may not have enough liquidity
#     """

#     # Initialise Variables
#     trading_balance = amount_in
#     quantity_bought = 0
#     acquired_coin = 0
#     counts = 0
#     for level in orderbook:

#         # Extract the level price and quantity
#         level_price = level[0]
#         level_available_quantity = level[1]

#         # Amount In is <= first level total amount
#         if trading_balance <= level_available_quantity:
#             quantity_bought = trading_balance
#             trading_balance = 0
#             amount_bought = quantity_bought * level_price

#         # Amount In is > a given level total amount
#         if trading_balance > level_available_quantity:
#             quantity_bought = level_available_quantity
#             trading_balance -= quantity_bought
#             amount_bought = quantity_bought * level_price

#         # Accumulate Acquired Coin
#         acquired_coin = acquired_coin + amount_bought

#         # Exit Trade
#         if trading_balance == 0:
#             return acquired_coin

#         # Exit if not enough order book levels
#         counts += 1
#         if counts == len(orderbook):
#             return 0

# # Reformat Order Book for Depth Calculation
# def reformated_orderbook(prices, c_direction):
#     price_list_main = []
#     if c_direction == "base_to_quote":
#         for p in prices["asks"]:
#             ask_price = p[0]
#             adj_price = 1 / ask_price if ask_price != 0 else 0
#             adj_quantity = p[1] * ask_price
#             price_list_main.append([adj_price, adj_quantity])
#     if c_direction == "quote_to_base":
#         for p in prices["bids"]:
#             bid_price = p[0]
#             adj_price = bid_price if bid_price != 0 else 0
#             adj_quantity = p[1]
#             price_list_main.append([adj_price, adj_quantity])
#     return price_list_main
    
# def get_depth_from_orderbook(surface_arb):
#     swap_1 = surface_arb["swap_1"]
#     starting_amount = 100
#     starting_amount_dict = {
#         "USDT": 100,
#         "USDC": 100,
#         "BTC": 0.02,
#         "ETH": 0.045,
#         "UNI": 16,
#         "BETH": 0.045
#     }
#     if swap_1 in starting_amount_dict:
#         starting_amount = starting_amount_dict[swap_1]

#     # Define pairs
#     contract_1 = surface_arb["contract_1"]
#     contract_2 = surface_arb["contract_2"]
#     contract_3 = surface_arb["contract_3"]

#     # Define direction for trades
#     contract_1_direction = surface_arb["direction_trade_1"]
#     contract_2_direction = surface_arb["direction_trade_2"]
#     contract_3_direction = surface_arb["direction_trade_3"]

#      # Get Order Book for First Trade Assessment
#     depth_1_prices = client.fetch_order_book(contract_1, limit=20)
#     depth_1_reformatted_prices = reformated_orderbook(depth_1_prices, contract_1_direction)
#     time.sleep(0.3)
#     depth_2_prices = client.fetch_order_book(contract_2, limit=20)
#     depth_2_reformatted_prices = reformated_orderbook(depth_2_prices, contract_2_direction)
#     time.sleep(0.3)
#     depth_3_prices = client.fetch_order_book(contract_3, limit=20)
#     depth_3_reformatted_prices = reformated_orderbook(depth_3_prices, contract_3_direction)

#     # Get Acquired Coins
#     acquired_coin_t1 = calculate_acquired_coin(starting_amount, depth_1_reformatted_prices)
#     acquired_coin_t2 = calculate_acquired_coin(acquired_coin_t1, depth_2_reformatted_prices)
#     acquired_coin_t3 = calculate_acquired_coin(acquired_coin_t2, depth_3_reformatted_prices)

#     # Calculate Profit Loss Also Known As Real Rate
#     profit_loss = acquired_coin_t3 - starting_amount
#     real_rate_perc = (profit_loss / starting_amount) * 100 if profit_loss != 0 else 0

#     if real_rate_perc > -1:
#         return_dict = {
#             "profit_loss": profit_loss,
#             "real_rate_perc": real_rate_perc,
#             "contract_1": contract_1,
#             "contract_2": contract_2,
#             "contract_3": contract_3,
#             "contract_1_direction": contract_1_direction,
#             "contract_2_direction": contract_2_direction,
#             "contract_3_direction": contract_3_direction
#         }
#         return return_dict
#     else:
#         return {}


# def main():
#     client = ccxt.bybit({
#         'apiKey': API_KEY,
#         'secret': API_SECRET,
#     })

#     # get_trianbular_pairs(client)
  
#     with open("coin_list.json") as json_file:
#         structured_pairs = json.load(json_file)
#     print(len(structured_pairs))
#     while True:
#         for t_pair in structured_pairs:
#             time.sleep(0.4)
#             try:
#                 print(t_pair['combined'])
#                 prices_dict = get_price_for_t_pair(t_pair)
#                 surface_dict = calc_triangular_arb_surface_rate(t_pair, prices_dict)
#                 if len(surface_dict) > 0:
#                     real_rate_arb = get_depth_from_orderbook(surface_dict)
#                     print(real_rate_arb, '\n\n')
#                     with open('trading_logs.txt', 'a') as f:
#                         f.write(f"Profit Loss{real_rate_arb}\n")
#                 time.sleep(0.4)
#             except Exception as e:
#                 if 'unsupported operand type(s)' in str(e):
#                     structured_pairs.remove(t_pair)
#                     logger.info(f"Removed {t_pair['combined']} from list")
#                     logger.info(f"lenght of structured_pairs: {len(structured_pairs)}")
#                 print(str(e))
#                 continue


# if __name__ == '__main__':
#     main()