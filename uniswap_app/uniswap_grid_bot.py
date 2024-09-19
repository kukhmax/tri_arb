

import asyncio
from web3 import Web3
from web3.middleware import geth_poa_middleware
import json

# Замените на свой Infura URL
INFURA_URL = "https://mainnet.infhttps://mainnet.infura.io/v3/8474021cb937499aa258782100e51f60"

# Адрес контракта Uniswap V3 Router
UNISWAP_V3_ROUTER_ADDRESS = "0xE592427A0AEce92De3Edee1F18E0157C05861564"

# ABI для Uniswap V3 Router (упрощенный, только необходимые функции)
UNISWAP_V3_ROUTER_ABI = json.loads('''
[{"inputs":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"}]
''')

# Настройки бота
TOKEN_IN = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
TOKEN_OUT = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # WETH
FEE_TIER = 3000  # 0.3%
GRID_STEP = 0.01  # 1% шаг сетки
AMOUNT_IN = Web3.to_wei(10, 'ether')  # 10 USDC на каждую сделку

# Инициализация Web3
w3 = Web3(Web3.HTTPProvider(INFURA_URL))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

# Загрузка контракта
uniswap_contract = w3.eth.contract(address=UNISWAP_V3_ROUTER_ADDRESS, abi=UNISWAP_V3_ROUTER_ABI)

# Функция для выполнения свопа
async def execute_swap(price):
    nonce = w3.eth.get_transaction_count('YOUR_WALLET_ADDRESS')
    
    # Подготовка транзакции
    swap_txn = uniswap_contract.functions.exactInputSingle(
        TOKEN_IN,
        TOKEN_OUT,
        FEE_TIER,
        'YOUR_WALLET_ADDRESS',
        w3.eth.get_block('latest')['timestamp'] + 300,  # deadline: 5 минут
        AMOUNT_IN,
        0,  # amountOutMinimum: 0 для примера, в реальности нужно рассчитать
        0  # sqrtPriceLimitX96: 0 означает без ограничений
    ).build_transaction({
        'from': 'YOUR_WALLET_ADDRESS',
        'gas': 250000,
        'gasPrice': w3.eth.gas_price,
        'nonce': nonce,
    })
    
    # Подписание и отправка транзакции
    signed_txn = w3.eth.account.sign_transaction(swap_txn, 'YOUR_PRIVATE_KEY')
    tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    
    # Ожидание подтверждения транзакции
    tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    print(f"Swap executed at price {price}. Transaction hash: {tx_receipt.transactionHash.hex()}")

# Основной цикл бота
async def grid_bot():
    last_price = None
    while True:
        # Здесь должна быть логика получения текущей цены из Uniswap V3
        current_price = get_current_price()  # Эту функцию нужно реализовать
        
        if last_price is None:
            last_price = current_price
        
        price_change = (current_price - last_price) / last_price
        
        if abs(price_change) >= GRID_STEP:
            await execute_swap(current_price)
            last_price = current_price
        
        await asyncio.sleep(60)  # Проверка каждую минуту

# Запуск бота
if __name__ == "__main__":
    asyncio.run(grid_bot())
