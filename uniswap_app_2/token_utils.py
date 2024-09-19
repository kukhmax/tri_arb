from web3 import Web3
import logging

def get_token_info(token_address):
    """
    Получает информацию о токене (symbol, name, decimals).

    Parameters
    ----------
    token_address : str
        Адрес токена в формате checksum.

    Returns
    -------
    dict
        Словарь с информацией о токене:
            - symbol : str, символ токена (например, "ETH")
            - name : str, полное имя токена (например, "Ethereum")
            - decimals : int, количество знаков после запятой (например, 18)
    """
    ERC20_ABI = [
        {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
    ]
    
    token_contract = Web3().eth.contract(address=token_address, abi=ERC20_ABI)
    
    try:
        symbol = token_contract.functions.symbol().call()
        name = token_contract.functions.name().call()
        decimals = token_contract.functions.decimals().call()
        return {"symbol": symbol, "name": name, "decimals": decimals}
    except Exception as e:
        logging.error(f"Ошибка при получении информации о токене {token_address}: {e}")
        return {"symbol": "UNKNOWN", "name": "UNKNOWN", "decimals": 18}
