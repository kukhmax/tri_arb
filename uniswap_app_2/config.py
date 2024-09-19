import os
import logging
from web3 import Web3
from colorama import Fore, Style, init
from datetime import datetime

# Инициализация colorama для поддержки цвета в консоли на Windows
init(autoreset=True)

# Устанавливаем формат даты для логов
log_filename = datetime.now().strftime("%Y-%m-%d_triangular_arbitrage.log")

def setup_logging():
    """
    Настраивает логирование с цветным выводом в консоли и записью в файл.
    """
    # Настройка логирования
    logging.basicConfig(
        filename=log_filename,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Добавляем консольный логгер с цветным выводом
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    
    # Форматтер для цветного логирования
    class ColoredFormatter(logging.Formatter):
        def format(self, record):
            log_msg = super().format(record)
            if record.levelname == 'INFO':
                return f"{Fore.GREEN}{log_msg}{Style.RESET_ALL}"
            elif record.levelname == 'WARNING':
                return f"{Fore.YELLOW}{log_msg}{Style.RESET_ALL}"
            elif record.levelname == 'ERROR':
                return f"{Fore.RED}{log_msg}{Style.RESET_ALL}"
            elif record.levelname == 'DEBUG':
                return f"{Fore.CYAN}{log_msg}{Style.RESET_ALL}"
            else:
                return log_msg
    
    formatter = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]", "%Y-%m-%d %H:%M:%S")
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

def connect_to_infura():
    """
    Подключается к Ethereum через Infura.

    Returns
    -------
    Web3
        Объект подключения к Ethereum.
    """
    INFURA_ID = os.getenv('INFURA_ID')
    INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_ID}"
    web3 = Web3(Web3.HTTPProvider(INFURA_URL))
    
    if not web3.is_connected():
        logging.error(f"{Fore.RED}Не удалось подключиться к Ethereum через Infura.")
        sys.exit(1)
    else:
        logging.info(f"{Fore.GREEN}Подключение к Ethereum через Infura успешно.")
    
    return web3
