#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import configparser
import logging
import random
import time
import warnings
from logging.handlers import TimedRotatingFileHandler
from typing import Any
from typing import Dict, Tuple

import requests_html
import yfinance as yf
from faker.providers import BaseProvider
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError
from yahoo_fin import stock_info as si

print(requests_html.__name__)

# Suppress warnings related to yfinance library
warnings.filterwarnings('ignore')
# Stock symbols for popular cryptocurrencies
STOCK_SYMBOLS = ["BTC-USD", "ETH-USD", "BNB-USD", "ADA-USD", "DOGE-USD", "SOL-USD", "XRP-USD", "LTC-USD", "DOT-USD"]


# Logging Configuration
def configure_logging(log_file: str, retention_days: int, debug: bool) -> None:
    log_level = logging.DEBUG if debug else logging.INFO

    handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=retention_days, encoding="utf-8"
    )
    handler.suffix = "%Y-%m-%d"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[handler, logging.StreamHandler()]
    )


logger = logging.getLogger(__name__)


class RealStockProvider(BaseProvider):
    """Custom provider for generating real-time cryptocurrency stock data."""

    @staticmethod
    def stock_name() -> str:
        """Return a random cryptocurrency symbol."""
        return random.choice(STOCK_SYMBOLS)

    @retry(stop=stop_after_attempt(15), wait=wait_fixed(2))
    def stock_value(self, stock_name: str) -> str | Any:
        """Fetch the live stock value with retry logic for reliability."""
        try:
            stock_value = si.get_live_price(stock_name.upper())
            if stock_value is None or stock_value == "":
                raise ValueError("Empty or invalid response from Yahoo Finance API")
            return round(stock_value, 2)
        except (ValueError, KeyError) as e:
            logger.warning(f"Yahoo Finance failed for {stock_name}: {e}. Switching to yfinance.")
            return self._fetch_from_yfinance(stock_name)
        except Exception as e:
            logger.warning(f"Failed to fetch stock value for {stock_name}: {e}")
            return -0.1

    @staticmethod
    def _fetch_from_yfinance(stock_name: str) -> float | Any:
        """Fallback to yfinance if yahoo_fin fails."""
        try:
            stock = yf.Ticker(stock_name)
            stock_value = stock.history(period='1d')['Close'].iloc[-1]
            return round(float(stock_value))
        except Exception as e:
            logger.warning(f"yfinance failed for {stock_name}: {e}")
            return -0.1

    def produce_msg(self) -> Tuple[Dict[str, any], Dict[str, str]]:
        """Generate a stock market message."""
        stock_name = self.stock_name()
        try:
            stock_value = self.stock_value(stock_name)
        except RetryError:
            stock_value = "Data Unavailable"  # Final fallback if retries fail

        message = {
            "stock_name": stock_name,
            "stock_value": stock_value,
            "timestamp": int(time.time() * 1000),
        }
        key = {"stock_name": stock_name}
        return message, key


def read_server_config(config_file: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    try:
        cfg.read(config_file)
        defaults = {
            'user': 'KAFKA_USER',
            'password': 'KAFKA_PASSWORD',
            'cafile': 'KAFKA_CA_CRT',
            'bootstrap_servers': 'localhost:9092',
            'ssl_check_hostname': 'true',
            'topic': 'debug-tool-01',
            'topic_replication_factor': '1',
            'topic_retention_ms': '15552000000',
            'flush_timeout_s': '60.0',
        }

        if 'kafka' not in cfg:
            cfg['kafka'] = {}

        for key, env_var in defaults.items():
            cfg['kafka'].setdefault(key, str(os.environ.get(env_var, defaults[key])))
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise
    return cfg


def main() -> None:
    args = parse_command_line()
    configure_logging(args.log_file, args.log_retention_days, args.debug)
    cfg = read_server_config(args.config_file)
    real_stock_producer = RealStockProvider(BaseProvider)

    while True:
        try:
            message, key = real_stock_producer.produce_msg()
            if message:
                logger.info(f"Successfully generated: {message}")
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            logger.critical(f"Critical failure in message generation loop: {e}")
            time.sleep(5)


def parse_command_line() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='real_stock_producer is a Python library that generates '
                                                 'synthetic host and CPU metrics with configurable logging and '
                                                 'message production for testing and debugging.')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--kafka-debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=r'D:\kafkaignite\logs\real_stock_producer.log', help='Log file')
    parser.add_argument('--log-retention-days', default=7, type=int, help='Log Retention Days')
    parser.add_argument('--config-file', default='real_stock_producer.conf', help='Configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    main()
