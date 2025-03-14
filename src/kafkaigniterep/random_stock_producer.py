#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import logging
import os
import random
import time
from logging.handlers import TimedRotatingFileHandler

from faker.providers import BaseProvider


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

StockNames = [
    "Deja Brew",
    "Jurassic Pork",
    "Lawn & Order",
    "Pita Pan",
    "Bread Pitt",
    "Indiana Jeans",
    "Thai Tanic",
]
StockCurrentValues = [10.0, 20.1, 20.2, 12.1, 25.1, 25.1, 27.5]
StockUpProb = [0.5, 0.6, 0.7, 0.8, 0.9, 0.4, 0.3]
ShuffleProb = 0.2
ChangeAmount = 0.8


class StockProvider(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)

    @staticmethod
    def stock_name():
        return random.choice(StockNames)

    @staticmethod
    def stock_value(stockname):
        indexStock = StockNames.index(stockname)
        currentval = StockCurrentValues[indexStock]
        goesup = 1 if random.random() <= StockUpProb[indexStock] else -1
        nextval = currentval + random.random() * ChangeAmount * goesup
        StockCurrentValues[indexStock] = nextval
        return nextval

    @staticmethod
    def reshuffle_probs(stockname):
        indexStock = StockNames.index(stockname)
        StockUpProb[indexStock] = random.random()

    def produce_msg(self):
        stockname = self.stock_name()
        ts = time.time()
        if random.random() > ShuffleProb:
            self.reshuffle_probs(stockname)
        message = {
            "stock_name": stockname,
            "stock_value": self.stock_value(stockname),
            "timestamp": int(ts * 1000),
        }
        key = {"user": "all_users"}
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
    random_stock_producer = StockProvider(BaseProvider)

    while True:
        try:
            message, key = random_stock_producer.produce_msg()
            if message:
                logger.info(f"Successfully generated: {message}")
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            logger.critical(f"Critical failure in message generation loop: {e}")
            time.sleep(5)


def parse_command_line() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='random_stock_producer.')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--kafka-debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=r'D:\kafkaignite\logs\random_stock_producer.log', help='Log file')
    parser.add_argument('--log-retention-days', default=7, type=int, help='Log Retention Days')
    parser.add_argument('--config-file', default='random_stock_producer.conf', help='Configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    main()
