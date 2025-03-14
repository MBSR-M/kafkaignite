#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import random
import time
from typing import Dict, Tuple

from faker import Faker
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


class PizzaProvider(BaseProvider):
    """Custom provider for generating pizza-related data."""

    PIZZA_NAMES = [
        "Margherita", "Marinara", "Diavola",
        "Mari & Monti", "Salami", "Peperoni"
    ]

    TOPPINGS = [
        "🍅 tomato", "🧀 blue cheese", "🥚 egg",
        "🫑 green peppers", "🌶️ hot pepper", "🥓 bacon",
        "🫒 olives", "🧄 garlic", "🐟 tuna", "🧅 onion",
        "🍍 pineapple", "🍓 strawberry", "🍌 banana"
    ]

    PIZZA_SHOPS = [
        "Mario's Pizza", "Luigi's Pizza", "Circular Pi Pizzeria",
        "I'll Make You a Pizza You Can't Refuse", "Mammamia Pizza",
        "It's-a me! Mario Pizza!"
    ]

    def pizza_name(self) -> str:
        """Return a random pizza name."""
        return random.choice(self.PIZZA_NAMES)

    def pizza_topping(self) -> str:
        """Return a random pizza topping."""
        return random.choice(self.TOPPINGS)

    def pizza_shop(self) -> str:
        """Return a random pizza shop name."""
        return random.choice(self.PIZZA_SHOPS)

    def produce_msg(
        self,
        faker_instance: Faker,
        order_count: int = 1,
        max_pizzas_in_order: int = 5,
        max_toppings_in_pizza: int = 3,
    ) -> Tuple[Dict[str, any], Dict[str, str]]:
        """Generate a sample pizza order message."""
        pizzas = [
            {
                "pizza_name": self.pizza_name(),
                "additional_toppings": [
                    self.pizza_topping() for _ in range(random.randint(0, max_toppings_in_pizza))
                ]
            }
            for _ in range(random.randint(1, max_pizzas_in_order))
        ]

        message = {
            "id": order_count,
            "shop": self.pizza_shop(),
            "name": faker_instance.unique.name(),
            "phone_number": faker_instance.unique.phone_number(),
            "address": faker_instance.address(),
            "pizzas": pizzas,
            "timestamp": int(time.time() * 1000),
        }
        key = {"shop": message["shop"]}

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
    pizza_producer = PizzaProvider(BaseProvider)

    while True:
        try:
            message, key = pizza_producer.produce_msg(Faker())
            if message:
                logger.info(f"Successfully generated: {message}")
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            logger.critical(f"Critical failure in message generation loop: {e}")
            time.sleep(5)


def parse_command_line() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='pizza_producer')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--kafka-debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=r'D:\kafkaignite\logs\pizza_producer.log', help='Log file')
    parser.add_argument('--log-retention-days', default=7, type=int, help='Log Retention Days')
    parser.add_argument('--config-file', default='pizza_producer.conf', help='Configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    main()
