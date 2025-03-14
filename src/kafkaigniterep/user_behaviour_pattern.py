#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import datetime
import logging
import os
import random
import time
import warnings
from logging.handlers import TimedRotatingFileHandler

from faker.providers import BaseProvider

warnings.filterwarnings('ignore')


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


class UserBehaviorProvider(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)

        # Data pools for flexibility
        self.user_ids = list(range(1, 10))
        self.item_ids = list(range(21, 30))
        self.behavior_names = ["view", "cart", "buy"]
        self.group_names = ["A", "B"]
        self.view_ids = [111, 222, 555]

    def user_id(self):
        return random.choice(self.user_ids)

    def item_id(self):
        return random.choice(self.item_ids)

    def behavior(self):
        return random.choices(
            self.behavior_names,
            weights=[0.5, 0.3, 0.2]  # More realistic distribution
        )[0]

    def group_name(self):
        return random.choice(self.group_names)

    def view_id(self):
        return random.choice(self.view_ids)
    @staticmethod
    def generate_timestamp(self, offset_seconds=0):
        """Generates a timestamp with optional random offset"""
        ts = time.time() + random.uniform(-5, 5) + offset_seconds
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    def produce_msg(self, include_view_id=True):
        """Produces a robust message with flexible parameters."""
        b = self.behavior()
        view_id = self.view_id() if b == "view" and include_view_id else None

        message = {
            "user_id": self.user_id(),
            "item_id": self.item_id(),
            "behavior": b,
            "view_id": view_id,
            "group_name": self.group_name(),
            "occurred_at": self.generate_timestamp(),
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
    user_behaviour_pattern = UserBehaviorProvider(BaseProvider)

    while True:
        try:
            message, key = user_behaviour_pattern.produce_msg()
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
    parser.add_argument('--log-file', default=r'D:\kafkaignite\logs\user_behaviour_pattern.log', help='Log file')
    parser.add_argument('--log-retention-days', default=7, type=int, help='Log Retention Days')
    parser.add_argument('--config-file', default='user_behaviour_pattern.conf', help='Configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    main()
