#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import configparser
import logging
import os
import random
import time
import warnings
from logging.handlers import TimedRotatingFileHandler

from faker import Faker
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

# Constants for flexibility and configuration
MIN_AMOUNT = 2
MAX_AMOUNT = 1000
ALPHA = 2.5  # Lower ALPHA for realistic betting patterns
RISK_FACTORS = {"high_risk": 0.8, "casual": 0.5, "low_risk": 0.3}


class UserBetsProvider(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)
        self.valid_usernames = [
            "nopineappleonpizza", "catanzaro99", "thedoctor",
            "bettingexpert01", "losingmoney66", "manutd007",
            "manutd009", "citylife1", "lysa_X", "aiventest"
        ]

        self.valid_events = [
            {"category": "Sport", "subcategory": "Football", "event": "ManUTD vs Chelsea"},
            {"category": "Sport", "subcategory": "Box", "event": "Chicken Legs vs Power Kick"},
            {"category": "Sport", "subcategory": "Curling", "event": "Italy vs England"},
            {"category": "Sport", "subcategory": "Netball", "event": "Sydney vs Canberra"},
            {"category": "Lottery", "subcategory": "Bingo", "event": "UK Bingo"},
            {"category": "Lottery", "subcategory": "WinForLife", "event": "Win For Life America"},
            {"category": "Event", "subcategory": "Music", "event": "Rick Astley #1 in World Charts"},
            {"category": "Event", "subcategory": "Politics", "event": "Mickey Mouse new Italian President"},
            {"category": "Event", "subcategory": "Celebrities", "event": "Donald Duck and Marge Simpson Wedding"},
        ]

    def username(self):
        return random.choice(self.valid_usernames)

    def bet_amount(self, risk_profile="casual"):
        """
        Generates a bet amount based on the user's risk profile.
        """
        risk_factor = RISK_FACTORS.get(risk_profile, 0.5)
        base_amount = random.uniform(MIN_AMOUNT, MAX_AMOUNT * risk_factor)
        return round(base_amount, 2)

    def bet_category_event(self):
        return random.choice(self.valid_events)

    @staticmethod
    def generate_timestamp(offset_seconds=0):
        """
        Generates a timestamp with optional time offset for future/past testing.
        """
        return int((time.time() + offset_seconds) * 1000)

    def dynamic_key(self, bet_event):
        """
        Generates dynamic keys for better Kafka partitioning by combining multiple fields.
        """
        return {
            "event": bet_event["event"],
            "category": bet_event["category"],
            "subcategory": bet_event["subcategory"]
        }

    def produce_msg(self, risk_profile="casual", offset_seconds=0):
        """
        Produces a structured betting message with flexible risk profiles and timestamps.
        """
        username = self.username()
        bet_amount = self.bet_amount(risk_profile)
        bet_event = self.bet_category_event()

        message = {
            "username": username,
            "event": {
                "category": bet_event["category"],
                "subcategory": bet_event["subcategory"],
                "name": bet_event["event"],
            },
            "amount": bet_amount,
            "timestamp": self.generate_timestamp(),
        }

        key = self.dynamic_key(bet_event)
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
    bet_data_producer = UserBetsProvider(BaseProvider)

    while True:
        try:
            message, key = bet_data_producer.produce_msg()
            if message:
                logger.info(f"Successfully generated: {message}")
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            logger.critical(f"Critical failure in message generation loop: {e}")
            time.sleep(5)


def parse_command_line() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='bet_data_producer')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--kafka-debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=r'D:\kafkaignite\logs\bet_data_producer.log', help='Log file')
    parser.add_argument('--log-retention-days', default=7, type=int, help='Log Retention Days')
    parser.add_argument('--config-file', default='bet_data_producer.conf', help='Configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    main()
